"""PT subscription-window restart safety for P115StrgmSub.

This module is installed as a small runtime patch so the temporary PT window
survives MoviePilot/container restarts. It also separates the user's master
"block system subscriptions" intent from the temporary runtime open/closed
state.
"""
from __future__ import annotations

import datetime
from typing import Any, Dict, Optional

import pytz

from app.core.config import settings
from app.log import logger


_WINDOW_DATA_KEY = "pt_window_runtime_v1"
_MIGRATION_DATA_KEY = "pt_window_restart_safe_migration_v1"
_HOOK_MARKER = "_p115_pt_restart_safe_installed"


def _timezone():
    try:
        return pytz.timezone(settings.TZ)
    except Exception:
        return pytz.timezone("Asia/Shanghai")


def _now() -> datetime.datetime:
    return datetime.datetime.now(tz=_timezone())


def _parse_datetime(value: Any) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    tz = _timezone()
    if parsed.tzinfo is None:
        parsed = tz.localize(parsed)
    else:
        parsed = parsed.astimezone(tz)
    return parsed


def _load_window_state(plugin) -> Dict[str, Any]:
    try:
        value = plugin.get_data(_WINDOW_DATA_KEY) or {}
    except Exception as error:
        logger.warning(f"读取 PT 窗口持久状态失败：{error}")
        return {}
    return value if isinstance(value, dict) else {}


def _save_window_state(
    plugin,
    *,
    active: bool,
    ends_at: Optional[datetime.datetime] = None,
    reason: str = "",
) -> None:
    state = {
        "schema_version": 1,
        "active": bool(active),
        "ends_at": ends_at.isoformat() if ends_at else "",
        "reason": str(reason or ""),
        "updated_at": _now().isoformat(),
    }
    try:
        plugin.save_data(_WINDOW_DATA_KEY, state)
    except Exception as error:
        logger.error(f"保存 PT 窗口持久状态失败：{error}")


def _clear_window_state(plugin, reason: str = "") -> None:
    _save_window_state(plugin, active=False, reason=reason)


def _migration_done(plugin) -> bool:
    try:
        value = plugin.get_data(_MIGRATION_DATA_KEY) or {}
    except Exception:
        return False
    return bool(isinstance(value, dict) and value.get("done"))


def _mark_migration_done(plugin) -> None:
    try:
        plugin.save_data(
            _MIGRATION_DATA_KEY,
            {
                "done": True,
                "version": "1.8.6",
                "migrated_at": _now().isoformat(),
            },
        )
    except Exception as error:
        logger.warning(f"保存 1.8.6 迁移标记失败：{error}")


def install_restart_safe(plugin_class) -> None:
    """Install the restart-safe behavior exactly once."""
    if getattr(plugin_class, _HOOK_MARKER, False):
        return

    original_init_plugin = plugin_class.init_plugin
    original_update_config = plugin_class._P115StrgmSub__update_config
    original_enter_blocked = plugin_class._enter_blocked

    def restart_safe_update_config(self):
        """Persist the user's master switch, not the temporary window state."""
        actual_runtime_state = bool(self._block_system_subscribe)
        desired_master_state = bool(
            getattr(self, "_pt_block_master_enabled", actual_runtime_state)
        )
        self._block_system_subscribe = desired_master_state
        try:
            return original_update_config(self)
        finally:
            self._block_system_subscribe = actual_runtime_state

    def restart_safe_enter_blocked(self, reason: str):
        result = original_enter_blocked(self, reason)
        if not getattr(self, "_pt_restart_safe_initializing", False):
            _clear_window_state(self, reason=f"进入屏蔽状态：{reason}")
        return result

    def restart_safe_schedule_reblock(self):
        """Persist the deadline before creating the in-memory APScheduler job."""
        hours = float(self._system_subscribe_window_hours or 0)
        if hours <= 0:
            _clear_window_state(self, reason="窗口时长为0")
            return

        self._ensure_toggle_scheduler()
        now = _now()
        run_date = now + datetime.timedelta(hours=hours)
        _save_window_state(
            self,
            active=True,
            ends_at=run_date,
            reason="系统订阅临时开放窗口",
        )

        self._toggle_scheduler.add_job(
            func=lambda: self._enter_blocked(reason="窗口到期"),
            trigger="date",
            run_date=run_date,
            id="p115_reblock_job",
            replace_existing=True,
        )
        logger.info(
            f"已持久化 PT 开放窗口：截止={run_date}；"
            "MoviePilot 重启后将恢复剩余时间"
        )

    def restore_active_window(self, ends_at: datetime.datetime) -> bool:
        """Restore an already-open window without extending its deadline."""
        now = _now()
        if ends_at <= now:
            return False
        if not self._window_enabled():
            logger.warning("PT 窗口持久状态存在，但当前窗口配置已禁用，保持屏蔽")
            return False

        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()
        self._init_subscribe_handler()

        site_ids = self._resolve_site_ids(
            ids=self._unblock_site_ids,
            names=self._unblock_site_names,
        )
        if not site_ids:
            logger.error("恢复 PT 窗口失败：开放站点解析为空，继续保持屏蔽")
            return False

        if not self._set_system_rss_sites(
            site_ids,
            reason="MoviePilot 重启后恢复未到期 PT 窗口",
        ):
            logger.error("恢复 PT 窗口失败：无法设置系统默认订阅站点")
            return False

        self._apply_sites_to_all_subscribes(
            site_ids,
            reason="重启恢复 PT 窗口：全量同步站点",
        )
        self._block_system_subscribe = False
        self._P115StrgmSub__update_config()

        self._toggle_scheduler.add_job(
            func=lambda: self._enter_blocked(reason="恢复窗口到期"),
            trigger="date",
            run_date=ends_at,
            id="p115_reblock_job",
            replace_existing=True,
        )
        _save_window_state(
            self,
            active=True,
            ends_at=ends_at,
            reason="MoviePilot 重启后恢复未到期窗口",
        )
        remaining = max(0, int((ends_at - now).total_seconds()))
        logger.info(
            f"已恢复重启前 PT 开放窗口：截止={ends_at}，"
            f"剩余约={remaining // 60}分钟；到期自动重新屏蔽"
        )
        return True

    def restart_safe_init_plugin(self, config: dict = None):
        supplied_config = config is not None
        effective_config = dict(config or {}) if supplied_config else None

        requested_master_state = bool(
            (effective_config or {}).get("block_system_subscribe", False)
        )

        # One-time migration for the exact 1.8.5 failure mode: an active window
        # was written as False and the in-memory reblock task disappeared on
        # restart. The local deployment intentionally enables the master switch
        # once; later user changes are respected.
        if supplied_config and not _migration_done(self):
            if not requested_master_state:
                requested_master_state = True
                effective_config["block_system_subscribe"] = True
                logger.warning(
                    "1.8.6 首次迁移：检测到旧版将临时 PT 窗口误写为关闭屏蔽，"
                    "已自动恢复‘屏蔽系统订阅’主开关"
                )
            _mark_migration_done(self)

        self._pt_block_master_enabled = requested_master_state
        saved_state = _load_window_state(self)
        saved_ends_at = _parse_datetime(saved_state.get("ends_at"))
        saved_active = bool(saved_state.get("active") and saved_ends_at)

        self._pt_restart_safe_initializing = True
        try:
            result = original_init_plugin(self, effective_config)
        finally:
            self._pt_restart_safe_initializing = False

        if not requested_master_state:
            _clear_window_state(self, reason="用户关闭屏蔽主开关")
            return result

        if saved_active and saved_ends_at and saved_ends_at > _now():
            if restore_active_window(self, saved_ends_at):
                return result
            self._enter_blocked(reason="重启恢复窗口失败")
            return result

        if saved_active:
            _clear_window_state(self, reason="重启时发现 PT 窗口已经到期")
            if not self._block_system_subscribe:
                self._enter_blocked(reason="重启时发现窗口已到期")

        return result

    plugin_class._P115StrgmSub__update_config = restart_safe_update_config
    plugin_class._enter_blocked = restart_safe_enter_blocked
    plugin_class._schedule_reblock_after_window = restart_safe_schedule_reblock
    plugin_class.init_plugin = restart_safe_init_plugin
    setattr(plugin_class, _HOOK_MARKER, True)
