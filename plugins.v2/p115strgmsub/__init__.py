"""
115网盘订阅追更插件
结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失剧集
"""
import datetime
import hashlib
import math
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.core.config import settings, global_vars
from app.core.event import Event, eventmanager
from app.chain.subscribe import SubscribeChain
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.models.site import Site
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, ChainEventType, MediaType, NotificationType

from .clients import (
    PanSouClient,
    P115ClientManager,
    NullbrClient,
    HDHiveOpenAPIClient,
    HDHiveOpenAPIError,
    OpenClawClassifierClient,
    AyclubClient,
)
from .handlers import SearchHandler, SyncHandler, SubscribeHandler, ApiHandler, LifecycleStore
from .ui import UIConfig
from .utils import download_so_file

lock = Lock()


class P115StrgmSub(_PluginBase):
    """115网盘订阅追更插件"""

    # 插件名称
    plugin_name = "115网盘订阅追更"
    # 插件描述
    plugin_desc = "结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失的电影和剧集。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    # 插件版本
    plugin_version = "1.9.8"
    # 插件作者
    plugin_author = "mrtian2016"
    # 作者主页
    author_url = "https://github.com/mrtian2016"
    # 插件配置项ID前缀
    plugin_config_prefix = "p115strgmsub_"
    plugin_order = 20
    auth_level = 1

    # 私有变量
    _scheduler: Optional[BackgroundScheduler] = None
    _toggle_scheduler: Optional[BackgroundScheduler] = None  # 用于延迟切换/窗口切换

    # 配置属性
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = "30 6,14,22 * * *"
    _notify: bool = False

    _cookies: str = ""
    _pansou_enabled: bool = True
    _pansou_url: str = "https://so.252035.xyz"
    _pansou_username: str = ""
    _pansou_password: str = ""
    _pansou_auth_enabled: bool = False
    _pansou_channels: str = "QukanMovie"
    # AYCLUB Telegram 桥接服务
    _ayclub_enabled: bool = False
    _ayclub_url: str = "http://127.0.0.1:11592"
    _ayclub_timeout: int = 120
    _ayclub_max_pages: int = 5

    _save_path: str = "/我的接收/MoviePilot/TV"
    _movie_save_path: str = "/我的接收/MoviePilot/Movie"
    _only_115: bool = True
    # 订阅过滤模式："exclude" 排除模式（处理除勾选外的全部订阅）/ "include" 指定模式（仅处理勾选的订阅）
    _subscribe_filter_mode: str = "exclude"
    _exclude_subscribes: List[int] = []
    _include_subscribes: List[int] = []
    # 搜索源优先级（按列表顺序），为空时默认 Nullbr > HDHive > PanSou
    _search_source_order: List[str] = []

    _nullbr_enabled: bool = False
    _nullbr_appid: str = ""
    _nullbr_api_key: str = ""

    _hdhive_enabled: bool = False
    _hdhive_username: str = ""
    _hdhive_password: str = ""
    _hdhive_cookie: str = ""
    _hdhive_auto_refresh: bool = False
    _hdhive_refresh_before: int = 86400
    _hdhive_query_mode: str = "api"
    # OpenAPI 应用凭证：应用 Secret 放 X-API-Key（沿用 hdhive_api_key 配置键）
    _hdhive_api_key: str = ""
    _hdhive_client_id: str = ""
    _hdhive_redirect_uri: str = ""
    # OAuth 用户授权（授权码为一次性输入，换取 Token 后自动清空）
    _hdhive_auth_code: str = ""
    _hdhive_access_token: str = ""
    _hdhive_refresh_token: str = ""
    _hdhive_token_expires_at: float = 0
    _hdhive_auto_unlock: bool = False
    _hdhive_max_unlock_points: int = 50
    _hdhive_max_points_per_sub: int = 20

    # 是否屏蔽系统订阅（True=已屏蔽系统订阅，False=已恢复系统订阅）
    _block_system_subscribe: bool = False

    _max_transfer_per_sync: int = 50
    _batch_size: int = 20
    _skip_other_season_dirs: bool = True
    
    # OpenClaw 七分类服务
    _classifier_enabled: bool = False
    _classifier_url: str = ""
    _classifier_token: str = ""
    _classifier_timeout: int = 120

    # 窗口配置：站点/延迟/窗口期
    _unblock_site_ids: List[int] = []
    _unblock_site_names: List[str] = []
    _unblock_delay_minutes: int = 5          # -1 禁用触发条件1（并视为禁用窗口）
    _system_subscribe_window_hours: float = 1.0  # 0 禁用窗口

    # 运行时对象
    _pansou_client: Optional[PanSouClient] = None
    _p115_manager: Optional[P115ClientManager] = None
    _nullbr_client: Optional[NullbrClient] = None
    _hdhive_client: Optional[Any] = None
    _ayclub_client: Optional[AyclubClient] = None
    _classifier_client: Optional[OpenClawClassifierClient] = None
    
    # 处理器
    _search_handler: Optional[SearchHandler] = None
    _subscribe_handler: Optional[SubscribeHandler] = None
    _sync_handler: Optional[SyncHandler] = None
    _api_handler: Optional[ApiHandler] = None
    _lifecycle_store: Optional[LifecycleStore] = None

    _MIN_INTERVAL_HOURS: int = 8
    _PT_GATE_RECHECK_SECONDS: int = 300
    _PT_GATE_MAX_WAIT_MINUTES: int = 90

    # ------------------ 调度器 ------------------

    def _ensure_toggle_scheduler(self):
        if not self._toggle_scheduler:
            self._toggle_scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._toggle_scheduler.start()

    def _cancel_toggle_jobs(self):
        if not self._toggle_scheduler:
            return
        for job_id in [
            "p115_unblock_job",
            "p115_reblock_job",
            "p115_pt_gate_job",
        ]:
            try:
                self._toggle_scheduler.remove_job(job_id)
            except Exception:
                pass

    # ------------------ cron间隔校验 ------------------

    @staticmethod
    def _cron_interval_ge_min_hours(cron_expr: str, min_hours: int) -> bool:
        cron_expr = (cron_expr or "").strip()
        if not cron_expr:
            return False
        try:
            tz = pytz.timezone(settings.TZ)
            trigger = CronTrigger.from_crontab(cron_expr, timezone=tz)
        except Exception:
            return False

        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        fire_times: List[datetime.datetime] = []
        prev = None
        current = now
        for _ in range(12):
            nxt = trigger.get_next_fire_time(prev, current)
            if not nxt:
                break
            fire_times.append(nxt)
            prev = nxt
            current = nxt + datetime.timedelta(seconds=1)

        if len(fire_times) < 2:
            return True

        min_delta = min(fire_times[i + 1] - fire_times[i] for i in range(len(fire_times) - 1))
        return min_delta >= datetime.timedelta(hours=min_hours)

    @staticmethod
    def _safe_int(
        value: Any,
        default: int,
        field_name: str,
    ) -> int:
        """安全读取整数配置，格式错误时回退默认值。"""
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            logger.warning(
                f"配置 {field_name}={value!r} 不是有效整数，"
                f"已回退默认值 {default}"
            )
            return int(default)

    @staticmethod
    def _safe_float(
        value: Any,
        default: float,
        field_name: str,
    ) -> float:
        """安全读取有限浮点数，格式错误时回退默认值。"""
        try:
            parsed = float(value)

            if not math.isfinite(parsed):
                raise ValueError("non-finite number")

            return parsed
        except (TypeError, ValueError, OverflowError):
            logger.warning(
                f"配置 {field_name}={value!r} 不是有效数字，"
                f"已回退默认值 {default}"
            )
            return float(default)
    # ------------------ 站点解析 ------------------

    def _load_site_records(self) -> List[Dict[str, Any]]:
        with SessionFactory() as db:
            rows = db.execute(text("SELECT id, name, is_active FROM site")).fetchall()
        out = []
        for r in rows:
            out.append({"id": int(r[0]), "name": str(r[1]), "is_active": bool(r[2])})
        return out

    def _resolve_site_ids(self, ids: Optional[List[int]] = None, names: Optional[List[str]] = None) -> List[int]:
        ids = ids or []
        names = names or []

        site_records = self._load_site_records()
        by_name = {s["name"]: s for s in site_records}
        by_id = {s["id"]: s for s in site_records}

        final_ids: List[int] = []
        for sid in ids:
            if sid in by_id:
                final_ids.append(sid)
            else:
                logger.warning(f"站点ID不存在：id={sid}（将跳过）")

        for nm in names:
            rec = by_name.get(nm)
            if not rec:
                logger.warning(f"站点名称不存在：name={nm}（将跳过）")
                continue
            final_ids.append(int(rec["id"]))

        seen = set()
        uniq = []
        for x in final_ids:
            if x not in seen:
                seen.add(x)
                uniq.append(x)

        mapped = []
        for x in uniq:
            rec = by_id.get(x, {})
            mapped.append(f"{rec.get('name','?')}({x})")
        logger.info(f"订阅站点解析结果：ids={uniq} | 映射={mapped}")
        return uniq

    def _ensure_115_site_id(self, db=None) -> int:
        """
        确保 115网盘 站点存在并返回 ID
        :param db: 可选的数据库会话，若未传入则创建新会话
        """
        def _do_ensure(session):
            row = session.execute(
                text("SELECT id, is_active FROM site WHERE name=:n LIMIT 1"),
                {"n": "115网盘"}
            ).fetchone()
            if row and row[0] is not None:
                site_id = int(row[0])
                if bool(row[1]):
                    session.execute(
                        text("UPDATE site SET is_active=FALSE WHERE id=:i"),
                        {"i": site_id}
                    )
                    session.commit()
                    logger.info(f"已禁用115虚拟站点：id={site_id}")
                return site_id

            # existing = Site.get(session, -1)
            row_ex = session.execute(text("SELECT id FROM site WHERE id=:i"), {"i": -1}).fetchone()
            if not row_ex:
                session.execute(
                    text(
                        "INSERT INTO site (id, name, url, is_active, limit_interval, limit_count, limit_seconds, timeout) "
                        "VALUES (:id, :name, :url, :is_active, :limit_interval ,:limit_count, :limit_seconds, :timeout)"
                    ),
                    {
                        "id": -1,
                        "name": "115网盘",
                        "url": "https://115.com",
                        "is_active": False,
                        "limit_interval": 10000000,
                        "limit_count": 1,
                        "limit_seconds": 10000000,
                        "timeout": 1
                    }
                )
                session.commit()
                logger.info("已插入站点记录：115网盘(id=-1)")
            return -1

        if db is not None:
            return _do_ensure(db)
        else:
            with SessionFactory() as new_db:
                return _do_ensure(new_db)

    def _is_subscribe_excluded(self, subscribe_id: int) -> bool:
        """
        按订阅过滤模式判断订阅是否不归本插件处理

        - exclude 排除模式：勾选的订阅被排除，其余全部处理
        - include 指定模式：仅处理勾选的订阅，其余全部排除
        """
        if self._subscribe_filter_mode == "include":
            return subscribe_id not in set(self._include_subscribes or [])
        return subscribe_id in set(self._exclude_subscribes or [])

    def _apply_sites_to_all_subscribes(self, site_ids: List[int], reason: str):
        """ 应用站点ID到所有订阅 """
        with SessionFactory() as db:
            # 复用 SubscribeOper 实例，避免循环中重复创建
            subscribe_oper = SubscribeOper(db=db)
            subs = subscribe_oper.list() or []
            updated = 0
            excluded = 0
            for s in subs:
                if self._is_subscribe_excluded(s.id):
                    excluded += 1
                    continue
                subscribe_oper.update(s.id, {"sites": site_ids})
                updated += 1
        logger.info(f"{reason}：已更新 {updated} 个订阅（跳过 {excluded} 个排除订阅）")

    # ------------------ 禁用窗口判断 ------------------

    def _window_disabled(self) -> bool:
        # 站点空 / 窗口=0 / delay=-1 => 始终保持屏蔽，不安排任何进入已恢复状态任务
        if not self._unblock_site_names:
            return True
        if float(self._system_subscribe_window_hours or 0) <= 0:
            return True
        if int(self._unblock_delay_minutes) < 0:
            return True
        return False

    def _window_enabled(self) -> bool:
        return not self._window_disabled()

    def _set_system_rss_sites(self, site_ids: List[int], reason: str) -> bool:
        """
        精确设置 MoviePilot 系统默认订阅站点 RssSites。

        屏蔽状态传入 [-1]，彻底阻止原生 PT 抢跑；
        恢复状态传入用户配置的 PT 站点列表。
        """
        try:
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey

            normalized = []
            for value in site_ids or []:
                try:
                    site_id = int(value)
                except (TypeError, ValueError):
                    continue

                if site_id not in normalized:
                    normalized.append(site_id)

            SystemConfigOper().set(
                SystemConfigKey.RssSites,
                normalized
            )

            logger.info(
                f"已设置系统默认订阅站点："
                f"站点数量={len(normalized)}，原因={reason}"
            )
            return True

        except Exception as e:
            logger.error(f"设置系统默认订阅站点失败：{e}")
            return False

    # ------------------ 系统默认订阅站点：只在已恢复系统订阅时尝试 ------------------

    def _try_set_default_sites_for_unblocked(self, site_ids: List[int]):
        """
        只在“已恢复系统订阅”时尝试设置系统默认订阅站点为窗口站点。
        若系统不存在对应key，会静默失败，不影响订阅 sites 已更新。
        """
        try:
            from app.db.systemconfig_oper import SystemConfigOper
        except Exception:
            return

        def _build_oper(db):
            try:
                return SystemConfigOper(db)
            except Exception:
                try:
                    return SystemConfigOper(db=db)
                except Exception:
                    return None

        candidate_keys = [
            "subscribe_sites",
            "subscribe_site_ids",
            "system_subscribe_sites",
            "system_subscribe_site_ids",
            "subscribe_sites_selected",
        ]

        with SessionFactory() as db:
            oper = _build_oper(db)
            if not oper:
                return
            get_fn = getattr(oper, "get", None) or getattr(oper, "get_by_key", None)
            set_fn = getattr(oper, "set", None) or getattr(oper, "set_by_key", None)
            if not get_fn or not set_fn:
                return

            for k in candidate_keys:
                try:
                    cur = get_fn(k)
                except Exception:
                    cur = None
                if cur is None:
                    continue
                try:
                    set_fn(k, site_ids)
                    logger.info(f"已恢复系统订阅：已尝试同步默认订阅站点 key={k} value={site_ids}")
                    break
                except Exception:
                    continue

    # ------------------ 两态切换（日志统一） ------------------

    def _enter_blocked(self, reason: str):
        """
        已屏蔽系统订阅：
        - 全量订阅 sites=仅115
        - 不再尝试设置屏蔽态默认站点=115（依赖 SubscribeAdded 兜底）
        - 取消所有窗口任务
        """
        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()
        self._init_subscribe_handler()

        if not self._set_system_rss_sites(
            [-1],
            reason=f"进入屏蔽状态：{reason}"
        ):
            logger.error("进入屏蔽状态失败：无法设置 RssSites=[-1]")
            return

        self._subscribe_handler.set_blocked_sites_only_115()
        self._block_system_subscribe = True
        self.__update_config()
        logger.info(f"已屏蔽系统订阅（仅115网盘）：{reason}")

    def _enter_unblocked(self, reason: str):
        """
        已恢复系统订阅：
        - 全量订阅 sites=UI站点
        - 尽力设置系统默认订阅站点=UI站点（若存在key）
        - 从进入时刻计窗口，到期切回屏蔽
        """
        if not self._window_enabled():
            self._block_system_subscribe = True
            self.__update_config()
            self._enter_blocked(reason=f"{reason}（窗口禁用）")
            return

        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()
        self._init_subscribe_handler()

        site_ids = self._resolve_site_ids(ids=self._unblock_site_ids, names=self._unblock_site_names)
        if not site_ids:
            self._block_system_subscribe = True
            self.__update_config()
            self._enter_blocked(reason=f"{reason}（站点解析失败）")
            return

        if not self._set_system_rss_sites(
            site_ids,
            reason=f"进入恢复窗口：{reason}"
        ):
            logger.error("恢复系统订阅失败：无法设置 PT 默认订阅站点")
            self._enter_blocked(reason=f"{reason}（恢复RssSites失败）")
            return

        self._apply_sites_to_all_subscribes(
            site_ids,
            reason="已恢复系统订阅：全量同步站点"
        )

        self._block_system_subscribe = False
        self.__update_config()
        logger.info(f"已恢复系统订阅：站点={self._unblock_site_names} 窗口期={self._system_subscribe_window_hours}h（{reason}）")

        self._schedule_reblock_after_window()

    def _schedule_reblock_after_window(self):
        hours = float(self._system_subscribe_window_hours or 0)
        if hours <= 0:
            return

        tz = pytz.timezone(settings.TZ)
        now = datetime.datetime.now(tz=tz)
        run_date = now + datetime.timedelta(hours=hours)

        self._toggle_scheduler.add_job(
            func=lambda: self._enter_blocked(reason="窗口到期"),
            trigger="date",
            run_date=run_date,
            id="p115_reblock_job",
            replace_existing=True
        )
        logger.info(f"已安排：{run_date} 切换为已屏蔽系统订阅（仅115网盘）")

    def _schedule_unblock_after_delay(self, base_time: datetime.datetime):
        """最后一次全量任务后，先通过 MP/媒体库入库屏障再开放 PT。"""
        delay = int(self._unblock_delay_minutes)
        if delay < 0:
            return
        if not self._window_enabled():
            return

        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()

        tz = pytz.timezone(settings.TZ)
        base_time = base_time.astimezone(tz)
        run_date = base_time + datetime.timedelta(minutes=delay)
        deadline = run_date + datetime.timedelta(
            minutes=self._PT_GATE_MAX_WAIT_MINUTES
        )

        self._toggle_scheduler.add_job(
            func=self._check_pt_unblock_gate,
            trigger="date",
            run_date=run_date,
            kwargs={
                "deadline_text": deadline.isoformat(),
                "attempt": 1,
            },
            id="p115_pt_gate_job",
            replace_existing=True,
        )
        logger.info(
            f"已安排 PT 开放前入库屏障：首次检查={run_date}，"
            f"最长等待={self._PT_GATE_MAX_WAIT_MINUTES}分钟，"
            f"初始延迟={delay}分钟"
        )

    @staticmethod
    def _pt_gate_task_label(task: Dict[str, Any]) -> str:
        subscribe_id = task.get("subscribe_id")
        episode = task.get("episode")
        status = task.get("status") or "unknown"
        suffix = f"/E{int(episode):02d}" if episode is not None else "/movie"
        return f"sub={subscribe_id}{suffix}:{status}"

    def _refresh_pt_gate_with_mp(self, subscribe_ids: List[int]) -> None:
        """只问 MoviePilot/媒体库，不登录115、不调用任何搜索源。"""
        self._init_lifecycle_store()
        if not self._sync_handler:
            self._init_handlers()

        try:
            with SessionFactory() as db:
                active_subscribes = SubscribeOper(db=db).list("N,R") or []
            self._lifecycle_store.reconcile_active(active_subscribes)
            active_by_id = {
                int(subscribe.id): subscribe
                for subscribe in active_subscribes
                if getattr(subscribe, "id", None) is not None
            }
        except Exception as error:
            logger.warning(
                f"PT开放前读取活动订阅失败，继续保持屏蔽：{error}"
            )
            return

        for sid in subscribe_ids:
            subscribe = active_by_id.get(int(sid))
            if not subscribe:
                logger.info(
                    f"PT开放前订阅 {sid} 已不在活动列表，不再阻塞窗口"
                )
                continue
            try:
                self._sync_handler.reconcile_subscribe_with_mp(subscribe)
            except Exception as error:
                logger.warning(
                    f"PT开放前定向确认订阅 {sid} 失败，继续保持屏蔽：{error}"
                )

    def _check_pt_unblock_gate(self, deadline_text: str, attempt: int = 1):
        """串行执行 PT 开放前入库屏障，避免与同步/事件对账竞态。"""
        with lock:
            self._check_pt_unblock_gate_locked(deadline_text, attempt)

    def _check_pt_unblock_gate_locked(
        self,
        deadline_text: str,
        attempt: int = 1,
    ):
        """仅当 MP/Emby 不再认为本插件刚投递的内容缺失时才开放 PT。"""
        if not self._enabled or not self._window_enabled():
            return
        if not self._block_system_subscribe:
            return

        tz = pytz.timezone(settings.TZ)
        now = datetime.datetime.now(tz=tz)
        try:
            deadline = datetime.datetime.fromisoformat(str(deadline_text))
            if deadline.tzinfo is None:
                deadline = tz.localize(deadline)
            else:
                deadline = deadline.astimezone(tz)
        except (TypeError, ValueError):
            deadline = now + datetime.timedelta(
                minutes=self._PT_GATE_MAX_WAIT_MINUTES
            )

        self._init_lifecycle_store()
        blocking = self._lifecycle_store.blocking_pending_tasks()
        if blocking:
            subscribe_ids = sorted({
                int(task.get("subscribe_id"))
                for task in blocking
                if task.get("subscribe_id") is not None
            })
            logger.info(
                f"PT开放前入库屏障第 {attempt} 次检查："
                f"仍有 {len(blocking)} 个活动在途任务，"
                f"向 MoviePilot/媒体库定向复核订阅 {subscribe_ids}"
            )
            self._refresh_pt_gate_with_mp(subscribe_ids)
            blocking = self._lifecycle_store.blocking_pending_tasks()
            now = datetime.datetime.now(tz=tz)

        if not blocking:
            logger.info(
                "PT开放前入库屏障已通过：MoviePilot/媒体库已确认所有活动在途任务，"
                "现在开放系统订阅窗口"
            )
            self._enter_unblocked(
                reason="最后一次任务：MP/Emby入库屏障已通过"
            )
            return

        if now >= deadline:
            labels = [self._pt_gate_task_label(task) for task in blocking[:12]]
            logger.warning(
                f"PT开放前入库屏障等待超过 {self._PT_GATE_MAX_WAIT_MINUTES} 分钟，"
                f"当晚跳过 PT 窗口，避免重复下载；"
                f"仍在途={labels}"
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】已跳过本次PT窗口",
                    text=(
                        "MoviePilot/媒体库在等待上限内仍未确认本轮入库，"
                        "为避免重复下载，本次保持系统订阅屏蔽。"
                    ),
                )
            self._enter_blocked(reason="PT开放前入库屏障超时")
            return

        next_run = now + datetime.timedelta(
            seconds=self._PT_GATE_RECHECK_SECONDS
        )
        labels = [self._pt_gate_task_label(task) for task in blocking[:12]]
        self._toggle_scheduler.add_job(
            func=self._check_pt_unblock_gate,
            trigger="date",
            run_date=next_run,
            kwargs={
                "deadline_text": deadline.isoformat(),
                "attempt": int(attempt) + 1,
            },
            id="p115_pt_gate_job",
            replace_existing=True,
        )
        logger.info(
            f"PT开放前入库屏障未通过，继续保持屏蔽；"
            f"仍在途={labels}，下次检查={next_run}"
        )

    # ------------------ 触发条件1：最后一次任务判断 ------------------

    def _is_last_run_today(self, run_start: datetime.datetime) -> bool:
        """判断当前运行是否是今天的最后一次任务"""
        try:
            tz = pytz.timezone(settings.TZ)
            run_start = run_start.astimezone(tz)
            trigger = CronTrigger.from_crontab(self._cron, timezone=tz)
            nxt = trigger.get_next_fire_time(None, run_start + datetime.timedelta(seconds=1))
            if not nxt:
                logger.debug(f"判断最后一次任务：无下次触发时间，返回 False")
                return False
            is_last = nxt.date() != run_start.date()
            logger.debug(f"判断最后一次任务：当前={run_start.strftime('%Y-%m-%d %H:%M')}, 下次={nxt.strftime('%Y-%m-%d %H:%M')}, 是否最后一次={is_last}")
            return is_last
        except Exception as e:
            logger.warning(f"判断是否当天最后一次触发失败：{e}，按 23:00 兜底")
            return run_start.hour == 23 and run_start.minute == 00

    # ------------------ MoviePilot 生命周期联动 ------------------

    def _init_lifecycle_store(self):
        if not self._lifecycle_store:
            self._lifecycle_store = LifecycleStore(
                get_data_func=self.get_data,
                save_data_func=self.save_data,
            )

    @staticmethod
    def _event_value(data: Any, key: str, default: Any = None) -> Any:
        if isinstance(data, dict):
            return data.get(key, default)
        return getattr(data, key, default)

    def _schedule_lifecycle_sync(
        self,
        reason: str,
        delay_seconds: int = 5,
        subscribe_ids: Optional[List[int]] = None,
    ):
        """事件只定向加速关联订阅；定时全量同步负责最终一致性。"""
        if not self._enabled:
            return

        targeted_ids: List[int] = []
        if subscribe_ids is not None:
            for value in subscribe_ids:
                try:
                    sid = int(value)
                except (TypeError, ValueError):
                    continue
                if sid > 0 and sid not in targeted_ids:
                    targeted_ids.append(sid)
            targeted_ids.sort()
            if not targeted_ids:
                logger.info(
                    f"MoviePilot 生命周期联动没有可用订阅 ID，跳过即时同步：{reason}"
                )
                return

        try:
            self._ensure_toggle_scheduler()
            run_date = (
                datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                + datetime.timedelta(seconds=max(1, delay_seconds))
            )
            if targeted_ids:
                digest = hashlib.sha256(
                    ",".join(str(sid) for sid in targeted_ids).encode("utf-8")
                ).hexdigest()[:12]
                job_id = f"p115_lifecycle_sync_{digest}"
            else:
                job_id = "p115_lifecycle_sync_full"

            self._toggle_scheduler.add_job(
                func=self.sync_subscribes,
                trigger="date",
                run_date=run_date,
                kwargs={
                    "target_subscribe_ids": targeted_ids or None,
                    "trigger_reason": reason,
                },
                id=job_id,
                replace_existing=True,
            )
            if targeted_ids:
                logger.info(
                    f"已安排 MoviePilot 生命周期定向同步：subscribe_ids={targeted_ids}，"
                    f"原因={reason}"
                )
            else:
                logger.info(f"已安排 MoviePilot 生命周期全量同步：{reason}")
        except Exception as error:
            logger.warning(f"安排生命周期联动同步失败：{error}")

    def _invalidate_subscribe_caches(self, subscribe_info: Any):
        """重置订阅时只清插件/桥接缓存，不改 MoviePilot 数据。"""
        if not subscribe_info:
            return
        tmdb_id = self._event_value(subscribe_info, "tmdbid") or self._event_value(subscribe_info, "tmdb_id")
        media_type = self._event_value(subscribe_info, "type")
        season = self._event_value(subscribe_info, "season")
        name = self._event_value(subscribe_info, "name", "")
        type_text = str(getattr(media_type, "value", media_type) or "")
        is_tv = type_text == MediaType.TV.value or type_text.lower() == "tv"
        sub_key = (
            f"tmdb_{tmdb_id}_S{season or 1}"
            if is_tv and tmdb_id
            else f"tmdb_{tmdb_id}_movie"
            if tmdb_id
            else f"{name}_S{season or 1}"
            if is_tv
            else f"{name}_movie"
        )
        try:
            if self._search_handler and hasattr(self._search_handler, "clear_sub_points"):
                self._search_handler.clear_sub_points(sub_key)
        except Exception as error:
            logger.warning(f"清理 HDHive 订阅缓存失败：{error}")
        try:
            if self._sync_handler and hasattr(self._sync_handler, "invalidate_subscription_caches"):
                self._sync_handler.invalidate_subscription_caches(
                    media_type=("tv" if is_tv else "movie"),
                    tmdb_id=tmdb_id,
                    season=season,
                )
        except Exception as error:
            logger.warning(f"清理发布门禁缓存失败：{error}")
        try:
            if self._ayclub_client and hasattr(self._ayclub_client, "invalidate_cache"):
                self._ayclub_client.invalidate_cache(
                    tmdb_id=tmdb_id,
                    media_type=("tv" if is_tv else "movie"),
                    season=season,
                )
        except Exception as error:
            logger.warning(f"清理 AYCLUB 桥接缓存失败：{error}")

    # ------------------ MoviePilot 订阅生命周期事件 ------------------

    def _get_subscribe_id_from_event(self, event: Event) -> Optional[int]:
        if not event or not event.event_data:
            return None
        data = event.event_data or {}
        subscribe_id = data.get("subscribe_id") or data.get("id")
        if not subscribe_id and isinstance(data.get("subscribe"), dict):
            subscribe_id = data["subscribe"].get("id")
        try:
            return int(subscribe_id) if subscribe_id is not None else None
        except Exception:
            return None

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: Event):
        """
        保留：新订阅兜底
        - 已屏蔽系统订阅时：新订阅必拉回仅115
        - 已恢复系统订阅时：新订阅同步窗口站点（保持一致）
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        if self._is_subscribe_excluded(sid):
            logger.info(f"新增订阅不在本插件处理范围（订阅过滤模式：{self._subscribe_filter_mode}），跳过站点同步（subscribe_id={sid}）")
            return
        self._init_lifecycle_store()
        event_data = event.event_data or {}
        subscribe_info = event_data.get("subscribe_info") if isinstance(event_data, dict) else None
        if subscribe_info is None:
            try:
                with SessionFactory() as db:
                    subscribe_info = SubscribeOper(db=db).get(sid)
            except Exception:
                subscribe_info = None
        lifecycle_record = self._lifecycle_store.on_added(sid, subscribe_info)
        if lifecycle_record.get("force_refresh"):
            self._invalidate_subscribe_caches(subscribe_info)
            logger.info(
                f"新增/重新订阅已开启新查询周期：subscribe_id={sid}，"
                "已清本周期门禁缓存并要求 AYCLUB 首次查询强制刷新"
            )
        self._schedule_lifecycle_sync(f"新增/重新订阅 {sid}", subscribe_ids=[sid])
        try:
            self._init_subscribe_handler()

            if self._block_system_subscribe:
                if hasattr(self._subscribe_handler, "set_sites_for_subscribe_only_115"):
                    self._subscribe_handler.set_sites_for_subscribe_only_115(sid)
                else:
                    # 兜底：使用统一的 db session
                    with SessionFactory() as db:
                        site_id_115 = self._ensure_115_site_id(db)
                        SubscribeOper(db=db).update(sid, {"sites": [site_id_115]})
                logger.info(f"已屏蔽系统订阅：新增订阅已拉回仅115（subscribe_id={sid}）")
            else:
                if self._window_enabled() and hasattr(self._subscribe_handler, "set_sites_for_subscribe_by_names"):
                    self._subscribe_handler.set_sites_for_subscribe_by_names(sid, self._unblock_site_names)
                    logger.info(f"已恢复系统订阅：新增订阅已同步窗口站点（subscribe_id={sid})")

        except Exception as e:
            logger.error(f"SubscribeAdded 兜底失败：{e}")

    @eventmanager.register(EventType.SubscribeModified)
    def on_subscribe_modified(self, event: Event):
        """响应 MP 普通修改、状态变化、重置和 Agent 更新。"""
        sid = self._get_subscribe_id_from_event(event)
        if not sid or self._is_subscribe_excluded(sid):
            return
        data = event.event_data or {}
        scene = str(data.get("scene") or "update") if isinstance(data, dict) else "update"
        fields = list(data.get("fields") or []) if isinstance(data, dict) else []
        subscribe_info = data.get("subscribe_info") if isinstance(data, dict) else None
        self._init_lifecycle_store()
        record = self._lifecycle_store.on_modified(
            sid,
            scene=scene,
            subscribe_info=subscribe_info,
            fields=fields,
        )
        logger.info(
            f"收到 MP 订阅修改：subscribe_id={sid}, scene={scene}, "
            f"fields={fields}, generation={record.get('generation')}"
        )
        if scene == "reset":
            self._invalidate_subscribe_caches(subscribe_info)
        self._schedule_lifecycle_sync(f"订阅修改 {sid}/{scene}", subscribe_ids=[sid])

    @eventmanager.register(EventType.SubscribeDeleted)
    def on_subscribe_deleted(self, event: Event):
        """MP 删除/取消订阅后立即停止本插件继续搜索。"""
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        data = event.event_data or {}
        subscribe_info = data.get("subscribe_info") if isinstance(data, dict) else None
        self._init_lifecycle_store()
        self._lifecycle_store.on_deleted(sid, subscribe_info)
        logger.info(f"收到 MP 订阅取消/删除：subscribe_id={sid}")

    @eventmanager.register(EventType.SubscribeComplete)
    def on_subscribe_complete(self, event: Event):
        """订阅完成只接受 MP 的正式完成事件。"""
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        data = event.event_data or {}
        subscribe_info = data.get("subscribe_info") if isinstance(data, dict) else None
        self._init_lifecycle_store()
        self._lifecycle_store.on_complete(sid, subscribe_info)
        logger.info(f"收到 MP 订阅完成：subscribe_id={sid}")

    def _handle_transfer_event(self, event: Event, success: bool):
        data = event.event_data or {}
        if not isinstance(data, dict):
            return
        mediainfo = data.get("mediainfo")
        meta = data.get("meta")
        if not mediainfo:
            return
        self._init_lifecycle_store()
        if success:
            matched = self._lifecycle_store.mark_transfer_complete(mediainfo, meta)
        else:
            transferinfo = data.get("transferinfo")
            reason = self._event_value(transferinfo, "message", "MoviePilot 整理失败")
            matched = self._lifecycle_store.mark_transfer_failed(mediainfo, meta, str(reason or ""))
        media_key = self._lifecycle_store.media_key_from_event(mediainfo, meta)
        logger.info(
            f"收到 MP {'入库完成' if success else '整理失败'}事件："
            f"media_key={media_key}, 匹配在途任务={len(matched)}"
        )

        # 整理事件只被动确认插件自己的在途状态。
        # 不刷新 MoviePilot 订阅、不触发即时同步，也不再次搜索或转存。
        # 后续是否补回仅由既有定时同步读取 MoviePilot 缺集口径后决定。
        if matched:
            logger.info(
                f"已被动更新在途状态，不接管后续整理链路：media_key={media_key}"
            )

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        self._handle_transfer_event(event, True)

    @eventmanager.register(EventType.TransferFailed)
    def on_transfer_failed(self, event: Event):
        self._handle_transfer_event(event, False)

    @eventmanager.register(ChainEventType.PluginDataReset)
    def on_plugin_data_reset(self, event: Event):
        """MP 清理本插件配置/数据前，停止本插件自己的调度与在内存中的任务。"""
        data = event.event_data or {}
        if not isinstance(data, dict):
            return
        plugin_id = str(data.get("plugin_id") or "")
        if plugin_id != self.__class__.__name__:
            return
        logger.info(
            f"收到 MP 插件数据重置通知：reset_config={bool(data.get('reset_config'))}, "
            f"reset_data={bool(data.get('reset_data'))}"
        )
        # 只停止本插件服务，不触碰 MoviePilot 本体、订阅或媒体库数据。
        self.stop_service()
        self._lifecycle_store = None

    # ------------------ init_plugin ------------------

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._ensure_toggle_scheduler()
        download_so_file(Path(__file__).parent / "lib")

        if config:
            self._enabled = config.get("enabled", False)

            configured_cron = (config.get("cron", self._cron) or "").strip()
            if configured_cron == "30 2,10,18 * * *":
                self._cron = "30 6,14,22 * * *"
                logger.info(
                    "检测到旧默认计划 02:30/10:30/18:30，"
                    "已迁移为 06:30/14:30/22:30"
                )
            else:
                self._cron = configured_cron
            if self._cron:
                ok = self._cron_interval_ge_min_hours(self._cron, self._MIN_INTERVAL_HOURS)
                if not ok:
                    logger.warning(
                        f"Cron 过于频繁（要求间隔>= {self._MIN_INTERVAL_HOURS}h）：{self._cron}，已回退默认 30 6,14,22 * * *"
                    )
                    self._cron = "30 6,14,22 * * *"

            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cookies = config.get("cookies", "")

            self._pansou_enabled = config.get("pansou_enabled", True)
            self._pansou_url = config.get("pansou_url", "https://so.252035.xyz/")
            self._pansou_username = config.get("pansou_username", "")
            self._pansou_password = config.get("pansou_password", "")
            self._pansou_auth_enabled = config.get("pansou_auth_enabled", False)
            self._pansou_channels = config.get("pansou_channels", "QukanMovie")
            # AYCLUB Telegram 桥接配置
            self._ayclub_enabled = bool(
                config.get("ayclub_enabled", False)
            )
            self._ayclub_url = (
                config.get(
                    "ayclub_url",
                    "http://127.0.0.1:11592",
                )
                or ""
            ).strip()
            self._ayclub_timeout = self._safe_int(
                config.get("ayclub_timeout", 120),
                120,
                "ayclub_timeout",
            )
            self._ayclub_max_pages = min(
                max(
                    self._safe_int(
                        config.get("ayclub_max_pages", 5),
                        5,
                        "ayclub_max_pages",
                    ),
                    1,
                ),
                10,
            )
            self._save_path = config.get("save_path", "/我的接收/MoviePilot/TV")
            self._movie_save_path = config.get("movie_save_path", "/我的接收/MoviePilot/Movie")
            self._only_115 = config.get("only_115", True)
            self._subscribe_filter_mode = config.get("subscribe_filter_mode", "exclude") or "exclude"
            self._exclude_subscribes = config.get("exclude_subscribes", []) or []
            self._include_subscribes = config.get("include_subscribes", []) or []
            if self._subscribe_filter_mode == "include":
                logger.info(f"订阅过滤模式：指定模式，仅处理 {len(self._include_subscribes)} 个勾选订阅")

            self._nullbr_enabled = config.get("nullbr_enabled", False)
            self._nullbr_appid = config.get("nullbr_appid", "")
            self._nullbr_api_key = config.get("nullbr_api_key", "")

            self._hdhive_enabled = config.get("hdhive_enabled", False)
            self._hdhive_query_mode = config.get("hdhive_query_mode", "api")
            self._hdhive_api_key = (config.get("hdhive_api_key", "") or "").strip()
            self._hdhive_client_id = (config.get("hdhive_client_id", "") or "").strip()
            self._hdhive_redirect_uri = (config.get("hdhive_redirect_uri", "") or "").strip()
            self._hdhive_auth_code = (config.get("hdhive_auth_code", "") or "").strip()
            self._hdhive_access_token = config.get("hdhive_access_token", "")
            self._hdhive_refresh_token = config.get("hdhive_refresh_token", "")
            self._hdhive_token_expires_at = self._safe_float(
                config.get("hdhive_token_expires_at", 0),
                0,
                "hdhive_token_expires_at",
            )
            self._hdhive_auto_unlock = config.get("hdhive_auto_unlock", False)
            self._hdhive_max_unlock_points = self._safe_int(
                config.get("hdhive_max_unlock_points", 50),
                50,
                "hdhive_max_unlock_points",
            )
            self._hdhive_max_points_per_sub = self._safe_int(
                config.get("hdhive_max_points_per_sub", 20),
                20,
                "hdhive_max_points_per_sub",
            )
            self._hdhive_username = config.get("hdhive_username", "")
            self._hdhive_password = config.get("hdhive_password", "")
            self._hdhive_cookie = config.get("hdhive_cookie", "")
            self._hdhive_auto_refresh = config.get("hdhive_auto_refresh", False)
            self._hdhive_refresh_before = self._safe_int(
                config.get("hdhive_refresh_before", 86400),
                86400,
                "hdhive_refresh_before",
            )
            self._max_transfer_per_sync = self._safe_int(
                config.get("max_transfer_per_sync", 50),
                50,
                "max_transfer_per_sync",
            )
            self._batch_size = self._safe_int(
                config.get("batch_size", 20),
                20,
                "batch_size",
            )
            self._skip_other_season_dirs = config.get("skip_other_season_dirs", True)
            
            # OpenClaw 七分类服务配置
            self._classifier_enabled = bool(
                config.get("classifier_enabled", False)
            )
            self._classifier_url = (
                config.get(
                    "classifier_url",
                    "",
                )
                or ""
            ).strip()
            self._classifier_token = (
                config.get("classifier_token", "")
                or ""
            ).strip()
            self._classifier_timeout = self._safe_int(
                config.get("classifier_timeout", 120),
                120,
                "classifier_timeout",
            )
            
            # 搜索源优先级（兼容逗号分隔字符串）
            raw_order = config.get("search_source_order", []) or []
            if isinstance(raw_order, str):
                self._search_source_order = [x.strip() for x in raw_order.split(",") if x.strip()]
            else:
                self._search_source_order = list(raw_order)
            if self._search_source_order:
                logger.info(f"搜索源自定义优先级：{' > '.join(self._search_source_order)}")

            # UI新增配置
            self._unblock_site_ids = config.get("unblock_site_ids", []) or []
            raw_sites = config.get("unblock_site_names", self._unblock_site_names)
            if isinstance(raw_sites, str):
                self._unblock_site_names = [x.strip() for x in raw_sites.split(",") if x.strip()]
            else:
                self._unblock_site_names = raw_sites or []

            self._unblock_delay_minutes = self._safe_int(
                config.get(
                    "unblock_delay_minutes",
                    self._unblock_delay_minutes,
                ),
                self._unblock_delay_minutes,
                "unblock_delay_minutes",
            )
            self._system_subscribe_window_hours = self._safe_float(
                config.get(
                    "unblock_window_hours",
                    config.get(
                        "system_subscribe_window_hours",
                        self._system_subscribe_window_hours,
                    ),
                ),
                self._system_subscribe_window_hours,
                "unblock_window_hours",
            )
            self._block_system_subscribe = bool(config.get("block_system_subscribe", False))

        # 初始化客户端/handlers
        self._init_clients()
        self._init_handlers()

        # 配置立即生效
        if self._block_system_subscribe:
            self._enter_blocked(reason="配置应用")
        else:
            # 用户手动关闭屏蔽：应用站点并取消窗口任务（不自动回弹）
            self._cancel_toggle_jobs()
            if self._unblock_site_names:
                site_ids = self._resolve_site_ids(ids=self._unblock_site_ids, names=self._unblock_site_names)
                if site_ids:
                    self._apply_sites_to_all_subscribes(site_ids, reason="用户关闭屏蔽：全量同步站点")
                    self._try_set_default_sites_for_unblocked(site_ids)
            self.__update_config()
            logger.info("用户已关闭屏蔽系统订阅（配置应用）")

        # 立即运行一次
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=self.sync_subscribes,
                    trigger='date',
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                    kwargs={"trigger_reason": "manual_once"},
                )
                if self._scheduler.get_jobs():
                    self._scheduler.start()

            if self._onlyonce:
                self._onlyonce = False
                self.__update_config()

    # ------------------ init clients/handlers ------------------

    def _init_clients(self):
        """初始化客户端"""
        proxy = settings.PROXY
        if proxy:
            logger.info(f"使用 MoviePilot PROXY: {proxy}")

        if self._pansou_enabled and self._pansou_url:
            self._pansou_client = PanSouClient(
                base_url=self._pansou_url,
                username=self._pansou_username,
                password=self._pansou_password,
                auth_enabled=self._pansou_auth_enabled,
                proxy=proxy
            )

        if self._nullbr_enabled:
            if not self._nullbr_appid or not self._nullbr_api_key:
                missing = []
                if not self._nullbr_appid:
                    missing.append("APP ID")
                if not self._nullbr_api_key:
                    missing.append("API Key")
                logger.warning(f"Nullbr 已启用但缺少必要配置：{', '.join(missing)}，将无法使用 Nullbr 查询功能")
                self._nullbr_client = None
            else:
                self._nullbr_client = NullbrClient(app_id=self._nullbr_appid, api_key=self._nullbr_api_key, proxy=proxy)
                logger.info("Nullbr 客户端初始化成功")

        # HDHive OpenAPI 客户端初始化（API 模式搜索/解锁共用；Playwright 模式搜索时动态创建浏览器客户端）
        self._init_hdhive_openapi_client(proxy)
        if self._hdhive_enabled:
            if self._hdhive_query_mode == "playwright" and (not self._hdhive_username or not self._hdhive_password):
                logger.warning("HDHive (Playwright 模式) 已启用但未配置用户名和密码，将无法使用 HDHive 查询功能")
            elif self._hdhive_query_mode == "api" and (not self._hdhive_client or not self._hdhive_client.is_ready):
                logger.warning("HDHive (API 模式) 已启用但未完成 OpenAPI 应用配置和用户授权，将无法使用 HDHive 查询功能")
            else:
                logger.info(f"HDHive 配置已加载（模式：{self._hdhive_query_mode}）")

        # 115 API is intentionally isolated in the p115-openclaw container.
        # Never import/install p115client in MoviePilot's shared Python environment.
        if self._classifier_url and self._classifier_token:
            self._p115_manager = P115ClientManager(
                cookies=self._cookies,  # accepted only for migration logging; never used
                base_url=self._classifier_url,
                token=self._classifier_token,
                timeout=self._classifier_timeout,
            )
        else:
            self._p115_manager = None
        # AYCLUB Telegram 桥接客户端
        self._ayclub_client = AyclubClient(
            base_url=self._ayclub_url,
            enabled=self._ayclub_enabled,
            timeout=self._ayclub_timeout,
            max_pages=self._ayclub_max_pages,
        )

        if self._ayclub_enabled:
            if self._ayclub_client.health():
                logger.info(
                    f"AYCLUB Telegram 桥接服务连接成功："
                    f"{self._ayclub_url}"
                )
            else:
                logger.warning(
                    f"AYCLUB Telegram 桥接服务不可用："
                    f"{self._ayclub_url}"
                )
            
        # OpenClaw 七分类客户端
        self._classifier_client = OpenClawClassifierClient(
            base_url=self._classifier_url,
            token=self._classifier_token,
            enabled=self._classifier_enabled,
            timeout=self._classifier_timeout,
        )

        if self._classifier_enabled:
            if self._classifier_client.is_ready:
                logger.info(
                    f"OpenClaw 七分类服务已启用："
                    f"{self._classifier_url}"
                )
            else:
                logger.warning(
                    "OpenClaw 七分类服务已启用，但地址或 Token 未配置完整"
                )
                
    # ------------------ HDHive OpenAPI ------------------

    def _on_hdhive_token_update(self, tokens: Dict[str, Any]):
        """Token 刷新后持久化到插件配置"""
        self._hdhive_access_token = tokens.get("access_token", "")
        self._hdhive_refresh_token = tokens.get("refresh_token", "")
        self._hdhive_token_expires_at = float(tokens.get("token_expires_at", 0) or 0)
        self.__update_config()

    def _init_hdhive_openapi_client(self, proxy=None):
        """
        初始化 HDHive OpenAPI 客户端，并处理一次性授权码换 Token

        新版接入模型：
        1. 在 HDHive 创建 OpenAPI 应用，审核通过后获得 client_id 和应用 Secret
        2. 配置 client_id、应用 Secret、回调地址后保存，从日志中复制授权链接到浏览器完成授权
        3. 将回调地址中的 code 参数填入"授权码"并保存，插件自动换取用户 Token
        """
        self._hdhive_client = None
        if not self._hdhive_api_key:
            return

        client = HDHiveOpenAPIClient(
            app_secret=self._hdhive_api_key,
            client_id=self._hdhive_client_id,
            access_token=self._hdhive_access_token,
            refresh_token=self._hdhive_refresh_token,
            token_expires_at=self._hdhive_token_expires_at,
            proxy=proxy,
            on_token_update=self._on_hdhive_token_update,
        )
        self._hdhive_client = client

        # 一次性授权码换取用户 Token
        if self._hdhive_auth_code:
            auth_code = self._hdhive_auth_code
            self._hdhive_auth_code = ""
            if not self._hdhive_redirect_uri:
                logger.error("HDHive OpenAPI: 已填写授权码但缺少回调地址（必须与发起授权时一致），无法换取 Token")
                self.__update_config()
            else:
                try:
                    data = client.exchange_code(auth_code, self._hdhive_redirect_uri)
                    scopes = data.get("scope") or " ".join(data.get("scopes") or [])
                    logger.info(f"HDHive OpenAPI: 用户授权成功，已获取 Access Token（scope: {scopes}）")
                    self.__update_config()
                except HDHiveOpenAPIError as e:
                    logger.error(f"HDHive OpenAPI: 授权码换取 Token 失败: [{e.code}] {e.message} {e.description}")
                    self.__update_config()
                except Exception as e:
                    logger.error(f"HDHive OpenAPI: 授权码换取 Token 异常: {e}")
                    self.__update_config()

        # 未完成授权时，打印授权链接引导用户操作
        if not client.is_ready:
            if self._hdhive_client_id and self._hdhive_redirect_uri:
                authorize_url = client.build_authorize_url(self._hdhive_redirect_uri)
                logger.warning(
                    f"HDHive OpenAPI: 尚未完成用户授权，请在浏览器打开以下链接完成授权，"
                    f"然后将回调地址中的 code 参数填入插件配置的「授权码」并保存：\n{authorize_url}"
                )
            else:
                logger.warning("HDHive OpenAPI: 请先在 HDHive 申请 OpenAPI 应用，并在插件中配置 Client ID、应用 Secret 和回调地址")

    def _init_subscribe_handler(self):
        self._subscribe_handler = SubscribeHandler(
            exclude_subscribes=self._exclude_subscribes,
            notify=self._notify,
            post_message_func=self.post_message,
            is_excluded_func=self._is_subscribe_excluded
        )

    def _init_handlers(self):
        self._init_subscribe_handler()
        self._init_lifecycle_store()

        self._search_handler = SearchHandler(
            pansou_client=self._pansou_client,
            nullbr_client=self._nullbr_client,
            hdhive_client=self._hdhive_client,
            pansou_enabled=self._pansou_enabled,
            nullbr_enabled=self._nullbr_enabled,
            hdhive_enabled=self._hdhive_enabled,
            hdhive_query_mode=self._hdhive_query_mode,
            hdhive_auto_unlock=self._hdhive_auto_unlock,
            hdhive_max_unlock_points=self._hdhive_max_unlock_points,
            hdhive_max_points_per_sub=self._hdhive_max_points_per_sub,
            hdhive_username=self._hdhive_username,
            hdhive_password=self._hdhive_password,
            hdhive_cookie=self._hdhive_cookie,
            only_115=self._only_115,
            pansou_channels=self._pansou_channels,
            search_source_order=self._search_source_order,
            ayclub_client=self._ayclub_client,
            ayclub_enabled=self._ayclub_enabled
        )
        # 设置持久化函数，用于保存订阅的历史积分花费
        self._search_handler.set_data_funcs(self.get_data, self.save_data)

        self._sync_handler = SyncHandler(
            p115_manager=self._p115_manager,
            search_handler=self._search_handler,
            subscribe_handler=self._subscribe_handler,
            chain=self.chain,
            save_path=self._save_path,
            movie_save_path=self._movie_save_path,
            classifier_client=self._classifier_client,
            max_transfer_per_sync=self._max_transfer_per_sync,
            batch_size=self._batch_size,
            skip_other_season_dirs=self._skip_other_season_dirs,
            notify=self._notify,
            post_message_func=self.post_message,
            get_data_func=self.get_data,
            save_data_func=self.save_data,
            lifecycle_store=self._lifecycle_store
        )

        self._api_handler = ApiHandler(
            pansou_client=self._pansou_client,
            p115_manager=self._p115_manager,
            only_115=self._only_115,
            save_path=self._save_path,
            get_data_func=self.get_data,
            save_data_func=self.save_data
        )

    # ------------------ 配置写回 ------------------

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "cron": self._cron,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "only_115": self._only_115,
            "save_path": self._save_path,
            "movie_save_path": self._movie_save_path,
            "cookies": self._cookies,
            "pansou_enabled": self._pansou_enabled,
            "pansou_url": self._pansou_url,
            "pansou_username": self._pansou_username,
            "pansou_password": self._pansou_password,
            "pansou_auth_enabled": self._pansou_auth_enabled,
            "pansou_channels": self._pansou_channels,
            # AYCLUB Telegram
            "ayclub_enabled": self._ayclub_enabled,
            "ayclub_url": self._ayclub_url,
            "ayclub_timeout": self._ayclub_timeout,
            "ayclub_max_pages": self._ayclub_max_pages,
            "nullbr_enabled": self._nullbr_enabled,
            "nullbr_appid": self._nullbr_appid,
            "nullbr_api_key": self._nullbr_api_key,
            # HDHive 配置
            "hdhive_enabled": self._hdhive_enabled,
            "hdhive_query_mode": self._hdhive_query_mode,
            "hdhive_api_key": self._hdhive_api_key,
            "hdhive_client_id": self._hdhive_client_id,
            "hdhive_redirect_uri": self._hdhive_redirect_uri,
            "hdhive_auth_code": self._hdhive_auth_code,
            "hdhive_access_token": self._hdhive_access_token,
            "hdhive_refresh_token": self._hdhive_refresh_token,
            "hdhive_token_expires_at": self._hdhive_token_expires_at,
            "hdhive_auto_unlock": self._hdhive_auto_unlock,
            "hdhive_max_unlock_points": self._hdhive_max_unlock_points,
            "hdhive_max_points_per_sub": self._hdhive_max_points_per_sub,
            "hdhive_username": self._hdhive_username,
            "hdhive_password": self._hdhive_password,
            "hdhive_cookie": self._hdhive_cookie,
            "hdhive_auto_refresh": self._hdhive_auto_refresh,
            "hdhive_refresh_before": self._hdhive_refresh_before,
            # 其他配置
            "search_source_order": self._search_source_order,
            "subscribe_filter_mode": self._subscribe_filter_mode,
            "exclude_subscribes": self._exclude_subscribes,
            "include_subscribes": self._include_subscribes,
            "block_system_subscribe": self._block_system_subscribe,
            "max_transfer_per_sync": self._max_transfer_per_sync,
            "batch_size": self._batch_size,
            "skip_other_season_dirs": self._skip_other_season_dirs,
            # OpenClaw 七分类服务配置
            "classifier_enabled": self._classifier_enabled,
            "classifier_url": self._classifier_url,
            "classifier_token": self._classifier_token,
            "classifier_timeout": self._classifier_timeout,
            "unblock_site_ids": self._unblock_site_ids,
            "unblock_site_names": self._unblock_site_names,
            "unblock_delay_minutes": self._unblock_delay_minutes,
            "system_subscribe_window_hours": self._system_subscribe_window_hours,
            "unblock_window_hours": self._system_subscribe_window_hours,
        })

    # ------------------ stop ------------------

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception:
            pass

        try:
            if self._toggle_scheduler:
                self._toggle_scheduler.remove_all_jobs()
                if self._toggle_scheduler.running:
                    self._toggle_scheduler.shutdown()
                self._toggle_scheduler = None
        except Exception:
            pass

    # ======================================================================
    # 必备：get_state / get_form / get_page / get_api / get_service
    # ======================================================================

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return UIConfig.get_form()

    def get_page(self) -> Optional[List[dict]]:
        history = self.get_data('history') or []
        return UIConfig.get_page(history)

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/sync_subscribes",
                "endpoint": self.sync_subscribes,
                "methods": ["GET"],
                "summary": "执行同步订阅追更"
            },
            {
                "path": "/clear_history",
                "endpoint": self.api_clear_history,
                "methods": ["POST"],
                "summary": "清空历史记录"
            }
        ]
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [{
            "cmd": "/p115_sub_action",
            "event": EventType.PluginAction,
            "desc": "115网盘订阅追更",
            "category": "订阅",
            "data": {
                "action": "p115_sub_action"
            }
        }]


    def scheduled_sync_subscribes(self):
        """MoviePilot 自动服务专用入口，避免调度器丢失业务参数。"""
        return self.sync_subscribes(trigger_reason="scheduled_cron")

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        services = []

        if self._cron and self._cron_interval_ge_min_hours(self._cron, self._MIN_INTERVAL_HOURS):
            try:
                services.append({
                    "id": "P115StrgmSub",
                    "name": "115网盘订阅追更服务",
                    "trigger": CronTrigger.from_crontab(
                        self._cron,
                        timezone=pytz.timezone(settings.TZ),
                    ),
                    "func": self.scheduled_sync_subscribes,
                })
            except Exception as e:
                logger.warning(f"Cron 表达式无效：{self._cron}，将回退 interval=8h。错误：{e}")
                services.append({
                    "id": "P115StrgmSub",
                    "name": "115网盘订阅追更服务",
                    "trigger": "interval",
                    "func": self.scheduled_sync_subscribes,
                    "kwargs": {"hours": 8}
                })
        else:
            services.append({
                "id": "P115StrgmSub",
                "name": "115网盘订阅追更服务",
                "trigger": "interval",
                "func": self.scheduled_sync_subscribes,
                "kwargs": {"hours": 8}
            })

        return services

    # ======================================================================
    # 必备：_do_sync（返回 bool）
    # ======================================================================

    def _do_sync(
        self,
        target_subscribe_ids: Optional[List[int]] = None,
        trigger_reason: str = "",
        scheduled_evening_refresh: bool = False,
    ) -> bool:
        targeted_request = target_subscribe_ids is not None
        target_ids: Set[int] = set()
        if targeted_request:
            for value in target_subscribe_ids or []:
                try:
                    sid = int(value)
                except (TypeError, ValueError):
                    continue
                if sid > 0:
                    target_ids.add(sid)
            if not target_ids:
                logger.info(
                    f"定向订阅同步没有有效订阅 ID，跳过：{trigger_reason or '未提供原因'}"
                )
                return True

        # 至少启用一个搜索源
        if (
            not self._pansou_enabled
            and not self._nullbr_enabled
            and not self._hdhive_enabled
            and not self._ayclub_enabled
        ):
            logger.error(
                "搜索源均未启用（PanSou/Nullbr/HDHive/AYCLUB），无法执行"
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】配置错误",
                    text=(
                        "PanSou、Nullbr、HDHive、AYCLUB 均未启用，"
                        "请至少启用一个搜索源。"
                    )
                )
            return False

        # 115 客户端检查
        if not self._p115_manager:
            logger.error("OpenClaw 115 执行服务未初始化，请检查地址和 Token 配置")
            return False

        if not self._p115_manager.check_login():
            logger.error("OpenClaw 115 执行服务不可用或其 115 账号未连接")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title="【115网盘订阅追更】登录失败",
                    text="请检查 p115-openclaw 运行状态及其独立 115 账号登录。"
                )
            return False

        if targeted_request:
            logger.info(
                f"开始执行 115 网盘定向订阅同步：subscribe_ids={sorted(target_ids)}，"
                f"原因={trigger_reason or 'MoviePilot生命周期事件'}"
            )
        else:
            logger.info("开始执行 115 网盘订阅同步...")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】开始执行",
                    text="正在扫描订阅列表并同步缺失内容..."
                )

        # reset api counters
        try:
            self._p115_manager.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._pansou_client:
                self._pansou_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._nullbr_client:
                self._nullbr_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._search_handler:
                self._search_handler.reset_task_spent_points()
        except Exception:
            pass

        # 获取 MoviePilot 全部活动订阅。生命周期状态仍用全量列表对账，
        # 业务处理阶段再按目标 ID 过滤，避免把未选中的活动订阅误标记为失活。
        with SessionFactory() as db:
            all_subscribes = SubscribeOper(db=db).list("N,R")

        self._init_lifecycle_store()
        self._lifecycle_store.reconcile_active(all_subscribes or [])

        if targeted_request:
            subscribes = [
                subscribe for subscribe in (all_subscribes or [])
                if int(subscribe.id) in target_ids
            ]
            found_ids = {int(subscribe.id) for subscribe in subscribes}
            missing_ids = sorted(target_ids - found_ids)
            if missing_ids:
                logger.info(
                    f"定向同步目标已不在 MoviePilot 活动订阅中，跳过：{missing_ids}"
                )
            if not subscribes:
                logger.info(
                    f"定向订阅同步结束：没有仍处于活动状态的目标订阅，"
                    f"原因={trigger_reason or 'MoviePilot生命周期事件'}"
                )
                return True
            logger.info(
                f"本次仅处理 MoviePilot 目标订阅："
                f"{[int(subscribe.id) for subscribe in subscribes]}"
            )
        else:
            subscribes = all_subscribes or []

        if not subscribes:
            logger.info("无订阅数据")
            if self._notify and not targeted_request:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】执行完成",
                    text="当前无订阅数据。"
                )
            return True

        tv_subscribes = [s for s in subscribes if s.type == MediaType.TV.value]
        movie_subscribes = [s for s in subscribes if s.type == MediaType.MOVIE.value]

        if not tv_subscribes and not movie_subscribes:
            logger.info("无电影/剧集订阅")
            return True

        history: List[dict] = self.get_data('history') or []
        transfer_details: List[Dict[str, Any]] = []
        transferred_count = 0

        exclude_ids = set(self._exclude_subscribes or [])
        skipped_count = 0

        # 处理电影
        for subscribe in movie_subscribes:
            if global_vars.is_system_stopped:
                break
            if self._is_subscribe_excluded(subscribe.id):
                skipped_count += 1
                continue
            transferred_count = self._sync_handler.process_movie_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count,
                scheduled_evening_refresh=scheduled_evening_refresh,
            )

        # 处理剧集
        for subscribe in tv_subscribes:
            if global_vars.is_system_stopped:
                break
            if self._is_subscribe_excluded(subscribe.id):
                skipped_count += 1
                continue
            transferred_count = self._sync_handler.process_tv_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count,
                exclude_ids=exclude_ids,
                scheduled_evening_refresh=scheduled_evening_refresh,
            )

        if skipped_count:
            mode_label = "指定模式" if self._subscribe_filter_mode == "include" else "排除模式"
            logger.info(f"订阅过滤（{mode_label}）：本次跳过 {skipped_count} 个不在处理范围的订阅")

        self.save_data('history', history)

        if targeted_request:
            logger.info(
                f"115 网盘定向订阅同步完成：subscribe_ids={sorted(target_ids)}，"
                f"共转存 {transferred_count} 个文件"
            )
        else:
            logger.info(f"115 网盘订阅同步完成，共转存 {transferred_count} 个文件")

        if self._notify:
            if transferred_count > 0:
                self._sync_handler.send_transfer_notification(transfer_details, transferred_count)
            elif not targeted_request:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】执行完成",
                    text="本次同步未发现需要转存的新资源。"
                )

        return True

    # ------------------ API包装（用于 get_api） ------------------

    def api_clear_history(self, apikey: str) -> dict:
        return self._api_handler.clear_history(apikey)

    # ------------------ 同步入口（触发条件1） ------------------

    def sync_subscribes(
        self,
        target_subscribe_ids: Optional[List[int]] = None,
        trigger_reason: str = "",
    ):
        with lock:
            tz = pytz.timezone(settings.TZ)
            run_start = datetime.datetime.now(tz=tz)
            targeted_request = target_subscribe_ids is not None
            is_last_run_today = bool(
                not targeted_request
                and trigger_reason == "scheduled_cron"
                and self._is_last_run_today(run_start)
            )
            scheduled_evening_refresh = bool(
                not targeted_request
                and trigger_reason == "scheduled_cron"
                and is_last_run_today
            )
            logger.info(
                "同步入口："
                f"trigger_reason={trigger_reason or 'manual_or_api'}，"
                f"targeted={targeted_request}，"
                f"is_last_run_today={is_last_run_today}，"
                f"scheduled_evening_refresh={scheduled_evening_refresh}，"
                f"cron={self._cron or 'interval=8h'}"
            )
            if trigger_reason == "scheduled_cron":
                logger.info(
                    "定时任务 AYCLUB 刷新权限："
                    f"{'当日最后一轮真实搜索' if scheduled_evening_refresh else '白天轮次仅缓存'}"
                )

            success = False
            try:
                success = self._do_sync(
                    target_subscribe_ids=target_subscribe_ids,
                    trigger_reason=trigger_reason,
                    scheduled_evening_refresh=scheduled_evening_refresh,
                )
            except Exception as e:
                logger.error(f"同步任务异常：{e}")
                success = False
            finally:
                # 生命周期定向同步不参与“当天最后一次全量任务”的站点窗口切换。
                if (
                    target_subscribe_ids is None
                    and success
                    and self._block_system_subscribe
                    and self._is_last_run_today(run_start)
                ):
                    if int(self._unblock_delay_minutes) < 0 or (not self._window_enabled()):
                        self._enter_blocked(reason="触发条件1")
                    else:
                        self._schedule_unblock_after_delay(
                            datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                        )

    # ------------------ 业务 API（保留） ------------------

    def api_search(self, keyword: str, apikey: str) -> dict:
        return self._api_handler.search(keyword, apikey)

    def api_transfer(self, share_url: str, save_path: str, apikey: str) -> dict:
        return self._api_handler.transfer(share_url, save_path, apikey)

    def api_list_directories(self, path: str = "/", apikey: str = "") -> dict:
        return self._api_handler.list_directories(path, apikey)

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "p115_sub_action":
            return

        logger.info("收到命令，开始执行追更任务")
        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更】开始执行",
            text="已收到远程命令，正在执行追更任务...",
            userid=event_data.get("user")
        )

        self.sync_subscribes()

        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更】执行完成",
            text="远程触发的追更任务已完成。",
            userid=event_data.get("user")
        )

# 1.8.6: persist and restore temporary PT windows across restarts.
from .restart_safe import install_restart_safe as _install_restart_safe
_install_restart_safe(P115StrgmSub)
