"""NextFind fallback handoff coordinator.

MoviePilot remains the source of truth. AYCLUB is searched first; only a real
AYCLUB attempt that produced no usable delivery may hand the media to
NextFind. While the same TMDB item is confirmed active in NextFind, this
coordinator creates synthetic lifecycle pending tasks so the existing search
and PT gates keep working without changing their mature behavior.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import time
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from app.log import logger

from ..clients.nextfind import NextFindClient
from .lifecycle import LifecycleStore


class NextFindHandoffManager:
    """Persist and verify temporary NextFind fallback ownership."""

    DATA_KEY = "nextfind_handoff_state_v1"
    SCHEMA_VERSION = 1
    PENDING_CONFIRMATION_MINUTES = 15
    REMOTE_COMPLETED_GRACE_HOURS = 6

    _FAILED_STATUS_TOKENS = {
        "failed",
        "failure",
        "error",
        "cancelled",
        "canceled",
        "removed",
        "deleted",
        "invalid",
        "失败",
        "错误",
        "已取消",
        "取消",
        "已删除",
        "删除",
        "无效",
    }

    def __init__(
        self,
        *,
        client: Optional[NextFindClient],
        bridge_client=None,
        lifecycle_store: LifecycleStore,
        get_data_func: Optional[Callable] = None,
        save_data_func: Optional[Callable] = None,
        wait_hours: int = 48,
        on_pending_created_func: Optional[Callable] = None,
    ) -> None:
        self._client = client
        self._bridge = bridge_client
        self._lifecycle = lifecycle_store
        self._get_data = get_data_func
        self._save_data = save_data_func
        self._wait_hours = max(6, min(int(wait_hours or 48), 168))
        self._on_pending_created = on_pending_created_func
        self._lock = RLock()
        self._remote_cache_at: Optional[datetime.datetime] = None
        self._remote_cache_ok: bool = False
        self._remote_cache_items: List[Dict[str, Any]] = []

    @property
    def is_ready(self) -> bool:
        return bool(self._client and self._client.is_ready)

    @staticmethod
    def _now() -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def _now_text(cls) -> str:
        return cls._now().isoformat()

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime.datetime]:
        if not value:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(str(value))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return parsed.astimezone(datetime.timezone.utc)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _media_key(tmdb_id: int, media_type: str) -> str:
        normalized = "tv" if str(media_type).casefold() == "tv" else "movie"
        return f"{normalized}:{int(tmdb_id)}"


    @staticmethod
    def _bridge_task_id(subscribe: Any, tmdb_id: int, media_type: str, season: Optional[int]) -> str:
        sid = int(getattr(subscribe, "id"))
        suffix = f":S{int(season or 1)}" if str(media_type).casefold() == "tv" else ":movie"
        return f"p115strgmsub:{sid}:{str(media_type).casefold()}:{int(tmdb_id)}{suffix}"

    def _register_bridge_task(
        self,
        *,
        subscribe: Any,
        tmdb_id: int,
        media_type: str,
        title: str,
        season: Optional[int],
        episodes: Optional[Iterable[int]],
    ) -> bool:
        bridge_ready = bool(
            self._bridge
            and getattr(
                self._bridge,
                "task_api_ready",
                getattr(self._bridge, "is_ready", False),
            )
        )
        if not bridge_ready:
            logger.warning("NextFind 已启用，但 OpenClaw 地址或令牌未就绪；不创建无法预筛选的任务")
            return False
        mode = "wash" if bool(getattr(subscribe, "best_version", False)) else "incremental"
        normalized = []
        for value in episodes or []:
            try:
                episode = int(value)
            except (TypeError, ValueError):
                continue
            if episode > 0 and episode not in normalized:
                normalized.append(episode)
        normalized.sort()
        if str(media_type).casefold() == "tv" and mode == "incremental" and not normalized:
            logger.warning("MoviePilot 未提供明确缺集，不登记 NextFind 增量任务")
            return False
        return bool(self._bridge.register_nextfind_task(
            task_id=self._bridge_task_id(subscribe, tmdb_id, media_type, season),
            subscribe_id=int(getattr(subscribe, "id")),
            media_type=media_type,
            title=title,
            year=getattr(subscribe, "year", None),
            tmdb_id=int(tmdb_id),
            season=season,
            missing_episodes=normalized,
            mode=mode,
        ))

    def manual_remote_subscriptions(self) -> List[Dict[str, Any]]:
        """Return active NextFind subscriptions not created by this plugin."""
        if not self.is_ready:
            return []
        ok, items = self._list_remote(force_refresh=False)
        if not ok:
            return []
        result: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict) or not self._client.is_active_subscription(item):
                continue
            tmdb_id = self._client.subscription_tmdb_id(item)
            media_type = self._client.subscription_media_type(item)
            if not tmdb_id or media_type not in {"tv", "movie"}:
                continue
            local = self._get_record(self._media_key(int(tmdb_id), media_type))
            if local and bool(local.get("managed_by_plugin")):
                continue
            sources = [item]
            for key in ("media", "subscription", "item", "info", "metadata"):
                value = item.get(key)
                if isinstance(value, dict):
                    sources.append(value)
            title = ""
            year = None
            season = None
            for source in sources:
                title = title or str(source.get("title") or source.get("name") or "").strip()
                if year is None:
                    try:
                        year = int(source.get("year")) if source.get("year") else None
                    except (TypeError, ValueError):
                        year = None
                if season is None:
                    try:
                        season = int(source.get("season")) if source.get("season") is not None else None
                    except (TypeError, ValueError):
                        season = None
            result.append({
                "tmdb_id": int(tmdb_id),
                "media_type": media_type,
                "title": title or f"TMDB {tmdb_id}",
                "year": year,
                "season": season,
            })
        return result

    def _delete_bridge_task(self, subscribe_id: int, tmdb_id: int, media_type: str, season: Optional[int]) -> None:
        if not self._bridge:
            return
        stub = type("SubscribeRef", (), {"id": int(subscribe_id)})()
        task_id = self._bridge_task_id(stub, tmdb_id, media_type, season)
        self._bridge.delete_nextfind_task(task_id)

    def _empty(self) -> Dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "records": {}}

    def _load(self) -> Dict[str, Any]:
        if not self._get_data:
            return self._empty()
        try:
            raw = self._get_data(self.DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取 NextFind 接管状态失败：{error}")
            return self._empty()
        if not isinstance(raw, dict):
            return self._empty()
        records = raw.get("records") or {}
        return {
            "schema_version": self.SCHEMA_VERSION,
            "records": {
                str(key): dict(value)
                for key, value in records.items()
                if isinstance(value, dict)
            } if isinstance(records, dict) else {},
        }

    def _save(self, state: Dict[str, Any]) -> None:
        if not self._save_data:
            return
        state["schema_version"] = self.SCHEMA_VERSION
        try:
            self._save_data(self.DATA_KEY, state)
        except Exception as error:
            logger.warning(f"保存 NextFind 接管状态失败：{error}")

    def _get_record(self, key: str) -> Dict[str, Any]:
        with self._lock:
            return dict((self._load().get("records") or {}).get(key) or {})

    def _upsert_record(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        title: str,
        subscribe_id: int,
        season: Optional[int],
        managed_by_plugin: Optional[bool],
        status: str,
        remote_status: str = "",
        progress_signature: str = "",
    ) -> Dict[str, Any]:
        key = self._media_key(tmdb_id, media_type)
        with self._lock:
            state = self._load()
            old = (state.get("records") or {}).get(key) or {}
            owners: Set[int] = set()
            for value in old.get("subscribe_ids") or []:
                try:
                    owners.add(int(value))
                except (TypeError, ValueError):
                    pass
            owners.add(int(subscribe_id))
            seasons: Set[int] = set()
            for value in old.get("seasons") or []:
                try:
                    seasons.add(int(value))
                except (TypeError, ValueError):
                    pass
            if season is not None:
                try:
                    seasons.add(int(season))
                except (TypeError, ValueError):
                    pass
            managed = (
                bool(old.get("managed_by_plugin"))
                if managed_by_plugin is None
                else bool(managed_by_plugin)
            )
            now_text = self._now_text()
            old_signature = str(old.get("progress_signature") or "")
            signature = str(progress_signature or old_signature)
            progress_changed = bool(
                progress_signature and progress_signature != old_signature
            )
            record = {
                **old,
                "key": key,
                "tmdb_id": int(tmdb_id),
                "media_type": (
                    "tv" if str(media_type).casefold() == "tv" else "movie"
                ),
                "title": str(title or old.get("title") or "")[:300],
                "subscribe_ids": sorted(owners),
                "seasons": sorted(seasons),
                "managed_by_plugin": managed,
                "status": str(status or "active"),
                "remote_status": str(remote_status or ""),
                "activated_at": old.get("activated_at") or now_text,
                "last_checked_at": now_text,
                "updated_at": now_text,
                "progress_signature": signature,
                "last_progress_at": (
                    now_text
                    if progress_changed
                    else old.get("last_progress_at")
                    or old.get("activated_at")
                    or now_text
                ),
            }
            if status in {"active", "remote_completed"}:
                record["last_remote_seen_at"] = now_text
            if status == "remote_completed":
                record["remote_completed_at"] = (
                    old.get("remote_completed_at") or now_text
                )
            elif old.get("status") == "remote_completed":
                record.pop("remote_completed_at", None)
            state.setdefault("records", {})[key] = record
            self._save(state)
            return dict(record)

    def _delete_record(self, key: str) -> None:
        with self._lock:
            state = self._load()
            if key in (state.get("records") or {}):
                state["records"].pop(key, None)
                self._save(state)

    def _mark_timed_out(
        self,
        *,
        key: str,
        record: Dict[str, Any],
        reason: str,
    ) -> None:
        seasons = [int(v) for v in record.get("seasons") or [] if str(v).isdigit()]
        season = seasons[0] if seasons else None
        for sid in record.get("subscribe_ids") or []:
            if str(sid).isdigit():
                self._delete_bridge_task(
                    int(sid), int(record.get("tmdb_id")),
                    str(record.get("media_type") or "movie"), season,
                )
        if bool(record.get("managed_by_plugin")) and self.is_ready:
            self._client.remove_subscription(
                tmdb_id=int(record.get("tmdb_id")),
                media_type=str(record.get("media_type") or "movie"),
            )
        with self._lock:
            state = self._load()
            current = (state.get("records") or {}).get(key) or record
            current["status"] = "timed_out"
            current["timeout_reason"] = str(reason or "NextFind 接管超时")
            current["timed_out_at"] = self._now_text()
            current["updated_at"] = self._now_text()
            state.setdefault("records", {})[key] = current
            self._save(state)

    @staticmethod
    def _status_values(payload: Any) -> Set[str]:
        values: Set[str] = set()
        queue: List[Any] = [payload]
        visited: Set[int] = set()
        while queue and len(visited) < 100:
            current = queue.pop(0)
            if id(current) in visited:
                continue
            visited.add(id(current))
            if isinstance(current, list):
                queue.extend(current[:50])
                continue
            if not isinstance(current, dict):
                continue
            for key, value in current.items():
                if key in {"status", "state", "result", "subscription_status"}:
                    text = str(value or "").strip().casefold()
                    if text:
                        values.add(text)
                elif isinstance(value, (dict, list)):
                    queue.append(value)
        return values

    @classmethod
    def _has_failed_status(cls, payload: Any) -> bool:
        for value in cls._status_values(payload):
            if value in cls._FAILED_STATUS_TOKENS:
                return True
            if any(
                token in value
                for token in ("failed", "error", "cancel", "失败", "错误", "取消")
            ):
                return True
        return False

    @staticmethod
    def _has_completed_status(payload: Any) -> bool:
        completed_tokens = {
            "completed",
            "complete",
            "done",
            "finished",
            "完成",
            "已完成",
        }
        return any(
            value in completed_tokens
            for value in NextFindHandoffManager._status_values(payload)
        )

    def _list_remote(
        self,
        *,
        force_refresh: bool = False,
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        if not self.is_ready:
            return False, []
        now = self._now()
        if (
            not force_refresh
            and self._remote_cache_at
            and (now - self._remote_cache_at).total_seconds() <= 15
        ):
            return self._remote_cache_ok, list(self._remote_cache_items)
        ok, items = self._client.list_subscriptions()
        self._remote_cache_at = now
        self._remote_cache_ok = bool(ok)
        self._remote_cache_items = list(items or [])
        return bool(ok), list(items or [])

    def _find_remote(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        force_refresh: bool = False,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ok, items = self._list_remote(force_refresh=force_refresh)
        if not ok:
            return False, None
        wanted_type = "tv" if str(media_type).casefold() == "tv" else "movie"
        for item in items:
            if self._client.subscription_tmdb_id(item) != int(tmdb_id):
                continue
            item_type = self._client.subscription_media_type(item)
            if item_type and item_type != wanted_type:
                continue
            if self._client.is_active_subscription(item):
                return True, item
        return True, None

    def _remote_info_status(self, *, tmdb_id: int, media_type: str) -> Any:
        if not self.is_ready:
            return None
        ok, payload = self._client.subscription_info(
            tmdb_id=tmdb_id,
            media_type=media_type,
        )
        return payload if ok else None

    @staticmethod
    def _progress_signature(payload: Any) -> str:
        if payload is None:
            return ""

        volatile_tokens = (
            "time",
            "date",
            "updated",
            "created",
            "checked",
            "poll",
            "heartbeat",
        )

        def normalize(value: Any) -> Any:
            if isinstance(value, list):
                return [normalize(item) for item in value[:100]]
            if not isinstance(value, dict):
                if isinstance(value, (str, int, float, bool)) or value is None:
                    return value
                return str(value)
            result: Dict[str, Any] = {}
            for key in sorted(value):
                folded = str(key).casefold()
                if any(token in folded for token in volatile_tokens):
                    continue
                result[str(key)] = normalize(value[key])
            return result

        try:
            encoded = json.dumps(
                normalize(payload),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except Exception:
            return ""
        return hashlib.sha256(encoded).hexdigest()[:20]

    def _progress_age_hours(self, record: Dict[str, Any]) -> float:
        started = self._parse_time(
            record.get("last_progress_at")
            or record.get("activated_at")
        )
        if not started:
            return float("inf")
        return max(0.0, (self._now() - started).total_seconds() / 3600)

    def _lifecycle_media_key(self, subscribe: Any) -> str:
        return self._lifecycle.media_key_from_subscribe(subscribe)

    def _ensure_pending(
        self,
        *,
        subscribe: Any,
        media_type: str,
        tmdb_id: int,
        season: Optional[int],
        episodes: Optional[Iterable[int]],
    ) -> List[str]:
        media_key = self._lifecycle_media_key(subscribe)
        normalized_episodes: List[int] = []
        for value in episodes or []:
            try:
                episode = int(value)
            except (TypeError, ValueError):
                continue
            if episode > 0 and episode not in normalized_episodes:
                normalized_episodes.append(episode)
        normalized_episodes.sort()
        share_ref = f"nextfind:{self._media_key(tmdb_id, media_type)}"
        if normalized_episodes:
            file_items = [
                {
                    "episode": episode,
                    "id": (
                        f"nextfind:{int(tmdb_id)}:"
                        f"S{int(season or 1):02d}E{episode:02d}"
                    ),
                    "name": "NextFind managed subscription",
                }
                for episode in normalized_episodes
            ]
        else:
            file_items = [{
                "id": f"nextfind:{int(tmdb_id)}:movie",
                "name": "NextFind managed subscription",
            }]
        already_live = self._lifecycle.has_live_pending_reference(
            media_key=media_key,
            share_ref=share_ref,
            episodes=normalized_episodes or None,
        )
        task_ids = self._lifecycle.add_pending(
            subscribe=subscribe,
            media_key=media_key,
            episodes=normalized_episodes or None,
            file_items=file_items,
            share_ref=share_ref,
            target_path="NextFind专用转存目录",
            source="nextfind",
        )
        if task_ids and not already_live and self._on_pending_created:
            try:
                self._on_pending_created(
                    subscribe_id=int(getattr(subscribe, "id")),
                    task_ids=list(task_ids),
                    source="nextfind",
                )
            except Exception as error:
                logger.warning(
                    f"通知 NextFind 在途任务失败："
                    f"subscribe_id={getattr(subscribe, 'id', '?')}，错误={error}"
                )
        return task_ids

    def _invalidate_pending(self, subscribe: Any, reason: str) -> None:
        try:
            self._lifecycle.invalidate_pending_for_media(
                media_key=self._lifecycle_media_key(subscribe),
                source="nextfind",
                reason=reason,
            )
        except Exception as error:
            logger.warning(
                f"失效 NextFind 在途状态失败："
                f"subscribe_id={getattr(subscribe, 'id', '?')}，错误={error}"
            )

    def _record_age_hours(self, record: Dict[str, Any]) -> float:
        started = self._parse_time(
            record.get("last_remote_seen_at")
            or record.get("activated_at")
            or record.get("updated_at")
        )
        if not started:
            return float("inf")
        return max(0.0, (self._now() - started).total_seconds() / 3600)

    def gate_before_search(
        self,
        *,
        subscribe: Any,
        tmdb_id: Optional[int],
        media_type: str,
        title: str,
        season: Optional[int] = None,
        episodes: Optional[Iterable[int]] = None,
    ) -> bool:
        """Return True when NextFind currently owns this media task."""
        if not self.is_ready or not tmdb_id:
            return False
        key = self._media_key(int(tmdb_id), media_type)
        local = self._get_record(key)
        remote_ok, remote = self._find_remote(
            tmdb_id=int(tmdb_id),
            media_type=media_type,
        )

        handoff_exists = bool(remote) or local.get("status") in {
            "active", "remote_completed", "pending_confirmation", "cleanup_pending"
        }
        if handoff_exists and not self._register_bridge_task(
            subscribe=subscribe, tmdb_id=int(tmdb_id), media_type=media_type,
            title=title, season=season, episodes=episodes,
        ):
            logger.warning(f"NextFind 缺集任务未在桥接器确认，恢复 AYCLUB：{title} (TMDB={tmdb_id})")
            return False

        if remote_ok and remote:
            info = self._remote_info_status(
                tmdb_id=int(tmdb_id),
                media_type=media_type,
            )
            if local.get("status") == "timed_out":
                logger.info(
                    f"NextFind 该媒体已超时解除接管，远端条目消失前不再阻止 AYCLUB："
                    f"{title} (TMDB={tmdb_id})"
                )
                return False
            if self._has_failed_status(remote) or self._has_failed_status(info):
                self._invalidate_pending(subscribe, "NextFind 明确返回失败状态")
                self._delete_record(key)
                logger.warning(
                    f"NextFind 已明确失败，恢复 AYCLUB："
                    f"{title} (TMDB={tmdb_id})"
                )
                return False
            status = (
                "remote_completed"
                if self._has_completed_status(info)
                else "active"
            )
            if status == "remote_completed" and local:
                completed_at = self._parse_time(local.get("remote_completed_at"))
                if (
                    completed_at
                    and (self._now() - completed_at).total_seconds()
                    > self.REMOTE_COMPLETED_GRACE_HOURS * 3600
                ):
                    timeout_reason = "NextFind 已完成但 MoviePilot 长时间仍缺失"
                    self._invalidate_pending(subscribe, timeout_reason)
                    self._mark_timed_out(
                        key=key,
                        record=local,
                        reason=timeout_reason,
                    )
                    logger.warning(
                        f"NextFind 已完成超过 {self.REMOTE_COMPLETED_GRACE_HOURS} 小时，"
                        f"MoviePilot 仍未满足，恢复 AYCLUB："
                        f"{title} (TMDB={tmdb_id})"
                    )
                    return False
            progress_signature = self._progress_signature(info)
            updated = self._upsert_record(
                tmdb_id=int(tmdb_id),
                media_type=media_type,
                title=title,
                subscribe_id=int(getattr(subscribe, "id")),
                season=season,
                managed_by_plugin=(
                    None if local else False
                ),
                status=status,
                remote_status=self._client.subscription_status(remote),
                progress_signature=progress_signature,
            )
            if (
                status == "active"
                and self._progress_age_hours(updated) > self._wait_hours
            ):
                timeout_reason = "NextFind 接管长时间无可确认进展"
                self._invalidate_pending(subscribe, timeout_reason)
                self._mark_timed_out(
                    key=key,
                    record=updated,
                    reason=timeout_reason,
                )
                logger.warning(
                    f"NextFind 接管超过 {self._wait_hours} 小时无可确认进展，"
                    f"恢复 AYCLUB：{title} (TMDB={tmdb_id})"
                )
                return False
            task_ids = self._ensure_pending(
                subscribe=subscribe,
                media_type=media_type,
                tmdb_id=int(tmdb_id),
                season=season,
                episodes=episodes,
            )
            logger.info(
                f"NextFind 已接管，暂停同媒体 AYCLUB："
                f"{title} (TMDB={tmdb_id})，在途={len(task_ids)}"
            )
            return True

        if remote_ok and not remote:
            pending_confirmation = (
                local.get("status") == "pending_confirmation"
                and self._record_age_hours(local)
                <= self.PENDING_CONFIRMATION_MINUTES / 60
            )
            if pending_confirmation:
                self._ensure_pending(
                    subscribe=subscribe,
                    media_type=media_type,
                    tmdb_id=int(tmdb_id),
                    season=season,
                    episodes=episodes,
                )
                logger.info(
                    f"NextFind 添加请求仍在确认窗口内，暂缓 AYCLUB："
                    f"{title} (TMDB={tmdb_id})"
                )
                return True
            if local:
                self._invalidate_pending(subscribe, "NextFind 远端已无对应订阅")
                self._delete_record(key)
                logger.info(
                    f"NextFind 远端订阅已消失，恢复 AYCLUB："
                    f"{title} (TMDB={tmdb_id})"
                )
            return False

        # Remote verification failed. Prefer a short conservative pause over
        # duplicate transfer, but never keep a stale handoff forever.
        if local and local.get("status") in {
            "active",
            "remote_completed",
            "pending_confirmation",
            "cleanup_pending",
        }:
            if self._record_age_hours(local) <= self._wait_hours:
                self._ensure_pending(
                    subscribe=subscribe,
                    media_type=media_type,
                    tmdb_id=int(tmdb_id),
                    season=season,
                    episodes=episodes,
                )
                logger.warning(
                    f"NextFind 暂时无法核验，沿用已确认接管状态防重复："
                    f"{title} (TMDB={tmdb_id})"
                )
                return True
            timeout_reason = "NextFind 核验超时，恢复 AYCLUB"
            self._invalidate_pending(subscribe, timeout_reason)
            self._mark_timed_out(
                key=key,
                record=local,
                reason=timeout_reason,
            )
            logger.warning(
                f"NextFind 接管超过 {self._wait_hours} 小时且无法核验，"
                f"恢复 AYCLUB：{title} (TMDB={tmdb_id})"
            )
        return False

    def handoff_after_ayclub(
        self,
        *,
        subscribe: Any,
        tmdb_id: Optional[int],
        media_type: str,
        title: str,
        season: Optional[int] = None,
        episodes: Optional[Iterable[int]] = None,
    ) -> bool:
        """Create or reuse one NextFind subscription after AYCLUB exhausts."""
        if not self.is_ready or not tmdb_id:
            return False
        tmdb_id = int(tmdb_id)
        key = self._media_key(tmdb_id, media_type)
        local = self._get_record(key)
        remote_ok, remote = self._find_remote(
            tmdb_id=tmdb_id,
            media_type=media_type,
            force_refresh=True,
        )
        if not remote_ok:
            logger.warning(
                f"NextFind 后端核验失败，本轮不创建接管任务："
                f"{title} (TMDB={tmdb_id})"
            )
            return False

        managed: Optional[bool] = None
        status = "active"
        if remote:
            managed = None if local else False
            if str(media_type).casefold() == "tv":
                # Existing TV subscriptions may be idle between polls; request
                # the documented high-priority missing-episode pass.
                self._client.fill_missing(
                    tmdb_id=tmdb_id,
                    media_type="tv",
                    title=title,
                )
        else:
            if not self._client.add_subscription(
                tmdb_id=tmdb_id,
                media_type=media_type,
                title=title,
            ):
                logger.warning(
                    f"NextFind 订阅添加失败，不暂停 AYCLUB："
                    f"{title} (TMDB={tmdb_id})"
                )
                return False
            managed = True
            confirmed = None
            for _ in range(2):
                time.sleep(0.8)
                check_ok, confirmed = self._find_remote(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    force_refresh=True,
                )
                if check_ok and confirmed:
                    break
            if not confirmed:
                # The POST was accepted but the listing has not converged yet.
                # Hold a short provisional lock and verify again next run.
                status = "pending_confirmation"
                logger.warning(
                    f"NextFind 已接受添加请求但暂未在订阅列表确认，"
                    f"进入 {self.PENDING_CONFIRMATION_MINUTES} 分钟保护窗口："
                    f"{title} (TMDB={tmdb_id})"
                )

        if not self._register_bridge_task(
            subscribe=subscribe, tmdb_id=tmdb_id, media_type=media_type,
            title=title, season=season, episodes=episodes,
        ):
            if managed is True:
                self._client.remove_subscription(tmdb_id=tmdb_id, media_type=media_type)
            logger.warning(f"桥接器未确认缺集清单，本轮不让 NextFind 接管：{title} (TMDB={tmdb_id})")
            return False

        record = self._upsert_record(
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title,
            subscribe_id=int(getattr(subscribe, "id")),
            season=season,
            managed_by_plugin=managed,
            status=status,
            remote_status=(
                self._client.subscription_status(remote) if remote else ""
            ),
        )
        task_ids = self._ensure_pending(
            subscribe=subscribe,
            media_type=media_type,
            tmdb_id=tmdb_id,
            season=season,
            episodes=episodes,
        )
        logger.info(
            f"AYCLUB 无有效投递，已交给 NextFind："
            f"{title} (TMDB={tmdb_id})，"
            f"managed={record.get('managed_by_plugin')}，在途={len(task_ids)}"
        )
        return True

    def mark_media_satisfied(
        self,
        *,
        subscribe: Any,
        tmdb_id: Optional[int],
        media_type: str,
    ) -> None:
        if not tmdb_id:
            return
        key = self._media_key(int(tmdb_id), media_type)
        self._release_owner(
            key=key,
            subscribe_id=int(getattr(subscribe, "id")),
            reason="MoviePilot 已确认满足订阅",
        )

    def release_subscribe_id(self, subscribe_id: int, reason: str) -> None:
        sid = int(subscribe_id)
        with self._lock:
            state = self._load()
            keys = [
                key
                for key, record in (state.get("records") or {}).items()
                if sid in {
                    int(value)
                    for value in record.get("subscribe_ids") or []
                    if str(value).isdigit()
                }
            ]
        for key in keys:
            self._release_owner(key=key, subscribe_id=sid, reason=reason)

    def reconcile_active_subscribe_ids(self, active_ids: Iterable[int]) -> None:
        active = {int(value) for value in active_ids}
        with self._lock:
            state = self._load()
            work: List[Tuple[str, int]] = []
            for key, record in (state.get("records") or {}).items():
                for value in record.get("subscribe_ids") or []:
                    try:
                        sid = int(value)
                    except (TypeError, ValueError):
                        continue
                    if sid not in active:
                        work.append((key, sid))
        for key, sid in work:
            self._release_owner(
                key=key,
                subscribe_id=sid,
                reason="MoviePilot 活动订阅对账已不存在",
            )

    def _release_owner(self, *, key: str, subscribe_id: int, reason: str) -> None:
        with self._lock:
            state = self._load()
            record = (state.get("records") or {}).get(key)
            if not record:
                return
            seasons = [int(v) for v in record.get("seasons") or [] if str(v).isdigit()]
            season = seasons[0] if seasons else None
            self._delete_bridge_task(
                int(subscribe_id), int(record.get("tmdb_id")),
                str(record.get("media_type") or "movie"), season,
            )
            owners = {
                int(value)
                for value in record.get("subscribe_ids") or []
                if str(value).isdigit()
            }
            owners.discard(int(subscribe_id))
            record["subscribe_ids"] = sorted(owners)
            record["updated_at"] = self._now_text()
            if owners:
                state["records"][key] = record
                self._save(state)
                return
            state["records"][key] = record
            self._save(state)

        if not bool(record.get("managed_by_plugin")):
            self._delete_record(key)
            logger.info(
                f"NextFind 手工订阅不由插件删除，仅解除本地绑定："
                f"{record.get('title') or key}"
            )
            return
        if not self.is_ready:
            with self._lock:
                state = self._load()
                current = (state.get("records") or {}).get(key) or record
                current["status"] = "cleanup_pending"
                current["cleanup_reason"] = reason
                current["updated_at"] = self._now_text()
                state.setdefault("records", {})[key] = current
                self._save(state)
            return

        removed = self._client.remove_subscription(
            tmdb_id=int(record.get("tmdb_id")),
            media_type=str(record.get("media_type") or "movie"),
        )
        if removed:
            check_ok, remote = self._find_remote(
                tmdb_id=int(record.get("tmdb_id")),
                media_type=str(record.get("media_type") or "movie"),
                force_refresh=True,
            )
            if check_ok and not remote:
                self._delete_record(key)
                logger.info(
                    f"MoviePilot 已完成/取消，插件创建的 NextFind 临时订阅已移除："
                    f"{record.get('title') or key}"
                )
                return

        with self._lock:
            state = self._load()
            current = (state.get("records") or {}).get(key) or record
            current["status"] = "cleanup_pending"
            current["cleanup_reason"] = reason
            current["updated_at"] = self._now_text()
            state.setdefault("records", {})[key] = current
            self._save(state)
        logger.warning(
            f"NextFind 临时订阅移除尚未确认，将在后续对账继续保护："
            f"{record.get('title') or key}"
        )
