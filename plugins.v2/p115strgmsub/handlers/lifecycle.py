"""
MoviePilot 订阅生命周期状态。

本模块只保存插件必须掌握的临时状态：订阅周期和已经投递、等待 MoviePilot
整理/入库的任务。媒体是否存在、缺少哪些集、订阅是否完成，始终交给
MoviePilot 的事件和 Chain 接口判断。
"""
from __future__ import annotations

import datetime
import hashlib
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from app.log import logger
from app.schemas.types import MediaType


class LifecycleStore:
    """持久化订阅周期和在途整理任务。"""

    DATA_KEY = "lifecycle_state"
    SCHEMA_VERSION = 2
    PENDING_TTL_HOURS = 12
    ED2K_PENDING_TTL_HOURS = 24

    def __init__(
        self,
        get_data_func: Optional[Callable] = None,
        save_data_func: Optional[Callable] = None,
    ):
        self._get_data = get_data_func
        self._save_data = save_data_func
        self._lock = RLock()

    @staticmethod
    def _now() -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def _now_text(cls) -> str:
        return cls._now().isoformat()

    @staticmethod
    def _parse_time(value: Optional[str]) -> Optional[datetime.datetime]:
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
    def _value(obj: Any, key: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    @classmethod
    def media_key_from_values(
        cls,
        media_type: Any,
        tmdb_id: Any = None,
        douban_id: Any = None,
        season: Any = None,
        title: str = "",
        year: Any = None,
    ) -> str:
        type_value = getattr(media_type, "value", media_type)
        is_tv = str(type_value).upper() in {
            str(MediaType.TV.value).upper(),
            "TV",
            "电视剧",
        }
        prefix = "tv" if is_tv else "movie"
        identity = None
        if tmdb_id not in (None, ""):
            identity = f"tmdb:{tmdb_id}"
        elif douban_id not in (None, ""):
            identity = f"douban:{douban_id}"
        else:
            safe_title = str(title or "unknown").strip().lower()
            identity = f"title:{safe_title}:{year or ''}"
        if is_tv:
            try:
                season_num = int(season or 1)
            except (TypeError, ValueError):
                season_num = 1
            return f"{prefix}:{identity}:S{season_num}"
        return f"{prefix}:{identity}"

    @classmethod
    def media_key_from_subscribe(cls, subscribe: Any) -> str:
        return cls.media_key_from_values(
            media_type=cls._value(subscribe, "type"),
            tmdb_id=cls._value(subscribe, "tmdbid"),
            douban_id=cls._value(subscribe, "doubanid"),
            season=cls._value(subscribe, "season"),
            title=cls._value(subscribe, "name", ""),
            year=cls._value(subscribe, "year"),
        )

    @classmethod
    def media_key_from_event(cls, mediainfo: Any, meta: Any = None) -> str:
        media_type = cls._value(mediainfo, "type") or cls._value(meta, "type")
        tmdb_id = cls._value(mediainfo, "tmdb_id") or cls._value(mediainfo, "tmdbid")
        douban_id = cls._value(mediainfo, "douban_id") or cls._value(mediainfo, "doubanid")
        season = (
            cls._value(meta, "begin_season")
            or cls._value(meta, "season")
            or cls._value(mediainfo, "season")
        )
        return cls.media_key_from_values(
            media_type=media_type,
            tmdb_id=tmdb_id,
            douban_id=douban_id,
            season=season,
            title=cls._value(mediainfo, "title", ""),
            year=cls._value(mediainfo, "year"),
        )

    def _empty(self) -> Dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "subscriptions": {},
            "pending": {},
        }

    def _load(self) -> Dict[str, Any]:
        if not self._get_data:
            return self._empty()
        try:
            raw = self._get_data(self.DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取订阅生命周期状态失败：{error}")
            return self._empty()
        if not isinstance(raw, dict):
            return self._empty()
        state = self._empty()
        subscriptions = raw.get("subscriptions") or {}
        pending = raw.get("pending") or {}
        if isinstance(subscriptions, dict):
            state["subscriptions"] = {
                str(key): value for key, value in subscriptions.items()
                if isinstance(value, dict)
            }
        if isinstance(pending, list):
            # 兼容早期草案格式。
            pending = {
                str(item.get("task_id") or index): item
                for index, item in enumerate(pending)
                if isinstance(item, dict)
            }
        if isinstance(pending, dict):
            state["pending"] = {
                str(key): value for key, value in pending.items()
                if isinstance(value, dict)
            }
        return state

    def _save(self, state: Dict[str, Any]) -> None:
        if not self._save_data:
            return
        state["schema_version"] = self.SCHEMA_VERSION
        try:
            self._save_data(self.DATA_KEY, state)
        except Exception as error:
            logger.warning(f"保存订阅生命周期状态失败：{error}")

    def ensure_subscription(
        self,
        subscribe_id: int,
        subscribe_info: Any = None,
        *,
        scene: str = "sync",
        new_cycle: bool = False,
    ) -> Dict[str, Any]:
        sid = str(int(subscribe_id))
        with self._lock:
            state = self._load()
            old = state["subscriptions"].get(sid) or {}
            generation = int(old.get("generation") or 1)
            if new_cycle and old:
                generation += 1
            media_key = (
                self.media_key_from_subscribe(subscribe_info)
                if subscribe_info is not None
                else old.get("media_key")
            )
            record = {
                **old,
                "subscribe_id": int(subscribe_id),
                "generation": generation,
                "media_key": media_key,
                "active": True,
                "status": "active",
                "last_scene": scene,
                "last_seen_at": self._now_text(),
                "updated_at": self._now_text(),
            }
            if subscribe_info is not None:
                record["last_lack_episode"] = self._value(subscribe_info, "lack_episode")
                record["last_note"] = list(self._value(subscribe_info, "note") or [])
                record["name"] = self._value(subscribe_info, "name")
                record["type"] = self._value(subscribe_info, "type")
                record["tmdbid"] = self._value(subscribe_info, "tmdbid")
                record["doubanid"] = self._value(subscribe_info, "doubanid")
                record["season"] = self._value(subscribe_info, "season")
            state["subscriptions"][sid] = record
            self._save(state)
            return dict(record)

    def on_added(self, subscribe_id: int, subscribe_info: Any = None) -> Dict[str, Any]:
        """新增 ID 代表新订阅周期，并要求 AYCLUB 首次查询绕过旧缓存。"""
        sid = str(int(subscribe_id))
        with self._lock:
            state = self._load()
            is_new = sid not in state["subscriptions"]

        record = self.ensure_subscription(
            subscribe_id,
            subscribe_info,
            scene="added",
            new_cycle=False,
        )

        if not is_new:
            return record

        with self._lock:
            state = self._load()
            current = state["subscriptions"].get(sid) or record
            current["force_refresh"] = True
            current["force_refresh_reason"] = "new_subscription"
            current["added_at"] = self._now_text()
            current["updated_at"] = self._now_text()
            state["subscriptions"][sid] = current
            self._save(state)
            return dict(current)

    def on_modified(
        self,
        subscribe_id: int,
        *,
        scene: str,
        subscribe_info: Any = None,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        scene = str(scene or "update")
        record = self.ensure_subscription(
            subscribe_id,
            subscribe_info,
            scene=scene,
            new_cycle=(scene == "reset"),
        )
        with self._lock:
            state = self._load()
            current = state["subscriptions"].get(str(int(subscribe_id))) or record
            current["last_fields"] = list(fields or [])
            if scene == "reset":
                current["force_refresh"] = True
                current["force_refresh_reason"] = "subscribe_reset"
                current["reset_at"] = self._now_text()
            state["subscriptions"][str(int(subscribe_id))] = current
            self._save(state)
            return dict(current)

    def on_deleted(self, subscribe_id: int, subscribe_info: Any = None) -> None:
        self._mark_inactive(subscribe_id, "cancelled", subscribe_info)

    def on_complete(self, subscribe_id: int, subscribe_info: Any = None) -> None:
        self._mark_inactive(subscribe_id, "completed", subscribe_info)
        record = self.get_subscription(subscribe_id)
        media_key = record.get("media_key")
        if media_key:
            self.reconcile_missing(media_key, media_satisfied=True)

    def _mark_inactive(self, subscribe_id: int, status: str, subscribe_info: Any = None) -> None:
        sid = str(int(subscribe_id))
        with self._lock:
            state = self._load()
            record = state["subscriptions"].get(sid) or {
                "subscribe_id": int(subscribe_id),
                "generation": 1,
            }
            if subscribe_info is not None:
                record["media_key"] = self.media_key_from_subscribe(subscribe_info)
                record["name"] = self._value(subscribe_info, "name")
            record.update({
                "active": False,
                "status": status,
                f"{status}_at": self._now_text(),
                "updated_at": self._now_text(),
            })
            state["subscriptions"][sid] = record
            self._save(state)

    def get_subscription(self, subscribe_id: int) -> Dict[str, Any]:
        with self._lock:
            return dict(
                (self._load().get("subscriptions") or {}).get(str(int(subscribe_id)))
                or {}
            )

    def generation(self, subscribe_id: int) -> int:
        return int(self.get_subscription(subscribe_id).get("generation") or 1)

    def peek_force_refresh(self, subscribe_id: int) -> bool:
        """读取重置后的强制刷新标记，但不提前消费。"""
        sid = str(int(subscribe_id))
        with self._lock:
            state = self._load()
            record = state["subscriptions"].get(sid) or {}
            return bool(record.get("force_refresh"))

    def clear_force_refresh(self, subscribe_id: int) -> None:
        """仅在 AYCLUB 已实际收到查询后清除强制刷新标记。"""
        sid = str(int(subscribe_id))
        with self._lock:
            state = self._load()
            record = state["subscriptions"].get(sid) or {}
            if not record.get("force_refresh"):
                return
            record["force_refresh"] = False
            record["force_refresh_cleared_at"] = self._now_text()
            record["updated_at"] = self._now_text()
            state["subscriptions"][sid] = record
            self._save(state)

    def consume_force_refresh(self, subscribe_id: int) -> bool:
        """兼容旧调用；新流程应使用 peek + clear。"""
        result = self.peek_force_refresh(subscribe_id)
        if result:
            self.clear_force_refresh(subscribe_id)
        return result

    def reconcile_active(self, subscribes: Iterable[Any]) -> None:
        """用 MP 当前活动订阅列表兜底取消/完成事件遗漏。"""
        active_ids: Set[str] = set()
        with self._lock:
            state = self._load()
            for subscribe in subscribes or []:
                sid_value = self._value(subscribe, "id")
                if sid_value is None:
                    continue
                sid = str(int(sid_value))
                active_ids.add(sid)
                old = state["subscriptions"].get(sid) or {}
                state["subscriptions"][sid] = {
                    **old,
                    "subscribe_id": int(sid_value),
                    "generation": int(old.get("generation") or 1),
                    "media_key": self.media_key_from_subscribe(subscribe),
                    "active": True,
                    "status": "active",
                    "last_seen_at": self._now_text(),
                    "last_lack_episode": self._value(subscribe, "lack_episode"),
                    "last_note": list(self._value(subscribe, "note") or []),
                    "name": self._value(subscribe, "name"),
                    "type": self._value(subscribe, "type"),
                    "tmdbid": self._value(subscribe, "tmdbid"),
                    "doubanid": self._value(subscribe, "doubanid"),
                    "season": self._value(subscribe, "season"),
                    "updated_at": self._now_text(),
                }

            for sid, record in list(state["subscriptions"].items()):
                if not record.get("active") or sid in active_ids:
                    continue
                # 若完成/删除事件丢失，只做保守停用，不冒充 MP 判定完成。
                record["active"] = False
                record["status"] = "inactive"
                record["inactive_at"] = self._now_text()
                record["updated_at"] = self._now_text()
                state["subscriptions"][sid] = record
                logger.info(
                    f"订阅 {sid} 已不在 MoviePilot 活动列表，生命周期状态改为 inactive"
                )
            self._save(state)

    @staticmethod
    def _task_id(
        subscribe_id: int,
        generation: int,
        media_key: str,
        episode: Optional[int],
        file_id: Any,
        share_ref: str,
    ) -> str:
        raw = "|".join([
            str(subscribe_id),
            str(generation),
            str(media_key),
            str(episode if episode is not None else "movie"),
            str(file_id or ""),
            str(share_ref or ""),
        ])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def add_pending(
        self,
        *,
        subscribe: Any,
        media_key: str,
        episodes: Optional[List[int]],
        file_items: Optional[List[Dict[str, Any]]] = None,
        share_ref: str = "",
        target_path: str = "",
        source: str = "",
    ) -> List[str]:
        subscribe_id = int(self._value(subscribe, "id"))
        generation = self.generation(subscribe_id)
        episode_list: List[Optional[int]] = [None]
        if episodes:
            episode_list = sorted({int(ep) for ep in episodes if ep is not None})
        items_by_episode: Dict[Optional[int], Dict[str, Any]] = {}
        for item in file_items or []:
            episode = item.get("episode")
            try:
                episode = int(episode) if episode is not None else None
            except (TypeError, ValueError):
                episode = None
            items_by_episode[episode] = item

        task_ids: List[str] = []
        with self._lock:
            state = self._load()
            for episode in episode_list:
                item = items_by_episode.get(episode) or {}
                file_info = item.get("file") if isinstance(item.get("file"), dict) else item
                file_id = file_info.get("id") if isinstance(file_info, dict) else None
                file_name = file_info.get("name") if isinstance(file_info, dict) else None
                task_id = self._task_id(
                    subscribe_id,
                    generation,
                    media_key,
                    episode,
                    file_id,
                    share_ref,
                )
                old = state["pending"].get(task_id) or {}
                state["pending"][task_id] = {
                    **old,
                    "task_id": task_id,
                    "subscribe_id": subscribe_id,
                    "generation": generation,
                    "media_key": media_key,
                    "media_type": self._value(subscribe, "type"),
                    "tmdbid": self._value(subscribe, "tmdbid"),
                    "doubanid": self._value(subscribe, "doubanid"),
                    "season": self._value(subscribe, "season"),
                    "episode": episode,
                    "file_id": file_id,
                    "file_name": file_name,
                    "share_ref": share_ref,
                    "target_path": target_path,
                    "source": source,
                    "status": (
                        "pending_transfer"
                        if str(source or "") == "ayclub_ed2k"
                        else "pending_organize"
                    ),
                    "created_at": old.get("created_at") or self._now_text(),
                    "updated_at": self._now_text(),
                }
                task_ids.append(task_id)
            self._save(state)
        return task_ids

    def _is_live_pending(self, task: Dict[str, Any]) -> bool:
        # organized 仍需等待 MP 媒体库/订阅缺失结果确认，避免 STRM/扫描延迟期间重复投递。
        return task.get("status") in {"pending_transfer", "pending_organize", "organized"}

    def _expire_stale(self, state: Dict[str, Any], media_key: str) -> bool:
        changed = False
        now = self._now()
        for task in state["pending"].values():
            if task.get("media_key") != media_key or not self._is_live_pending(task):
                continue
            ttl_hours = (
                self.ED2K_PENDING_TTL_HOURS
                if str(task.get("source") or "") == "ayclub_ed2k"
                else self.PENDING_TTL_HOURS
            )
            deadline = now - datetime.timedelta(hours=ttl_hours)
            created = self._parse_time(task.get("created_at"))
            if created and created < deadline:
                task["status"] = "stale"
                task["failed_at"] = self._now_text()
                task["failure_reason"] = "MoviePilot 长时间未确认入库"
                task["updated_at"] = self._now_text()
                changed = True
        return changed

    def pending_episodes(self, media_key: str) -> Set[int]:
        with self._lock:
            state = self._load()
            changed = self._expire_stale(state, media_key)
            result = {
                int(task["episode"])
                for task in state["pending"].values()
                if task.get("media_key") == media_key
                and self._is_live_pending(task)
                and task.get("episode") is not None
            }
            if changed:
                self._save(state)
            return result

    def has_pending_movie(self, media_key: str) -> bool:
        with self._lock:
            state = self._load()
            changed = self._expire_stale(state, media_key)
            result = any(
                task.get("media_key") == media_key
                and self._is_live_pending(task)
                and task.get("episode") is None
                for task in state["pending"].values()
            )
            if changed:
                self._save(state)
            return result

    def blocking_pending_tasks(self) -> List[Dict[str, Any]]:
        """返回仍可能造成 MoviePilot 原生订阅重复下载的活动在途任务。

        仅活动订阅且状态仍为 pending_transfer / pending_organize / organized
        的任务会阻止 PT 窗口开启。整理失败、已由 MP 确认、已取消或已完成
        的订阅不会阻塞。
        """
        with self._lock:
            state = self._load()
            active_ids = {
                int(record.get("subscribe_id"))
                for record in state.get("subscriptions", {}).values()
                if record.get("active")
                and record.get("subscribe_id") is not None
            }

            changed = False
            media_keys = {
                str(task.get("media_key"))
                for task in state.get("pending", {}).values()
                if task.get("media_key")
            }
            for media_key in media_keys:
                changed = self._expire_stale(state, media_key) or changed

            result: List[Dict[str, Any]] = []
            for task in state.get("pending", {}).values():
                try:
                    subscribe_id = int(task.get("subscribe_id"))
                except (TypeError, ValueError):
                    continue
                if subscribe_id not in active_ids or not self._is_live_pending(task):
                    continue
                result.append(dict(task))

            if changed:
                self._save(state)

            return sorted(
                result,
                key=lambda item: (
                    int(item.get("subscribe_id") or 0),
                    int(item.get("episode") or 0),
                    str(item.get("task_id") or ""),
                ),
            )

    @staticmethod
    def episodes_from_meta(meta: Any) -> Set[int]:
        values: List[Any] = []
        episode_list = LifecycleStore._value(meta, "episode_list")
        if episode_list:
            values.extend(list(episode_list))
        begin = LifecycleStore._value(meta, "begin_episode")
        end = LifecycleStore._value(meta, "end_episode")
        if begin is not None:
            try:
                begin_num = int(begin)
                end_num = int(end) if end is not None else begin_num
                values.extend(range(begin_num, end_num + 1))
            except (TypeError, ValueError):
                pass
        result: Set[int] = set()
        for value in values:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                result.add(number)
        return result

    def mark_transfer_complete(self, mediainfo: Any, meta: Any = None) -> List[Dict[str, Any]]:
        media_key = self.media_key_from_event(mediainfo, meta)
        episodes = self.episodes_from_meta(meta)
        if media_key.startswith("tv:") and not episodes:
            logger.warning(f"MP 入库事件缺少剧集号，暂不批量确认在途任务：{media_key}")
            return []
        matched: List[Dict[str, Any]] = []
        with self._lock:
            state = self._load()
            for task in state["pending"].values():
                if task.get("media_key") != media_key or not self._is_live_pending(task):
                    continue
                episode = task.get("episode")
                if episode is not None and episodes and int(episode) not in episodes:
                    continue
                task["status"] = "organized"
                task["organized_at"] = self._now_text()
                task["updated_at"] = self._now_text()
                matched.append(dict(task))
            if matched:
                self._save(state)
        return matched

    def mark_transfer_failed(self, mediainfo: Any, meta: Any = None, reason: str = "") -> List[Dict[str, Any]]:
        media_key = self.media_key_from_event(mediainfo, meta)
        episodes = self.episodes_from_meta(meta)
        if media_key.startswith("tv:") and not episodes:
            logger.warning(f"MP 整理失败事件缺少剧集号，暂不批量失败在途任务：{media_key}")
            return []
        matched: List[Dict[str, Any]] = []
        with self._lock:
            state = self._load()
            for task in state["pending"].values():
                if task.get("media_key") != media_key or not self._is_live_pending(task):
                    continue
                episode = task.get("episode")
                if episode is not None and episodes and int(episode) not in episodes:
                    continue
                task["status"] = "failed"
                task["failed_at"] = self._now_text()
                task["failure_reason"] = str(reason or "MoviePilot 整理失败")
                task["updated_at"] = self._now_text()
                matched.append(dict(task))
            if matched:
                self._save(state)
        return matched

    def reconcile_missing(
        self,
        media_key: str,
        missing_episodes: Optional[Iterable[int]] = None,
        *,
        media_satisfied: bool = False,
    ) -> List[Dict[str, Any]]:
        """事件漏失时，以 MP 当前缺失结果补记在途任务完成。"""
        missing = {int(ep) for ep in (missing_episodes or [])}
        confirmed: List[Dict[str, Any]] = []
        with self._lock:
            state = self._load()
            for task in state["pending"].values():
                if task.get("media_key") != media_key or not self._is_live_pending(task):
                    continue
                episode = task.get("episode")
                fulfilled = media_satisfied or (
                    episode is not None and int(episode) not in missing
                )
                if not fulfilled:
                    continue
                task["status"] = "verified_by_mp"
                task["verified_at"] = self._now_text()
                task["updated_at"] = self._now_text()
                confirmed.append(dict(task))
            if confirmed:
                self._save(state)
        return confirmed

    def active_subscribe_ids_for_media(self, media_key: str) -> List[int]:
        with self._lock:
            state = self._load()
            return [
                int(record["subscribe_id"])
                for record in state["subscriptions"].values()
                if record.get("active") and record.get("media_key") == media_key
            ]
