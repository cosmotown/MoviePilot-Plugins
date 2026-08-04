"""Event-driven bridge for P115StrmHelper's existing life-event database.

The bridge never starts a second 115 consumer and never polls a local or remote
folder on a timer.  It watches the single SQLite database file with Linux
inotify, blocks while idle, and performs a bounded incremental SELECT only when
that database is actually written.  Startup catch-up and unresolved-path
retries are finite and cursor based.
"""
from __future__ import annotations

import ctypes
import importlib
import os
import select
import struct
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, List, Optional

from sqlalchemy import text

from app.log import logger


@dataclass(frozen=True)
class PollBatchResult:
    forwarded: int
    rows_seen: int
    cursor_before: int
    cursor_after: int
    pending_path: bool
    pending_delivery: bool


class P115StrmHelperLifeEventTailer:
    """Forward relevant helper DB rows after filesystem write notifications."""

    CURSOR_KEY = "p115strmhelper_life_event_bridge_cursor_v2"
    HANDOFF_ROOTS = (
        "/nextfind",
        "/dbonline",
        "/OpenClaw_ED2K下载中",
    )
    EVENT_TYPES = (1, 2, 5, 6, 14, 17, 18, 20, 22, 23, 24)

    # Linux inotify constants.  The MoviePilot/NAS runtime is Linux; if inotify
    # is unavailable the bridge fails closed after one startup catch-up instead
    # of silently falling back to a permanent polling loop.
    _IN_MODIFY = 0x00000002
    _IN_ATTRIB = 0x00000004
    _IN_CLOSE_WRITE = 0x00000008
    _IN_MOVED_TO = 0x00000080
    _IN_CREATE = 0x00000100
    _IN_DELETE = 0x00000200
    _IN_DELETE_SELF = 0x00000400
    _IN_MOVE_SELF = 0x00000800
    _IN_IGNORED = 0x00008000
    _INOTIFY_EVENT = struct.Struct("iIII")

    def __init__(
        self,
        *,
        bridge_client: Any,
        nextfind_manager: Any = None,
        get_data: Callable[[str], Any],
        save_data: Callable[[str, Any], None],
        lookback_seconds: int = 86400,
        batch_size: int = 300,
        unresolved_grace_seconds: int = 60,
        coalesce_seconds: float = 2.0,
        max_drain_batches: int = 20,
    ) -> None:
        self._bridge = bridge_client
        self._nextfind = nextfind_manager
        self._get_data = get_data
        self._save_data = save_data
        self._lookback_seconds = max(60, min(int(lookback_seconds), 7 * 86400))
        self._batch_size = max(20, min(int(batch_size), 1000))
        self._unresolved_grace_seconds = max(
            10, min(int(unresolved_grace_seconds), 300)
        )
        self._coalesce_seconds = max(0.5, min(float(coalesce_seconds), 10.0))
        self._max_drain_batches = max(1, min(int(max_drain_batches), 100))
        self._poll_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._db_module = None
        self._cache_module = None
        self._config_module = None
        self._warned_unavailable = False
        self._manual_cache: List[Dict[str, Any]] = []
        self._manual_cache_at = 0.0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stop_pipe_r: Optional[int] = None
        self._stop_pipe_w: Optional[int] = None

    @staticmethod
    def _normalize_path(value: str) -> str:
        parts = [
            part
            for part in str(value or "").replace("\\", "/").split("/")
            if part not in {"", ".", ".."}
        ]
        return "/" + "/".join(parts)

    @classmethod
    def _in_scope(cls, path: str) -> bool:
        normalized = cls._normalize_path(path)
        return any(
            normalized == root or normalized.startswith(root + "/")
            for root in cls.HANDOFF_ROOTS
        )

    @classmethod
    def _is_nextfind_path(cls, path: str) -> bool:
        normalized = cls._normalize_path(path)
        return normalized == "/nextfind" or normalized.startswith("/nextfind/")

    @staticmethod
    def _usable_db_module(module: Any) -> bool:
        manager = getattr(module, "ct_db_manager", None)
        if manager is None:
            return False
        try:
            return bool(manager.is_initialized()) and bool(
                getattr(manager, "SessionFactory", None)
            )
        except Exception:
            return False

    def _load_db_module(self):
        if self._db_module is not None and self._usable_db_module(self._db_module):
            return self._db_module

        for name, module in tuple(sys.modules.items()):
            if (
                name.endswith("p115strmhelper.db_manager")
                and module is not None
                and self._usable_db_module(module)
            ):
                self._db_module = module
                return module

        for name in (
            "app.plugins.p115strmhelper.db_manager",
            "plugins.v2.p115strmhelper.db_manager",
            "p115strmhelper.db_manager",
        ):
            try:
                module = importlib.import_module(name)
            except Exception:
                continue
            if not self._usable_db_module(module):
                continue
            self._db_module = module
            return module
        return None

    def _load_config_module(self):
        if self._config_module is not None:
            return self._config_module
        for name, module in tuple(sys.modules.items()):
            if name.endswith("p115strmhelper.core.config") and module is not None:
                if getattr(module, "configer", None) is not None:
                    self._config_module = module
                    return module
        for name in (
            "app.plugins.p115strmhelper.core.config",
            "plugins.v2.p115strmhelper.core.config",
            "p115strmhelper.core.config",
        ):
            try:
                module = importlib.import_module(name)
            except Exception:
                continue
            if getattr(module, "configer", None) is not None:
                self._config_module = module
                return module
        return None

    def _load_cache_module(self):
        if self._cache_module is not None:
            return self._cache_module
        db_module = self._load_db_module()
        candidate_names: List[str] = []
        if db_module is not None:
            db_name = str(getattr(db_module, "__name__", ""))
            if db_name.endswith(".db_manager"):
                candidate_names.append(db_name[: -len(".db_manager")] + ".core.cache")
        candidate_names.extend(
            (
                "app.plugins.p115strmhelper.core.cache",
                "plugins.v2.p115strmhelper.core.cache",
                "p115strmhelper.core.cache",
            )
        )

        for name, module in tuple(sys.modules.items()):
            if name.endswith("p115strmhelper.core.cache") and module is not None:
                if getattr(module, "idpathcacher", None) is not None:
                    self._cache_module = module
                    return module
        for name in dict.fromkeys(candidate_names):
            try:
                module = importlib.import_module(name)
            except Exception:
                continue
            if getattr(module, "idpathcacher", None) is not None:
                self._cache_module = module
                return module
        return None

    def _session_factory(self):
        module = self._load_db_module()
        if module is None:
            return None
        manager = getattr(module, "ct_db_manager", None)
        if manager is None:
            return None
        try:
            initialized = bool(manager.is_initialized())
        except Exception:
            initialized = False
        if not initialized:
            return None
        return getattr(manager, "SessionFactory", None)

    def _database_path(self) -> Optional[Path]:
        module = self._load_db_module()
        manager = getattr(module, "ct_db_manager", None) if module else None
        engine = getattr(manager, "Engine", None) if manager else None
        database = getattr(getattr(engine, "url", None), "database", None)
        if database:
            return Path(str(database)).expanduser().resolve()

        config_module = self._load_config_module()
        configer = getattr(config_module, "configer", None) if config_module else None
        path = getattr(configer, "PLUGIN_DB_PATH", None) if configer else None
        if path:
            return Path(str(path)).expanduser().resolve()
        return None

    def helper_pull_mode(self) -> str:
        module = self._load_config_module()
        configer = getattr(module, "configer", None) if module else None
        value = getattr(configer, "monitor_life_first_pull_mode", "") if configer else ""
        return str(value or "").strip().casefold()

    def _cursor(self) -> int:
        try:
            raw = self._get_data(self.CURSOR_KEY) or {}
        except Exception:
            return 0
        if isinstance(raw, dict):
            raw = raw.get("last_event_id", 0)
        try:
            return max(0, int(raw or 0))
        except (TypeError, ValueError):
            return 0

    def _save_cursor(self, value: int) -> None:
        try:
            self._save_data(
                self.CURSOR_KEY,
                {
                    "last_event_id": int(value),
                    "updated_at": int(time.time()),
                    "source": "p115strmhelper_db_inotify",
                },
            )
        except Exception as error:
            logger.warning(f"保存115生活事件只读游标失败：{type(error).__name__}")

    def _cached_parent_path(self, parent_id: int) -> str:
        if parent_id <= 0:
            return "/" if parent_id == 0 else ""
        module = self._load_cache_module()
        cacher = getattr(module, "idpathcacher", None) if module else None
        if cacher is None or not hasattr(cacher, "get_dir_by_id"):
            return ""
        try:
            value = cacher.get_dir_by_id(parent_id)
        except Exception:
            return ""
        return str(value or "")

    def _resolve_path(self, db, row: Dict[str, Any]) -> str:
        file_id = int(row.get("file_id") or 0)
        parent_id = int(row.get("parent_id") or 0)
        file_name = str(row.get("file_name") or "").strip()

        if file_id > 0:
            result = db.execute(
                text(
                    "SELECT path FROM files WHERE id=:id "
                    "UNION ALL SELECT path FROM folders WHERE id=:id LIMIT 1"
                ),
                {"id": file_id},
            ).first()
            if result and result[0]:
                return str(result[0])

        if parent_id >= 0 and file_name:
            result = db.execute(
                text("SELECT path FROM folders WHERE id=:id LIMIT 1"),
                {"id": parent_id},
            ).first()
            parent_path = str(result[0]) if result and result[0] else ""
            if not parent_path:
                parent_path = self._cached_parent_path(parent_id)
            if parent_path:
                return str(PurePosixPath(parent_path) / file_name)
        return ""

    def _manual_subscriptions(self) -> List[Dict[str, Any]]:
        now = time.monotonic()
        if now - self._manual_cache_at < 30:
            return [dict(item) for item in self._manual_cache]
        result: List[Dict[str, Any]] = []
        try:
            if self._nextfind and hasattr(
                self._nextfind, "manual_remote_subscriptions"
            ):
                raw = self._nextfind.manual_remote_subscriptions() or []
                result = [dict(item) for item in raw if isinstance(item, dict)]
        except Exception as error:
            logger.warning(f"读取NextFind手工订阅失败：{type(error).__name__}")
        self._manual_cache = result[:200]
        self._manual_cache_at = now
        return [dict(item) for item in self._manual_cache]

    def manual_subscriptions_snapshot(self) -> List[Dict[str, Any]]:
        return self._manual_subscriptions()

    def _request_reconcile(self, reason: str) -> bool:
        """Request one bounded OpenClaw handoff pass.

        This is never a polling loop. It is used only for an observed database
        write, plugin startup, cursor reset, or MoviePilot lifecycle recovery.
        """
        if not self._bridge or not hasattr(
            self._bridge, "reconcile_p115_handoffs"
        ):
            logger.warning(
                "OpenClaw客户端不支持115一次性交接补偿"
            )
            return False
        safe_reason = str(reason or "life_event")[:128]
        try:
            ok = bool(
                self._bridge.reconcile_p115_handoffs(
                    manual_subscriptions=self._manual_subscriptions(),
                    reason=safe_reason,
                )
            )
        except Exception as error:
            logger.warning(
                f"请求OpenClaw一次性交接补偿失败："
                f"reason={safe_reason}，错误={type(error).__name__}"
            )
            return False
        if ok:
            logger.info(
                f"已请求OpenClaw一次性交接补偿：reason={safe_reason}"
            )
        else:
            logger.warning(
                f"OpenClaw未接受一次性交接补偿：reason={safe_reason}"
            )
        return ok

    @staticmethod
    def _payload(row: Dict[str, Any], path: str) -> Dict[str, Any]:
        return {
            "event_id": str(row.get("id") or ""),
            "event_type": int(row.get("type") or 0),
            "file_id": str(row.get("file_id") or ""),
            "parent_id": str(row.get("parent_id") or ""),
            "path": P115StrmHelperLifeEventTailer._normalize_path(path),
            "name": str(row.get("file_name") or ""),
            "size": int(row.get("file_size") or 0),
            "is_dir": int(row.get("file_category") or 0) == 0,
            "update_time": int(row.get("update_time") or 0),
        }

    def _poll_batch(self) -> PollBatchResult:
        if not self._poll_lock.acquire(blocking=False):
            cursor = self._cursor()
            return PollBatchResult(0, 0, cursor, cursor, False, False)
        try:
            session_factory = self._session_factory()
            cursor = self._cursor()
            if session_factory is None:
                if not self._warned_unavailable:
                    logger.info(
                        "115助手生活事件数据库尚未就绪；等待数据库文件写入通知，不启动轮询"
                    )
                    self._warned_unavailable = True
                return PollBatchResult(0, 0, cursor, cursor, False, False)
            self._warned_unavailable = False

            forwarded = 0
            advanced = cursor
            pending_path = False
            pending_delivery = False
            reconcile_requested = False
            now = int(time.time())
            placeholders = ",".join(str(value) for value in self.EVENT_TYPES)

            with session_factory() as db:
                if cursor > 0:
                    max_row = db.execute(
                        text("SELECT COALESCE(MAX(id),0) FROM life_event")
                    ).first()
                    max_event_id = int(max_row[0] if max_row else 0)
                    if cursor > max_event_id:
                        logger.warning(
                            f"115助手生活事件数据库游标回退："
                            f"saved={cursor}，db_max={max_event_id}；"
                            "请求一次有界补偿并重置游标"
                        )
                        ok = self._request_reconcile(
                            "life_event_cursor_rebased"
                        )
                        self._save_cursor(max_event_id)
                        return PollBatchResult(
                            0,
                            0,
                            cursor,
                            max_event_id,
                            False,
                            not ok,
                        )

                if cursor > 0:
                    statement = text(
                        f"SELECT id,type,file_id,parent_id,file_name,file_category,"
                        f"file_size,update_time FROM life_event "
                        f"WHERE id>:cursor AND type IN ({placeholders}) "
                        f"ORDER BY id ASC LIMIT :limit"
                    )
                    rows = db.execute(
                        statement,
                        {"cursor": cursor, "limit": self._batch_size},
                    ).mappings().all()
                else:
                    statement = text(
                        f"SELECT id,type,file_id,parent_id,file_name,file_category,"
                        f"file_size,update_time FROM life_event "
                        f"WHERE update_time>=:cutoff AND type IN ({placeholders}) "
                        f"ORDER BY id ASC LIMIT :limit"
                    )
                    rows = db.execute(
                        statement,
                        {
                            "cutoff": now - self._lookback_seconds,
                            "limit": self._batch_size,
                        },
                    ).mappings().all()
                    if not rows:
                        max_row = db.execute(
                            text("SELECT COALESCE(MAX(id),0) FROM life_event")
                        ).first()
                        advanced = int(max_row[0] if max_row else 0)

                for mapping in rows:
                    row = dict(mapping)
                    event_id = int(row.get("id") or 0)
                    path = self._resolve_path(db, row)
                    if not path:
                        event_time = int(row.get("update_time") or 0)
                        if (
                            event_time <= 0
                            or now - event_time
                            < self._unresolved_grace_seconds
                        ):
                            pending_path = True
                            break

                        # /dbonline、/nextfind 和 ED2K 暂存目录不一定在
                        # P115StrmHelper 的 files/folders 路径表中。以前这里
                        # 会直接跳过并推进游标，导致桥接器永远收不到唤醒。
                        # 现在每个数据库写入批次最多请求一次有界补偿；
                        # OpenClaw只扫描三个固定暂存目录一次，不启动循环。
                        if not reconcile_requested:
                            ok = self._request_reconcile(
                                "life_event_unresolved_path"
                            )
                            if not ok:
                                pending_delivery = True
                                break
                            reconcile_requested = True
                            logger.info(
                                "115生活事件路径不可解析，"
                                "已改用一次性有界交接补偿："
                                f"event_id={event_id}"
                            )

                        advanced = event_id
                        continue

                    if self._in_scope(path):
                        manual: Iterable[Dict[str, Any]] = []
                        if self._is_nextfind_path(path):
                            manual = self._manual_subscriptions()
                        ok = bool(
                            self._bridge
                            and self._bridge.forward_p115_life_event(
                                self._payload(row, path),
                                manual_subscriptions=manual,
                            )
                        )
                        if not ok:
                            pending_delivery = True
                            break
                        forwarded += 1
                    advanced = event_id

            if advanced != cursor:
                self._save_cursor(advanced)
            return PollBatchResult(
                forwarded,
                len(rows),
                cursor,
                advanced,
                pending_path,
                pending_delivery,
            )
        except Exception as error:
            cursor = self._cursor()
            logger.warning(
                f"读取115助手生活事件数据库失败：{type(error).__name__}: {error}"
            )
            return PollBatchResult(0, 0, cursor, cursor, False, False)
        finally:
            self._poll_lock.release()

    def poll_once(self) -> int:
        """Compatibility helper for tests/manual one-shot execution."""
        return self._poll_batch().forwarded

    def _drain_bounded(self, reason: str) -> int:
        total = 0
        batches_used = 0
        pending = False

        while batches_used < self._max_drain_batches:
            result = self._poll_batch()
            batches_used += 1
            total += result.forwarded
            pending = result.pending_path or result.pending_delivery
            if pending:
                break
            if result.cursor_after == result.cursor_before:
                break
            if result.rows_seen < self._batch_size:
                break

        if pending:
            # P115StrmHelper writes life_event before finishing its own path
            # cache/table updates. Retry only this observed write, with a finite
            # schedule. No retry loop exists while the system is otherwise idle.
            for delay in (2, 5, 15, 30, 60, 300):
                if self._stop_event.wait(delay):
                    return total
                result = self._poll_batch()
                total += result.forwarded
                if result.pending_path or result.pending_delivery:
                    continue
                # Continue draining only within the original hard batch cap.
                while (
                    batches_used < self._max_drain_batches
                    and result.cursor_after != result.cursor_before
                    and result.rows_seen >= self._batch_size
                ):
                    result = self._poll_batch()
                    batches_used += 1
                    total += result.forwarded
                    if result.pending_path or result.pending_delivery:
                        break
                break
        if total:
            logger.debug(f"115助手生活事件桥接[{reason}]转发 {total} 条")
        return total

    @classmethod
    def _open_inotify(cls, directory: Path) -> tuple[int, int]:
        libc = ctypes.CDLL(None, use_errno=True)
        init = getattr(libc, "inotify_init1", None)
        add = getattr(libc, "inotify_add_watch", None)
        if init is None or add is None:
            raise OSError("Linux inotify API unavailable")
        init.argtypes = [ctypes.c_int]
        init.restype = ctypes.c_int
        add.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add.restype = ctypes.c_int
        fd = int(init(os.O_CLOEXEC | os.O_NONBLOCK))
        if fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        mask = (
            cls._IN_MODIFY
            | cls._IN_ATTRIB
            | cls._IN_CLOSE_WRITE
            | cls._IN_MOVED_TO
            | cls._IN_CREATE
            | cls._IN_DELETE
            | cls._IN_DELETE_SELF
            | cls._IN_MOVE_SELF
        )
        wd = int(add(fd, os.fsencode(str(directory)), mask))
        if wd < 0:
            error = ctypes.get_errno()
            os.close(fd)
            raise OSError(error, os.strerror(error))
        return fd, wd

    @classmethod
    def _consume_inotify(cls, fd: int, watched_names: set[str]) -> bool:
        relevant = False
        while True:
            try:
                chunk = os.read(fd, 65536)
            except BlockingIOError:
                break
            if not chunk:
                break
            offset = 0
            while offset + cls._INOTIFY_EVENT.size <= len(chunk):
                _wd, mask, _cookie, name_len = cls._INOTIFY_EVENT.unpack_from(
                    chunk, offset
                )
                offset += cls._INOTIFY_EVENT.size
                raw_name = chunk[offset : offset + name_len]
                offset += name_len
                name = raw_name.split(b"\0", 1)[0].decode(errors="replace")
                if mask & cls._IN_IGNORED:
                    relevant = True
                elif name in watched_names:
                    relevant = True
        return relevant

    def _watch_worker(self, db_path: Path) -> None:
        directory = db_path.parent
        watched_names = {
            db_path.name,
            db_path.name + "-wal",
            db_path.name + "-shm",
            db_path.name + "-journal",
        }
        inotify_fd: Optional[int] = None
        try:
            inotify_fd, _wd = self._open_inotify(directory)
            if self._stop_pipe_r is None:
                return
            logger.info(
                f"115助手生活事件桥接已阻塞监听数据库写入：{db_path}；空闲时零SELECT"
            )
            self._drain_bounded("startup")

            # 插件启动时做且仅做一次固定目录补偿，覆盖：
            # 1. 115助手稍晚于本插件初始化；
            # 2. MoviePilot上次异常退出；
            # 3. 115助手在MP整理期间暂停而未补拉的生活事件。
            # 此调用不会建立扫描循环。
            self._request_reconcile("plugin_startup")

            while not self._stop_event.is_set():
                readable, _, _ = select.select(
                    [inotify_fd, self._stop_pipe_r], [], []
                )
                if self._stop_pipe_r in readable:
                    return
                if inotify_fd not in readable:
                    continue
                relevant = self._consume_inotify(inotify_fd, watched_names)
                if not relevant:
                    continue
                if self._stop_event.wait(self._coalesce_seconds):
                    return
                self._consume_inotify(inotify_fd, watched_names)
                self._drain_bounded("db_write")
        except Exception as error:
            logger.error(
                f"115助手生活事件数据库阻塞监听退出：{type(error).__name__}: {error}；"
                "不会降级为定时轮询"
            )
            # One finite attempt remains useful when inotify cannot start.
            self._drain_bounded("watcher_failure_once")
        finally:
            if inotify_fd is not None:
                try:
                    os.close(inotify_fd)
                except OSError:
                    pass

    def start(self) -> bool:
        with self._lifecycle_lock:
            if self._thread and self._thread.is_alive():
                return True
            db_path = self._database_path()
            if db_path is None:
                logger.error(
                    "无法定位115助手数据库；只执行一次增量恢复，不启动任何轮询"
                )
                self._drain_bounded("startup_without_watch")
                return False
            if not db_path.parent.is_dir():
                logger.error(
                    f"115助手数据库目录不存在：{db_path.parent}；不启动轮询"
                )
                return False
            self._stop_event.clear()
            self._stop_pipe_r, self._stop_pipe_w = os.pipe2(
                os.O_CLOEXEC | os.O_NONBLOCK
            )
            self._thread = threading.Thread(
                target=self._watch_worker,
                args=(db_path,),
                name="p115strgmsub-life-event-inotify",
                daemon=True,
            )
            self._thread.start()
            mode = self.helper_pull_mode()
            if mode == "last":
                logger.info(
                    "115助手生活事件恢复模式=last；MP整理暂停期间的事件将由助手原游标补拉"
                )
            else:
                logger.warning(
                    f"115助手生活事件恢复模式={mode or 'unknown'}，暂停期间事件未必补拉；"
                    "插件将依靠MP整理结束后的一次性定向补偿，不会循环扫描目录"
                )
            return True

    def stop(self, timeout: float = 5.0) -> None:
        with self._lifecycle_lock:
            thread = self._thread
            if not thread:
                return
            self._stop_event.set()
            if self._stop_pipe_w is not None:
                try:
                    os.write(self._stop_pipe_w, b"x")
                except OSError:
                    pass
            thread.join(timeout=max(0.1, float(timeout)))
            for fd_name in ("_stop_pipe_r", "_stop_pipe_w"):
                fd = getattr(self, fd_name)
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    setattr(self, fd_name, None)
            self._thread = None
