"""
同步处理模块
负责核心的同步逻辑：处理电影订阅、处理电视剧订阅
"""
import datetime
import hashlib
import os
import re
import uuid
from pathlib import Path
from urllib.parse import unquote

import pytz
from typing import List, Dict, Any, Set, Optional, Callable

from app.core.config import global_vars, settings
from app.core.metainfo import MetaInfo
from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.db import SessionFactory
from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType, NotificationType
from app.utils.string import StringUtils

from ..utils import FileMatcher, SubscribeFilter
from .search import SearchHandler
from .subscribe import SubscribeHandler
from .release_gate import ReleaseGateStore
from .lifecycle import LifecycleStore
from .season_wash import select_complete_uniform_ed2k_pack


class SyncHandler:
    """同步处理器"""

    AYCLUB_DAILY_REFRESH_DATA_KEY = "ayclub_daily_refresh_state"
    # 保留旧的 22:00-23:59 手动晚间窗口；配置 cron 的最后一轮另由
    # scheduled_evening_refresh 显式授权，不再依赖这里的固定小时。
    AYCLUB_REFRESH_WINDOW_START_HOUR = 22
    AYCLUB_REFRESH_WINDOW_END_HOUR = 24
    AYCLUB_DAILY_REFRESH_RETENTION_DAYS = 30
    ED2K_DISPATCH_DATA_KEY = "ed2k_dispatch_history"
    ED2K_DISPATCH_TTL_HOURS = 24
    ED2K_MIGRATION_DATA_KEY = "ed2k_dispatch_migration_v189"
    ED2K_EPISODE_MIGRATION_DATA_KEY = "ed2k_episode_mapping_migration_v190"
    ED2K_CONTRACT_MIGRATION_DATA_KEY = "ed2k_contract_migration_v192"
    COMPLETED_ED2K_WASH_DATA_KEY = "completed_ed2k_season_wash_state"
    COMPLETED_ED2K_WASH_NO_PACK_RETRY_DAYS = 3
    COMPLETED_ED2K_WASH_PARTIAL_RETRY_DAYS = 1
    COMPLETED_ED2K_WASH_RECHECK_DAYS = 30
    COMPLETED_ED2K_WASH_RETENTION_DAYS = 180

    def __init__(
        self,
        p115_manager,
        search_handler: SearchHandler,
        subscribe_handler: SubscribeHandler,
        chain,
        save_path: str,
        movie_save_path: str,
        classifier_client=None,
        max_transfer_per_sync: int = 50,
        batch_size: int = 20,
        skip_other_season_dirs: bool = True,
        notify: bool = False,
        post_message_func: Callable = None,
        get_data_func: Callable = None,
        save_data_func: Callable = None,
        lifecycle_store: Optional[LifecycleStore] = None
    ):
        """
        初始化同步处理器

        :param p115_manager: 115 客户端管理器
        :param search_handler: 搜索处理器
        :param subscribe_handler: 订阅处理器
        :param chain: MediaChain 实例
        :param save_path: 电视剧转存目录
        :param movie_save_path: 电影转存目录
        :param classifier_client: OpenClaw 七分类客户端
        :param max_transfer_per_sync: 单次同步最大转存数量
        :param batch_size: 批量转存每批文件数
        :param skip_other_season_dirs: 跳过其他季目录
        :param notify: 是否发送通知
        :param post_message_func: 发送消息的函数
        :param get_data_func: 获取数据的函数
        :param save_data_func: 保存数据的函数
        """
        self._p115_manager = p115_manager
        self._search_handler = search_handler
        self._subscribe_handler = subscribe_handler
        self._chain = chain
        self._save_path = save_path
        self._movie_save_path = movie_save_path
        self._classifier_client = classifier_client
        self._max_transfer_per_sync = max_transfer_per_sync
        self._batch_size = batch_size
        self._skip_other_season_dirs = skip_other_season_dirs
        self._notify = notify
        self._post_message = post_message_func
        self._get_data = get_data_func
        self._save_data = save_data_func
        self._lifecycle = lifecycle_store or LifecycleStore(
            get_data_func=get_data_func,
            save_data_func=save_data_func,
        )
        self._release_gate = ReleaseGateStore(
            get_data_func=get_data_func,
            save_data_func=save_data_func,
        )
        self._migrate_ed2k_state_v189()
        self._migrate_ed2k_episode_state_v190()
        self._migrate_ed2k_contract_state_v192()

    def _migrate_ed2k_state_v189(self) -> None:
        """清理 1.8.8 可能由模糊 HTTP 2xx 产生的伪在途状态。"""
        if not self._get_data or not self._save_data:
            return
        try:
            marker = self._get_data(self.ED2K_MIGRATION_DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取 ED2K 1.8.9 迁移标记失败：{error}")
            return
        if isinstance(marker, dict) and marker.get("completed") is True:
            return

        try:
            invalidated = self._lifecycle.invalidate_pending_by_source(
                "ayclub_ed2k",
                reason="1.8.9 严格任务确认迁移：旧 ED2K 在途状态不再可信",
            )
            old_history = self._get_data(self.ED2K_DISPATCH_DATA_KEY) or {}
            old_history_count = len(old_history) if isinstance(old_history, dict) else 0
            self._save_data(self.ED2K_DISPATCH_DATA_KEY, {})
            self._save_data(
                self.ED2K_MIGRATION_DATA_KEY,
                {
                    "completed": True,
                    "invalidated_pending": len(invalidated),
                    "cleared_history": old_history_count,
                    "completed_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
            )
        except Exception as error:
            logger.error(
                f"ED2K 1.8.9 状态迁移未完成，将在下次初始化重试："
                f"{type(error).__name__}"
            )
            return

        logger.warning(
            f"ED2K 1.8.9 状态迁移完成：失效旧在途={len(invalidated)}，"
            f"清空旧去重记录={old_history_count}；后续仅明确活动任务才会阻止搜索"
        )

    def _migrate_ed2k_contract_state_v192(self) -> None:
        """Invalidate plugin-side ED2K state created before backend contract v2."""
        if not self._get_data or not self._save_data:
            return
        try:
            marker = self._get_data(self.ED2K_CONTRACT_MIGRATION_DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取 ED2K 1.9.2 contract 迁移标记失败：{error}")
            return
        if isinstance(marker, dict) and marker.get("done") is True:
            return

        try:
            invalidated = self._lifecycle.invalidate_pending_by_source(
                "ayclub_ed2k",
                reason="1.9.2 ED2K contract v2 迁移：旧后端任务状态不再可信",
            )
            old_history = self._get_data(self.ED2K_DISPATCH_DATA_KEY) or {}
            old_history_count = len(old_history) if isinstance(old_history, dict) else 0
            self._save_data(self.ED2K_DISPATCH_DATA_KEY, {})
            self._save_data(
                self.ED2K_CONTRACT_MIGRATION_DATA_KEY,
                {
                    "done": True,
                    "migrated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "invalidated_pending": len(invalidated),
                    "cleared_history": old_history_count,
                },
            )
        except Exception as error:
            logger.warning(
                f"ED2K 1.9.2 contract 迁移未完成，将在下次初始化重试：{error}"
            )
            return

        logger.warning(
            f"ED2K 1.9.2 contract v2 迁移完成：失效旧在途={len(invalidated)}，"
            f"清空旧去重记录={old_history_count}；旧后端任务不会被自动接管"
        )

    def _migrate_ed2k_episode_state_v190(self) -> None:
        """清理 1.8.9 可能把无明确集号 ED2K 误标为当前缺集的状态。"""
        if not self._get_data or not self._save_data:
            return
        try:
            marker = self._get_data(self.ED2K_EPISODE_MIGRATION_DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取 ED2K 1.9.0 集号迁移标记失败：{error}")
            return
        if isinstance(marker, dict) and marker.get("completed") is True:
            return

        try:
            invalidated = self._lifecycle.invalidate_pending_by_source(
                "ayclub_ed2k",
                reason="1.9.0 电视剧 ED2K 集号严格匹配迁移：旧映射不再可信",
            )
            old_history = self._get_data(self.ED2K_DISPATCH_DATA_KEY) or {}
            old_history_count = len(old_history) if isinstance(old_history, dict) else 0
            self._save_data(self.ED2K_DISPATCH_DATA_KEY, {})
            self._save_data(
                self.ED2K_EPISODE_MIGRATION_DATA_KEY,
                {
                    "completed": True,
                    "invalidated_pending": len(invalidated),
                    "cleared_history": old_history_count,
                    "completed_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                },
            )
        except Exception as error:
            logger.error(
                f"ED2K 1.9.0 集号迁移未完成，将在下次初始化重试："
                f"{type(error).__name__}"
            )
            return

        logger.warning(
            f"ED2K 1.9.0 集号迁移完成：失效旧在途={len(invalidated)}，"
            f"清空旧去重记录={old_history_count}；后续电视剧 ED2K 必须有明确集号"
        )

    def invalidate_subscription_caches(
        self,
        *,
        media_type: str,
        tmdb_id: Optional[int],
        season: Optional[int] = None,
    ) -> None:
        self._release_gate.invalidate_media(
            media_type=media_type,
            tmdb_id=tmdb_id,
            season=season,
        )

    def _current_cycle_history(
        self,
        history: List[dict],
        subscribe,
        media_key: str,
    ) -> List[dict]:
        generation = self._lifecycle.generation(int(subscribe.id))
        return [
            item for item in history or []
            if int(item.get("subscribe_id") or -1) == int(subscribe.id)
            and int(item.get("generation") or -1) == generation
            and item.get("media_key") == media_key
        ]

    @staticmethod
    def _extract_missing_episodes(
        no_exists: Any,
        mediakey: Any,
        season: int,
    ) -> List[int]:
        if not no_exists or mediakey is None:
            return []
        season_map = no_exists.get(mediakey, {}) if isinstance(no_exists, dict) else {}
        info = season_map.get(season) if isinstance(season_map, dict) else None
        if not info:
            return []
        episodes = list(getattr(info, "episodes", None) or [])
        if not episodes and getattr(info, "total_episode", None):
            start = int(getattr(info, "start_episode", None) or 1)
            episodes = list(range(start, int(info.total_episode) + 1))
        result = []
        for episode in episodes:
            try:
                number = int(episode)
            except (TypeError, ValueError):
                continue
            if number > 0 and number not in result:
                result.append(number)
        return sorted(result)

    @staticmethod
    def _resource_episode_set(resource: Dict[str, Any]) -> Set[int]:
        """提取可信集号；ED2K 以标题/真实文件名为准，防止把 2160p 当集号范围。"""
        structured: Set[int] = set()
        values = resource.get("episodes") or []
        if not isinstance(values, (list, tuple, set)):
            values = []
        for value in values:
            try:
                episode = int(value)
            except (TypeError, ValueError):
                continue
            if 0 < episode <= 999:
                structured.add(episode)
        try:
            episode = int(resource.get("episode"))
            if 0 < episode <= 999:
                structured.add(episode)
        except (TypeError, ValueError):
            pass
        try:
            range_start = int(resource.get("episode_start"))
            range_end = int(resource.get("episode_end") or range_start)
            if 0 < range_start <= range_end <= 999:
                structured.update(range(range_start, range_end + 1))
        except (TypeError, ValueError):
            pass

        title = str(resource.get("title") or "")
        parse_texts = [title]
        source_url = str(resource.get("url") or "").strip()
        is_ed2k = source_url.casefold().startswith("ed2k://|file|")
        if is_ed2k:
            parts = source_url.split("|")
            if len(parts) >= 4:
                try:
                    file_name = unquote(parts[2]).strip()
                except Exception:
                    file_name = str(parts[2] or "").strip()
                if file_name and file_name not in parse_texts:
                    parse_texts.append(file_name)

        parse_text = " ".join(value for value in parse_texts if value)
        parsed: Set[int] = set()

        # 范围必须在第二个集号后结束；S01E17-2160p 不能误判成 E17-E216。
        for match in re.finditer(
            r"(?:S\d{1,2})?E0*(\d{1,3})\s*[-~至—_]\s*(?:E\s*)?0*(\d{1,3})(?![0-9pP])",
            parse_text,
            flags=re.IGNORECASE,
        ):
            try:
                range_start = int(match.group(1))
                range_end = int(match.group(2))
            except (TypeError, ValueError):
                continue
            if 0 < range_start <= range_end <= 999:
                parsed.update(range(range_start, range_end + 1))

        # 单集独立提取，允许文件名后接分辨率等字段。
        for match in re.finditer(
            r"(?:S\d{1,2})?E0*(\d{1,3})(?!\d)",
            parse_text,
            flags=re.IGNORECASE,
        ):
            try:
                episode = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < episode <= 999:
                parsed.add(episode)

        for match in re.finditer(r"第\s*0*(\d{1,3})\s*集", parse_text):
            try:
                episode = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < episode <= 999:
                parsed.add(episode)

        # ED2K 的真实文件名最可信，不能让桥接旧缓存中的错误范围覆盖它。
        if is_ed2k:
            return parsed
        return structured | parsed

    @staticmethod
    def _resource_season_set(resource: Dict[str, Any]) -> Set[int]:
        result: Set[int] = set()
        raw_season = resource.get("season")
        try:
            if raw_season is not None and int(raw_season) > 0:
                result.add(int(raw_season))
        except (TypeError, ValueError):
            pass

        parse_texts = [str(resource.get("title") or "")]
        source_url = str(resource.get("url") or "").strip()
        if source_url.casefold().startswith("ed2k://|file|"):
            parts = source_url.split("|")
            if len(parts) >= 4:
                try:
                    file_name = unquote(parts[2]).strip()
                except Exception:
                    file_name = str(parts[2] or "").strip()
                if file_name:
                    parse_texts.append(file_name)

        parse_text = " ".join(value for value in parse_texts if value)
        for match in re.finditer(r"S\s*0*(\d{1,2})(?=E|[ ._\-])", parse_text, re.I):
            try:
                season = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if season > 0:
                result.add(season)
        return result

    @staticmethod
    def _normalized_title_token(value: str) -> str:
        return "".join(char.casefold() for char in str(value or "") if char.isalnum())

    @staticmethod
    def _episode_identity_from_text(
        value: str,
        *,
        default_season: Optional[int] = None,
    ) -> tuple[Set[int], Set[int]]:
        """Extract explicit season/episode identity without resolution collisions."""
        text = str(value or "")
        seasons: Set[int] = set()
        episodes: Set[int] = set()
        for match in re.finditer(r"S\s*0*(\d{1,3})(?=E|[ ._\-])", text, re.I):
            try:
                number = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < number <= 999:
                seasons.add(number)
        for match in re.finditer(r"(?:Season|第)\s*0*(\d{1,3})\s*(?:季)?", text, re.I):
            try:
                number = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < number <= 999:
                seasons.add(number)
        for match in re.finditer(
            r"(?:S\s*\d{1,3})?E\s*0*(\d{1,3})\s*[-~至—_]\s*(?:E\s*)?0*(\d{1,3})(?![0-9pP])",
            text,
            re.I,
        ):
            try:
                start, end = int(match.group(1)), int(match.group(2))
            except (TypeError, ValueError):
                continue
            if 0 < start <= end <= 999 and end - start <= 200:
                episodes.update(range(start, end + 1))
        for match in re.finditer(r"(?:S\s*\d{1,3})?E\s*0*(\d{1,3})(?!\d)", text, re.I):
            try:
                number = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < number <= 999:
                episodes.add(number)
        for match in re.finditer(r"第\s*0*(\d{1,3})\s*[集话話]", text):
            try:
                number = int(match.group(1))
            except (TypeError, ValueError):
                continue
            if 0 < number <= 999:
                episodes.add(number)
        if episodes and not seasons and default_season:
            seasons.add(int(default_season))
        return seasons, episodes

    def _scan_local_strm_episodes(
        self,
        *,
        mediainfo: MediaInfo,
        season: int,
    ) -> tuple[str, Set[int], List[str]]:
        """Return current playable episodes from the local STRM tree.

        Status is ``ok``, ``not_found`` or ``unavailable``.  A matched show
        directory with zero STRM files is still an authoritative ``ok`` result.
        """
        root = Path(os.getenv("P115STRGMSUB_STRM_ROOT", "/STRM")).expanduser()
        if not root.is_dir():
            return "unavailable", set(), []

        title_token = self._normalized_title_token(getattr(mediainfo, "title", ""))
        tmdb_id = getattr(mediainfo, "tmdb_id", None)
        tmdb_tokens = {
            f"tmdb-{int(tmdb_id)}",
            f"tmdb={int(tmdb_id)}",
            f"tmdb_{int(tmdb_id)}",
        } if tmdb_id else set()
        candidates: List[Path] = []
        tmdb_candidates: List[Path] = []

        try:
            for current, dirs, _files in os.walk(root):
                current_path = Path(current)
                try:
                    depth = len(current_path.relative_to(root).parts)
                except ValueError:
                    continue
                if depth > 5:
                    dirs[:] = []
                    continue
                folded_path = str(current_path).casefold()
                relative_parts = current_path.relative_to(root).parts
                normalized_parts = [
                    self._normalized_title_token(part)
                    for part in relative_parts
                ]
                matched_tmdb = any(token in folded_path for token in tmdb_tokens)
                matched_title = bool(
                    title_token
                    and any(
                        part == title_token or part.startswith(title_token)
                        for part in normalized_parts
                    )
                )
                if not (matched_tmdb or matched_title):
                    continue
                candidate = current_path
                if re.fullmatch(r"(?:Season|S|第)\s*0*\d{1,3}\s*(?:季)?", candidate.name, re.I):
                    candidate = candidate.parent
                target = tmdb_candidates if matched_tmdb else candidates
                if candidate not in target:
                    target.append(candidate)
        except OSError as error:
            logger.warning(f"扫描本地 STRM 目录失败：{type(error).__name__}")
            return "unavailable", set(), []

        selected = tmdb_candidates or candidates
        if not selected:
            return "not_found", set(), []

        # Keep only top-most unique show roots; title matches may also hit a
        # Season directory or a nested metadata folder.
        unique: List[Path] = []
        for candidate in sorted(selected, key=lambda item: len(item.parts)):
            if any(candidate == old or old in candidate.parents for old in unique):
                continue
            unique.append(candidate)

        episodes: Set[int] = set()
        scanned = 0
        for show_root in unique[:8]:
            try:
                for item in show_root.rglob("*.strm"):
                    scanned += 1
                    if scanned > 5000:
                        logger.warning("本地 STRM 对账超过 5000 个文件，停止继续扫描")
                        break
                    relative_text = str(item.relative_to(show_root))
                    seasons, found = self._episode_identity_from_text(
                        relative_text,
                        default_season=season,
                    )
                    if found and (not seasons or int(season) in seasons):
                        episodes.update(found)
            except (OSError, ValueError):
                continue
        return "ok", episodes, [str(item) for item in unique[:8]]

    @staticmethod
    def _share_file_name(item: Dict[str, Any]) -> str:
        return str(
            item.get("name") or item.get("n") or item.get("fn")
            or item.get("file_name") or item.get("title") or ""
        ).strip()

    def _complete_pack_episode_set(
        self,
        *,
        resource: Dict[str, Any],
        share_files: List[Dict[str, Any]],
        season: int,
        total_episode: int,
        missing_episodes: List[int],
    ) -> Set[int]:
        """Verify and return all episodes of a complete single-season pack."""
        kind = str(resource.get("resource_kind") or "").strip().casefold()
        complete_marker = bool(resource.get("is_complete_season")) or kind == "season_pack"
        try:
            declared_total = int(resource.get("declared_total_episodes") or 0)
        except (TypeError, ValueError):
            declared_total = 0
        if not complete_marker and not declared_total:
            return set()

        explicit_seasons = self._resource_season_set(resource)
        if explicit_seasons and int(season) not in explicit_seasons:
            return set()

        metadata_available = set(self._resource_episode_set(resource))
        share_available: Set[int] = set()
        for item in share_files or []:
            name = self._share_file_name(item)
            seasons, episodes = self._episode_identity_from_text(
                name,
                default_season=season,
            )
            if (
                len(episodes) == 1
                and (not seasons or int(season) in seasons)
            ):
                share_available.update(episodes)

        wanted_missing = {int(value) for value in missing_episodes if int(value) > 0}
        if not share_available or not (share_available & wanted_missing):
            return set()

        expected_total = int(total_episode or declared_total or 0)
        if expected_total > 0:
            complete = set(range(1, expected_total + 1))
            # Expansion requires real per-episode files in the share.  Metadata
            # alone must never make one archive/range file appear 40 times.
            if complete.issubset(share_available):
                return complete
        if declared_total > 0:
            complete = set(range(1, declared_total + 1))
            if complete.issubset(share_available):
                return complete
        # No trusted total: require both metadata and the actual share to
        # describe the same contiguous sequence starting at E01.
        if (
            share_available
            and share_available == set(range(1, max(share_available) + 1))
            and (not metadata_available or metadata_available.issubset(share_available))
        ):
            return share_available
        return set()

    def _select_tv_ed2k_candidate_episodes(
        self,
        *,
        resource: Dict[str, Any],
        source_episodes: List[int],
        season: int,
    ) -> tuple[List[int], str]:
        """电视剧 ED2K 只允许明确季集号与当前缺集相交的候选。"""
        explicit_episodes = self._resource_episode_set(resource)
        if not explicit_episodes:
            return [], "missing_episode_metadata"

        explicit_seasons = self._resource_season_set(resource)
        if explicit_seasons and int(season) not in explicit_seasons:
            return [], "season_mismatch"

        candidate_episodes = sorted(
            {int(value) for value in source_episodes if int(value) > 0}
            & explicit_episodes
        )
        if not candidate_episodes:
            return [], "episode_mismatch"
        return candidate_episodes, "matched"


    @staticmethod
    def _is_ed2k_resource(resource: Dict[str, Any]) -> bool:
        source_kind = str(resource.get("source_kind") or "").strip().casefold()
        source_url = str(resource.get("url") or "").strip()
        lowered = source_url.casefold()
        return bool(
            source_kind in {"", "ed2k"}
            and 20 <= len(source_url) <= 16384
            and "\r" not in source_url
            and "\n" not in source_url
            and lowered.startswith("ed2k://|file|")
            and lowered.endswith("|/")
            and source_url.count("|") >= 5
        )

    @staticmethod
    def _ed2k_source_ref(source_url: str) -> str:
        return hashlib.sha256(
            (source_url or "").encode("utf-8")
        ).hexdigest()[:16]

    def _load_ed2k_dispatch_history(self) -> Dict[str, Dict[str, Any]]:
        if not self._get_data:
            return {}
        try:
            raw = self._get_data(self.ED2K_DISPATCH_DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取 ED2K 提交去重记录失败：{error}")
            return {}
        if not isinstance(raw, dict):
            return {}

        now = datetime.datetime.now(datetime.timezone.utc)
        retained: Dict[str, Dict[str, Any]] = {}
        changed = False
        for key, value in raw.items():
            if not isinstance(value, dict):
                changed = True
                continue
            submitted_at = self._parse_utc_datetime(value.get("submitted_at"))
            if not submitted_at:
                changed = True
                continue
            age_hours = (now - submitted_at).total_seconds() / 3600
            if age_hours <= self.ED2K_DISPATCH_TTL_HOURS:
                retained[str(key)] = value
            else:
                changed = True

        if changed:
            self._save_ed2k_dispatch_history(retained)
        return retained

    def _save_ed2k_dispatch_history(
        self,
        history: Dict[str, Dict[str, Any]],
    ) -> None:
        if not self._save_data:
            return
        try:
            self._save_data(self.ED2K_DISPATCH_DATA_KEY, history)
        except Exception as error:
            logger.warning(f"保存 ED2K 提交去重记录失败：{error}")

    def _register_ed2k_pending(
        self,
        *,
        subscribe: Any,
        media_key: str,
        source_ref: str,
        resource_title: str,
        episodes: Optional[List[int]],
        attempt_id: str,
    ) -> None:
        normalized_episodes = []
        for value in episodes or []:
            try:
                episode = int(value)
            except (TypeError, ValueError):
                continue
            if episode > 0 and episode not in normalized_episodes:
                normalized_episodes.append(episode)
        normalized_episodes.sort()

        if normalized_episodes:
            file_items = [
                {
                    "episode": episode,
                    "id": f"ed2k:{source_ref}:{attempt_id}:{episode}",
                    "name": resource_title,
                }
                for episode in normalized_episodes
            ]
        else:
            file_items = [{
                "id": f"ed2k:{source_ref}:{attempt_id}",
                "name": resource_title,
            }]

        self._lifecycle.add_pending(
            subscribe=subscribe,
            media_key=media_key,
            episodes=normalized_episodes or None,
            file_items=file_items,
            share_ref=f"ed2k:{source_ref}",
            target_path="/OpenClaw_ED2K下载中",
            source="ayclub_ed2k",
        )

    def _dispatch_ed2k_resource(
        self,
        *,
        subscribe: Any,
        media_key: str,
        resource: Dict[str, Any],
        mediainfo: MediaInfo,
        media_type: str,
        season: Optional[int] = None,
        episodes: Optional[List[int]] = None,
    ) -> tuple[bool, bool]:
        """提交 ED2K；返回 (已接受或去重命中, 是否去重命中)。"""
        source_url = str(resource.get("url") or "").strip()
        resource_title = str(resource.get("title") or "").strip()
        if resource_title.casefold().startswith("ed2k://"):
            resource_title = "ED2K resource"
        resource_title = resource_title[:500]
        source_ref = self._ed2k_source_ref(source_url)

        if not self._is_ed2k_resource(resource):
            return False, False

        if (
            not self._classifier_client
            or not hasattr(self._classifier_client, "submit_ed2k")
        ):
            logger.warning(
                f"无法提交 ED2K：OpenClaw 客户端未就绪，ref={source_ref}"
            )
            return False, False

        subscribe_id = int(getattr(subscribe, "id"))
        generation = self._lifecycle.generation(subscribe_id)
        dispatch_key = f"{subscribe_id}:{generation}:{source_ref}"
        history = self._load_ed2k_dispatch_history()
        old = history.get(dispatch_key) or {}

        if old:
            share_ref = f"ed2k:{source_ref}"
            if self._lifecycle.has_live_pending_reference(
                media_key=media_key,
                share_ref=share_ref,
                episodes=episodes,
            ):
                logger.info(
                    f"ED2K 本订阅周期已有真实活动在途，跳过重复 POST："
                    f"ref={source_ref}"
                )
                return True, True

            # 去重历史本身不能重新制造 pending_transfer。若历史记录没有对应
            # 活动在途，删除旧历史并重新提交，让后端明确返回 queued/started 等。
            history.pop(dispatch_key, None)
            self._save_ed2k_dispatch_history(history)
            logger.warning(
                f"ED2K 去重记录缺少对应活动在途，已废弃并重新提交："
                f"ref={source_ref}"
            )

        result = self._classifier_client.submit_ed2k(
            source_url=source_url,
            media_type=media_type,
            title=mediainfo.title,
            year=mediainfo.year,
            tmdb_id=mediainfo.tmdb_id,
            season=season,
            episodes=episodes,
            resource_title=resource_title,
            request_id=dispatch_key,
        )
        if not result:
            return False, False

        now = datetime.datetime.now(datetime.timezone.utc)
        attempt_id = str(int(now.timestamp()))
        history[dispatch_key] = {
            "subscribe_id": subscribe_id,
            "generation": generation,
            "media_key": media_key,
            "source_ref": source_ref,
            "resource_title": resource_title,
            "episodes": list(episodes or []),
            "attempt_id": attempt_id,
            "submitted_at": now.isoformat(),
            "status": str(result.get("status") or "queued"),
            "contract_version": int(result.get("contract_version") or 0),
            "backend_job_id": str(result.get("job_id") or ""),
            "request_id": dispatch_key,
        }
        self._save_ed2k_dispatch_history(history)
        self._register_ed2k_pending(
            subscribe=subscribe,
            media_key=media_key,
            source_ref=source_ref,
            resource_title=resource_title,
            episodes=episodes,
            attempt_id=attempt_id,
        )
        logger.info(
            f"ED2K 已交给 OpenClaw/p115 后端：ref={source_ref}，"
            f"等待 MoviePilot 入库事件"
        )
        return True, False

    def _prefilter_ayclub_results(
        self,
        resources: List[Dict[str, Any]],
        missing_episodes: List[int],
        season: int,
    ) -> List[Dict[str, Any]]:
        """打开115分享前淘汰明确不覆盖当前缺集的 AYCLUB 候选。"""
        missing = {int(ep) for ep in missing_episodes}
        kept: List[Dict[str, Any]] = []
        filtered: List[str] = []

        for resource in resources or []:
            title = str(resource.get("title") or "")
            explicit_season = resource.get("season")
            season_mismatch = False
            try:
                if explicit_season is not None:
                    season_mismatch = int(explicit_season) != int(season)
            except (TypeError, ValueError):
                pass
            if explicit_season is None:
                title_seasons = {
                    int(value)
                    for value in re.findall(r"S(\d{1,2})", title, flags=re.IGNORECASE)
                }
                if len(title_seasons) == 1:
                    season_mismatch = int(season) not in title_seasons

            episodes = self._resource_episode_set(resource)
            if season_mismatch or (episodes and not (episodes & missing)):
                filtered.append(title)
                continue
            # 集号未知的整季/合集仍保留，稍后打开分享核验真实文件。
            kept.append(resource)

        if filtered:
            logger.info(
                f"AYCLUB候选预过滤：跳过 {len(filtered)} 个明确不覆盖当前缺集的资源，"
                f"保留 {len(kept)} 个待验证候选"
            )
            logger.debug(f"AYCLUB预过滤示例：{filtered[:5]}")
        return kept

    @staticmethod
    def _parse_utc_datetime(value: Any) -> Optional[datetime.datetime]:
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
    def _ayclub_daily_refresh_key(tmdb_id: int, season: int) -> str:
        return f"tv:{int(tmdb_id)}:S{int(season)}"

    @staticmethod
    def _ayclub_local_timezone():
        try:
            return pytz.timezone(settings.TZ)
        except Exception:
            logger.warning(
                f"MoviePilot 时区 {getattr(settings, 'TZ', None)!r} 无效，"
                "AYCLUB 晚间窗口回退 Asia/Shanghai"
            )
            return pytz.timezone("Asia/Shanghai")

    def _ayclub_local_now(self) -> datetime.datetime:
        return datetime.datetime.now(tz=self._ayclub_local_timezone())

    def _ayclub_in_refresh_window(
        self,
        now: Optional[datetime.datetime] = None,
    ) -> bool:
        current = now or self._ayclub_local_now()
        if current.tzinfo is None:
            current = self._ayclub_local_timezone().localize(current)
        hour = int(current.hour)
        return (
            self.AYCLUB_REFRESH_WINDOW_START_HOUR
            <= hour
            < self.AYCLUB_REFRESH_WINDOW_END_HOUR
        )

    def _load_ayclub_daily_refresh_state(self) -> Dict[str, Dict[str, Any]]:
        if not self._get_data:
            return {}
        try:
            raw = self._get_data(self.AYCLUB_DAILY_REFRESH_DATA_KEY) or {}
            if not isinstance(raw, dict):
                return {}
            return {
                str(key): value
                for key, value in raw.items()
                if isinstance(value, dict)
            }
        except Exception as error:
            logger.warning(f"读取 AYCLUB 每日真实搜索状态失败：{error}")
            return {}

    def _save_ayclub_daily_refresh_state(
        self,
        states: Dict[str, Dict[str, Any]],
    ) -> None:
        if not self._save_data:
            return
        try:
            self._save_data(self.AYCLUB_DAILY_REFRESH_DATA_KEY, states)
        except Exception as error:
            logger.warning(f"保存 AYCLUB 每日真实搜索状态失败：{error}")

    def _record_ayclub_daily_refresh(
        self,
        *,
        tmdb_id: int,
        season: int,
        status: str,
        reason: str,
        force_honored: bool = False,
    ) -> None:
        local_now = self._ayclub_local_now()
        utc_now = local_now.astimezone(datetime.timezone.utc)
        states = self._load_ayclub_daily_refresh_state()
        retained: Dict[str, Dict[str, Any]] = {}

        for key, value in states.items():
            checked_at = self._parse_utc_datetime(value.get("checked_at"))
            if not checked_at:
                continue
            age_days = (utc_now - checked_at).total_seconds() / 86400
            if age_days <= self.AYCLUB_DAILY_REFRESH_RETENTION_DAYS:
                retained[key] = value

        key = self._ayclub_daily_refresh_key(tmdb_id, season)
        retained[key] = {
            "tmdb_id": int(tmdb_id),
            "season": int(season),
            "local_date": local_now.date().isoformat(),
            "checked_at": utc_now.isoformat(),
            "status": str(status or "unknown"),
            "reason": str(reason or "unknown"),
            "force_honored": bool(force_honored),
        }
        self._save_ayclub_daily_refresh_state(retained)

    def _ayclub_daily_refresh_due(
        self,
        *,
        tmdb_id: int,
        season: int,
        now: Optional[datetime.datetime] = None,
    ) -> bool:
        current = now or self._ayclub_local_now()
        if current.tzinfo is None:
            current = self._ayclub_local_timezone().localize(current)
        key = self._ayclub_daily_refresh_key(tmdb_id, season)
        record = self._load_ayclub_daily_refresh_state().get(key) or {}
        return str(record.get("local_date") or "") != current.date().isoformat()

    def _ayclub_tv_query_mode(
        self,
        *,
        tmdb_id: Optional[int],
        season: int,
        lifecycle_force_refresh: bool,
        scheduled_evening_refresh: bool = False,
    ) -> tuple[bool, bool, str]:
        """决定本轮 AYCLUB 是真实查询还是严格只读缓存。

        cron 当天最后一轮优先于固定晚间窗口和每日去重；普通手动或
        生命周期定向同步不会自动获得该标记。
        返回：(force_refresh, cache_only, reason)。
        """
        if scheduled_evening_refresh and tmdb_id:
            # 配置 cron 的当天最后一轮必须真实搜索。即使白天曾由新增订阅
            # 或 reset 做过一次强刷，也不能把最后定时轮次降级为只读缓存。
            return True, False, "scheduled_evening_refresh"

        if lifecycle_force_refresh:
            return True, False, "lifecycle_force_refresh"

        now = self._ayclub_local_now()
        if not tmdb_id:
            return False, True, "cache_only_missing_tmdb"

        in_legacy_evening_window = self._ayclub_in_refresh_window(now)
        if not scheduled_evening_refresh and not in_legacy_evening_window:
            return False, True, "cache_only_outside_evening_window"

        if not self._ayclub_daily_refresh_due(
            tmdb_id=int(tmdb_id),
            season=int(season),
            now=now,
        ):
            return False, True, "cache_only_already_refreshed_today"

        return True, False, "evening_refresh_window"

    def _record_ayclub_query_if_real(
        self,
        *,
        tmdb_id: Optional[int],
        season: int,
        reason: str,
    ) -> bool:
        if not tmdb_id:
            return False

        status = self._search_handler.get_ayclub_last_status()
        cached = self._search_handler.get_ayclub_last_cached()
        force_honored = self._search_handler.was_ayclub_force_refresh_honored()
        real_statuses = {"ok_matched", "ok_empty", "invalid_result"}
        real_query = bool(force_honored or (cached is False and status in real_statuses))

        if real_query:
            self._record_ayclub_daily_refresh(
                tmdb_id=int(tmdb_id),
                season=int(season),
                status=status,
                reason=reason,
                force_honored=force_honored,
            )
        return real_query

    def _sort_ayclub_results(
        self,
        resources: List[Dict[str, Any]],
        missing_episodes: List[int],
        season: int,
        season_complete: bool,
        subscribe_filter,
    ) -> List[Dict[str, Any]]:
        missing = set(missing_episodes)

        def score(resource: Dict[str, Any]) -> tuple:
            title = str(resource.get("title") or "")
            kind = str(resource.get("resource_kind") or "").lower()
            episodes = self._resource_episode_set(resource)
            coverage = len(episodes & missing) if episodes else 0
            explicit_complete = bool(resource.get("is_complete_season"))
            lower_title = title.lower()
            title_complete = any(
                token in lower_title
                for token in ("complete", "全集", "全季", "完结", "全 ")
            )
            season_tokens = {
                f"s{int(season):02d}".lower(),
                f"s{int(season)}".lower(),
                f"第{int(season)}季",
            }
            if not kind:
                if resource.get("episode") is not None:
                    kind = "single"
                elif explicit_complete or title_complete:
                    kind = "season_pack"
                elif episodes and len(episodes) > 1:
                    kind = "multi_episode"
                elif (
                    any(token in lower_title for token in season_tokens)
                    and not re.search(r"s\d{1,2}e\d{1,3}", lower_title)
                ):
                    kind = "season_pack"
            season_value = resource.get("season")
            season_score = 0
            try:
                if season_value is not None:
                    season_score = 40 if int(season_value) == int(season) else -100
            except (TypeError, ValueError):
                pass
            # 桥接明确标记完整整季时始终优先；这是 AYCLUB 的高质量资源。
            kind_score = 0
            if explicit_complete:
                kind_score = 500
            elif season_complete and kind == "season_pack":
                kind_score = 400
            elif kind == "multi_episode":
                kind_score = 240
            elif kind == "single":
                kind_score = 220 if len(missing) <= 2 else 100
            elif kind == "season_pack":
                kind_score = 180
            filter_score = 0
            try:
                if subscribe_filter and subscribe_filter.has_filters():
                    _, filter_score = subscribe_filter.match(title)
            except Exception:
                filter_score = 0
            if title_complete:
                # 当前桥接尚未返回结构化字段时，也能识别“全集/完结/Complete”。
                kind_score = max(kind_score, 500)
            return (
                season_score + kind_score + coverage * 25 + int(filter_score or 0),
                coverage,
                len(episodes),
            )

        return sorted(resources or [], key=score, reverse=True)

    @staticmethod
    def _safe_share_ref(share_url: str) -> str:
        """
        生成不可逆的分享链接标识。

        完整分享链接只在当前转存流程内使用，
        不写入日志、插件历史或下载历史。
        """
        if not share_url:
            return "115share#empty"

        digest = hashlib.sha256(
            share_url.encode("utf-8")
        ).hexdigest()[:12]

        return f"115share#{digest}"
    def _resolve_target_root(
        self,
        share_url: str,
        media_type: str,
        title: str,
        fallback_root: str,
        year: Optional[int] = None,
        tmdb_id: Optional[int] = None,
        season: Optional[int] = None,
        resource_title: str = "",
        file_names: Optional[List[str]] = None,
    ) -> Optional[str]:
        """
        获取七分类目标根目录。

        1.9.5 起所有 115 写操作必须经过独立 OpenClaw 后端。
        ``fallback_root`` 仅保留函数签名兼容，不再允许绕过边界校验。
        """
        del fallback_root
        if not self._classifier_client or not self._classifier_client.is_ready:
            logger.warning(
                "OpenClaw 115 执行服务未配置完整，拒绝直接在 MoviePilot 内转存"
            )
            return None

        result = self._classifier_client.inspect_share(
            share_url=share_url,
            media_type=media_type,
            title=title,
            year=year,
            tmdb_id=tmdb_id,
            season=season,
            resource_title=resource_title,
            file_names=file_names,
        )

        if not result:
            logger.warning(
                f"分类失败或需要人工确认，跳过转存："
                f"{title} - {resource_title}"
            )
            return None

        return result["target_dir"]

    def reconcile_subscribe_with_mp(self, subscribe: Any) -> bool:
        """仅使用 MoviePilot 官方媒体库口径确认在途任务，不触发资源搜索。

        返回 True 表示本次完成了有效的 MP 缺失状态读取；False 表示识别或
        查询失败，调用方应继续保持 PT 屏蔽。
        """
        try:
            is_tv = subscribe.type == MediaType.TV.value
            media_type = MediaType.TV if is_tv else MediaType.MOVIE
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.type = media_type
            if is_tv:
                meta.begin_season = subscribe.season or 1

            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=media_type,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True,
            )
            if not mediainfo:
                logger.warning(
                    f"PT开放前无法识别媒体，继续保持屏蔽：{subscribe.name}"
                )
                return False

            mp_subscribe_chain = SubscribeChain()
            if is_tv:
                try:
                    mp_subscribe_chain.refresh_subscribe_progress(
                        subscribe=subscribe,
                        scene="pt_unblock_gate",
                    )
                except Exception as error:
                    logger.warning(
                        f"PT开放前刷新订阅 {subscribe.id} 进度失败，"
                        f"继续使用 MP 缺失接口确认：{error}"
                    )

            mediakey = mediainfo.tmdb_id or mediainfo.douban_id
            exist_flag, no_exists = mp_subscribe_chain.resolve_subscribe_missing(
                subscribe=subscribe,
                meta=meta,
                mediainfo=mediainfo,
                mediakey=mediakey,
            )
            media_key = self._lifecycle.media_key_from_subscribe(subscribe)

            if exist_flag:
                confirmed = self._lifecycle.reconcile_missing(
                    media_key,
                    media_satisfied=True,
                )
                logger.info(
                    f"PT开放前 MP/媒体库确认已满足：{subscribe.name}，"
                    f"确认在途任务={len(confirmed)}"
                )
                return True

            if not is_tv:
                logger.info(
                    f"PT开放前 MP/媒体库仍未确认电影入库：{subscribe.name}"
                )
                return True

            season = int(meta.begin_season or 1)
            missing_episodes = self._extract_missing_episodes(
                no_exists=no_exists,
                mediakey=mediakey,
                season=season,
            )
            if not missing_episodes:
                logger.warning(
                    f"PT开放前 MP 返回未满足但没有可解析的缺失集："
                    f"{subscribe.name} S{season}，本轮不放行"
                )
                return False

            confirmed = self._lifecycle.reconcile_missing(
                media_key,
                missing_episodes=missing_episodes,
            )
            logger.info(
                f"PT开放前 MP/媒体库缺失确认：{subscribe.name} S{season}，"
                f"仍缺={missing_episodes}，确认在途任务={len(confirmed)}"
            )
            return True

        except Exception as error:
            logger.warning(
                f"PT开放前读取 MoviePilot 缺失状态失败："
                f"subscribe_id={getattr(subscribe, 'id', '?')}，错误={error}"
            )
            return False

    def process_movie_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        scheduled_evening_refresh: bool = False,
    ) -> int:
        """
        处理单个电影订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :return: 更新后的转存数量
        """
        try:
            logger.info(f"处理电影订阅：{subscribe.name} ({subscribe.year})")

            # 加载该订阅的历史积分花费（用 tmdb_id 作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_movie" if subscribe.tmdbid else f"{subscribe.name}_movie"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # MoviePilot 是媒体与订阅状态真相源。
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.type = MediaType.MOVIE

            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.MOVIE,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )
            if not mediainfo:
                logger.warning(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count

            media_key = self._lifecycle.media_key_from_subscribe(subscribe)
            self._lifecycle.ensure_subscription(
                int(subscribe.id), subscribe, scene="sync"
            )
            lifecycle_force_refresh = self._lifecycle.peek_force_refresh(int(subscribe.id))

            mp_subscribe_chain = SubscribeChain()
            mediakey = subscribe.tmdbid or subscribe.doubanid
            try:
                exist_flag, _ = mp_subscribe_chain.resolve_subscribe_missing(
                    subscribe=subscribe,
                    meta=meta,
                    mediainfo=mediainfo,
                    mediakey=mediakey,
                )
            except Exception as error:
                logger.warning(f"使用 MP 官方接口判断电影缺失失败：{error}")
                exist_flag = False

            if exist_flag:
                self._lifecycle.reconcile_missing(
                    media_key, media_satisfied=True
                )
                logger.info(f"MoviePilot 确认电影已满足订阅：{mediainfo.title_year}")
                try:
                    mp_subscribe_chain.check_and_handle_existing_media(
                        subscribe=subscribe,
                        meta=meta,
                        mediainfo=mediainfo,
                        mediakey=mediakey,
                    )
                except Exception as error:
                    logger.warning(f"交由 MP 完成电影订阅失败，将由后续对账恢复：{error}")
                return transferred_count

            if self._lifecycle.has_pending_movie(media_key):
                logger.info(
                    f"电影 {mediainfo.title_year} 已投递并等待 MoviePilot 整理，"
                    "本次不重复搜索"
                )
                return transferred_count

            # 历史只参与当前订阅周期的洗版评分，不再决定媒体是否存在。
            movie_history_score = -1
            movie_perfect_match = False
            is_best_version = bool(subscribe.best_version)
            if is_best_version:
                for item in self._current_cycle_history(history, subscribe, media_key):
                    if item.get("type") != "电影" or item.get("status") != "成功":
                        continue
                    score = int(item.get("filter_score") or 0)
                    if score > movie_history_score:
                        movie_history_score = score
                        movie_perfect_match = bool(item.get("perfect_match"))

            # 判断本次是否允许查询 AYCLUB。
            # 门禁只控制 AYCLUB，不影响 PanSou、HDHive、Nullbr。
            movie_gate = {
                "allow_ayclub": False,
                "ayclub_first": False,
                "probe_due": False,
                "released": False,
                "reason": "missing_tmdb_id",
            }

            if mediainfo.tmdb_id:
                movie_gate = self._release_gate.evaluate_movie(
                    int(mediainfo.tmdb_id),
                    theatrical_date=getattr(mediainfo, "release_date", None),
                    lifecycle_force_refresh=lifecycle_force_refresh,
                    scheduled_evening_refresh=scheduled_evening_refresh,
                )
            else:
                logger.info(
                    f"电影 {mediainfo.title} 缺少 TMDB ID，"
                    f"本次不查询 AYCLUB"
                )

            logger.info(
                f"电影 {mediainfo.title} AYCLUB 发布门禁："
                f"允许={movie_gate.get('allow_ayclub')}，"
                f"优先={movie_gate.get('ayclub_first')}，"
                f"原因={movie_gate.get('reason')}，"
                f"模式={'强刷' if movie_gate.get('force_refresh') else ('仅缓存' if movie_gate.get('cache_only') else '禁用')}，"
                f"间隔={movie_gate.get('interval_days')}天，"
                f"下次允许={movie_gate.get('next_search_at')}，"
                f"force_refresh_pending={movie_gate.get('force_refresh_pending')}，"
                f"last_real_search_at={movie_gate.get('last_real_search_at')}，"
                f"retry_after={movie_gate.get('retry_after')}，"
                f"daily_search_count={movie_gate.get('daily_search_count')}，"
                f"no_result_cooldown_until={movie_gate.get('no_result_cooldown_until')}，"
                f"跳过原因={movie_gate.get('skip_reason')}"
            )

            # 防止读取到上一个订阅遗留的 AYCLUB 查询状态
            self._search_handler.reset_ayclub_status()
            ayclub_request_id = uuid.uuid4().hex
            ayclub_attempt_reserved = False
            if (
                mediainfo.tmdb_id
                and movie_gate.get("allow_ayclub")
                and movie_gate.get("force_refresh")
                and "ayclub" in self._search_handler.get_enabled_sources(
                    ayclub_first=True,
                    allow_ayclub=True,
                )
            ):
                ayclub_attempt_reserved = (
                    self._release_gate.reserve_movie_real_search(
                        tmdb_id=int(mediainfo.tmdb_id),
                        trigger_reason=str(
                            movie_gate.get("reason") or "unknown"
                        ),
                        lifecycle_force_refresh=lifecycle_force_refresh,
                        request_id=ayclub_request_id,
                    )
                )
                if not ayclub_attempt_reserved:
                    movie_gate["force_refresh"] = False
                    movie_gate["cache_only"] = True
                    movie_gate["reason"] = "daily_search_limit"
            # 创建订阅过滤条件
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"电影 {subscribe.name} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 延迟逐源搜索：只有当前来源的候选资源全部不可用，
            # 才真正查询下一个来源
            movie_transferred = False
            resource_found = False
            ayclub_usable_candidate = False
            movie_ed2k_dispatched = False

            resource_iterator = self._search_handler.iter_resources(
                mediainfo=mediainfo,
                media_type=MediaType.MOVIE,
                ayclub_first=bool(
                    movie_gate.get("ayclub_first")
                ),
                allow_ayclub=bool(
                    movie_gate.get("allow_ayclub")
                ),
                force_refresh=bool(movie_gate.get("force_refresh")),
                cache_only=bool(movie_gate.get("cache_only")),
                request_id=ayclub_request_id,
                yield_source_end=True,
            )

            for resource in resource_iterator:
                if resource.get("_source_end"):
                    source_name = str(
                        resource.get("search_source") or ""
                    ).casefold()
                    if movie_ed2k_dispatched and source_name == "ayclub":
                        logger.info(
                            "AYCLUB ED2K 已提交且本源无可用 115，"
                            "不再查询后续来源"
                        )
                        break
                    continue

                resource_found = True
                share_url = resource.get("url", "")
                resource_title = resource.get("title", "")
                resource_source = str(
                    resource.get("search_source")
                    or resource.get("source")
                    or ""
                ).lower()
                if self._is_ed2k_resource(resource):
                    accepted, _ = self._dispatch_ed2k_resource(
                        subscribe=subscribe,
                        media_key=media_key,
                        resource=resource,
                        mediainfo=mediainfo,
                        media_type="movie",
                        season=None,
                        episodes=None,
                    )
                    if accepted:
                        movie_ed2k_dispatched = True
                        movie_transferred = True
                        if resource_source == "ayclub":
                            ayclub_usable_candidate = True
                    continue

                # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                if resource.get("need_unlock") and not share_url:
                    slug = resource.get("slug")
                    if slug:
                        logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                        unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                        if not unlocked_url:
                            logger.error(f"未能解锁收费资源: {resource_title}")
                            continue
                        share_url = unlocked_url
                        # 更新当前字典以便历史存入或下次能沿用这个 url
                        resource["url"] = share_url
                        resource["need_unlock"] = False

                if not share_url:
                    continue

                share_ref = self._safe_share_ref(share_url)

                logger.info(
                    f"检查分享：{resource_title} - {share_ref}"
                )

                try:
                    # 先检查分享链接是否有效
                    share_status = self._p115_manager.check_share_status(share_url)
                    if not share_status.is_valid:
                        logger.warning(
                            f"分享链接无效：{share_ref}，"
                            f"原因：{share_status.status_text}"
                        )
                        continue

                    share_files = self._p115_manager.list_share_files(share_url)
                    if not share_files:
                        logger.info(
                            f"分享链接无内容：{share_ref}"
                        )
                        continue

                    # 匹配电影文件
                    matched_file = FileMatcher.match_movie_file(
                        share_files,
                        mediainfo.title,
                        year=mediainfo.year,
                        subscribe_filter=subscribe_filter,
                    )

                    if matched_file:
                        if resource_source == "ayclub":
                            ayclub_usable_candidate = True
                        file_name = matched_file.get('name', '')
                        logger.info(f"找到匹配文件：{file_name}")

                        # 计算当前文件的过滤分数和是否完美匹配
                        _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                        is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                        # 洗版模式下检查是否需要升级资源
                        if is_best_version and movie_history_score >= 0:
                            if current_score <= movie_history_score:
                                logger.info(f"电影 {mediainfo.title} 已有分数 {movie_history_score}，当前 {current_score}，跳过")
                                continue
                            else:
                                logger.info(f"电影 {mediainfo.title} 洗版：旧分数 {movie_history_score} -> 新分数 {current_score}")

                        # 调用 OpenClaw 七分类服务确定目标根目录
                        target_root = self._resolve_target_root(
                            share_url=share_url,
                            media_type="movie",
                            title=mediainfo.title,
                            fallback_root=self._movie_save_path,
                            year=mediainfo.year,
                            tmdb_id=mediainfo.tmdb_id,
                            resource_title=resource_title,
                            file_names=[file_name],
                        )
                        if not target_root:
                            continue

                        # 分类根目录下继续保留 MoviePilot 标准标题 + 年份目录
                        movie_folder = (
                            f"{mediainfo.title} ({mediainfo.year})"
                            if mediainfo.year
                            else mediainfo.title
                        )
                        save_dir = f"{target_root.rstrip('/')}/{movie_folder}"
                        logger.info(f"转存目标路径: {save_dir}")

                        # 执行转存
                        success = self._p115_manager.transfer_file(
                            share_url=share_url,
                            file_id=matched_file.get("id"),
                            save_path=save_dir
                        )

                        # 记录历史
                        history_item = {
                            "title": mediainfo.title,
                            "year": mediainfo.year,
                            "type": "电影",
                            "status": "成功" if success else "失败",
                            "share_ref": share_ref,
                            "file_name": file_name,
                            "filter_score": current_score,
                            "perfect_match": is_perfect,
                            "subscribe_id": int(subscribe.id),
                            "generation": self._lifecycle.generation(int(subscribe.id)),
                            "media_key": media_key,
                            "stage": "pending_organize" if success else "transfer_failed",
                            "search_source": resource.get("search_source") or resource.get("source"),
                            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        history.append(history_item)

                        if success:
                            transferred_count += 1
                            movie_transferred = True
                            movie_history_score = current_score
                            score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                            logger.info(f"成功转存电影：{mediainfo.title} {score_info}")

                            # 收集转存详情用于通知
                            transfer_details.append({
                                "type": "电影",
                                "title": mediainfo.title,
                                "year": mediainfo.year,
                                "image": mediainfo.get_poster_image(),
                                "file_name": file_name
                            })

                            # 转存成功只表示已经投递到 MP 整理入口。
                            # 不写 MoviePilot 下载事实，不直接修改订阅进度或强制完成。
                            self._lifecycle.add_pending(
                                subscribe=subscribe,
                                media_key=media_key,
                                episodes=None,
                                file_items=[{
                                    "id": matched_file.get("id"),
                                    "name": file_name,
                                }],
                                share_ref=share_ref,
                                target_path=save_dir,
                                source=str(resource.get("search_source") or resource.get("source") or ""),
                            )
                            logger.info(
                                f"电影 {mediainfo.title_year} 已投递，等待 MoviePilot TransferComplete 入库通知"
                            )

                            # 实际转存成功，立即结束资源迭代，
                            # 避免生成器继续查询后续搜索源
                            break
                        else:
                            logger.error(f"转存失败：{mediainfo.title}")

                except Exception as e:
                    logger.error(
                        f"处理分享链接出错：{share_ref}，"
                        f"错误类型：{type(e).__name__}"
                    )
                    continue
                    
            ayclub_query_status = self._search_handler.get_ayclub_last_status()
            ayclub_cached = self._search_handler.get_ayclub_last_cached()
            force_honored = self._search_handler.was_ayclub_force_refresh_honored()
            ayclub_late_reply = self._search_handler.was_ayclub_late_reply()
            ayclub_result_request_id = (
                self._search_handler.get_ayclub_last_origin_request_id()
                or self._search_handler.get_ayclub_last_request_id()
                or ayclub_request_id
            )

            result_recorded = False
            if mediainfo.tmdb_id and movie_gate.get("allow_ayclub"):
                result_recorded = self._release_gate.mark_movie_search_result(
                    tmdb_id=int(mediainfo.tmdb_id),
                    search_status=ayclub_query_status,
                    cached=ayclub_cached,
                    force_honored=force_honored,
                    usable_candidate=(
                        ayclub_usable_candidate
                        if ayclub_query_status == "ok_matched"
                        else None
                    ),
                    attempt_reserved=ayclub_attempt_reserved,
                    late_reply=ayclub_late_reply,
                    request_id=ayclub_result_request_id,
                )

            if lifecycle_force_refresh:
                lifecycle_terminal = (
                    ayclub_query_status in {
                        "ok_empty",
                        "ok_matched",
                        "cached_empty",
                    }
                    and (
                        force_honored
                        or ayclub_cached is False
                        or ayclub_late_reply
                    )
                )
                if lifecycle_terminal:
                    self._lifecycle.clear_force_refresh(int(subscribe.id))
                    logger.info(
                        f"订阅 {subscribe.id} 的 AYCLUB 生命周期强刷已取得终态，"
                        f"status={ayclub_query_status}，迟到回复={ayclub_late_reply}，"
                        f"request_id={ayclub_result_request_id}，清除一次性标记"
                    )
                elif ayclub_query_status not in {"idle", "disabled"}:
                    logger.warning(
                        f"订阅 {subscribe.id} 已请求 AYCLUB 生命周期强刷，"
                        f"但尚未取得终态；force_refresh_pending=True，"
                        f"status={ayclub_query_status}，"
                        f"request_id={ayclub_result_request_id}，"
                        "保留强刷标记并按 retry_after/每日额度限制后续自动任务"
                    )

            if result_recorded and mediainfo.tmdb_id:
                result_state = self._release_gate.get_movie(
                    int(mediainfo.tmdb_id)
                )
                logger.info(
                    f"电影 {mediainfo.title} AYCLUB 状态："
                    f"force_refresh_pending={result_state.get('force_refresh_pending')}，"
                    f"last_real_search_at={result_state.get('last_real_search_at')}，"
                    f"retry_after={result_state.get('retry_after')}，"
                    f"daily_search_count={result_state.get('daily_search_count')}，"
                    f"no_result_cooldown_until={result_state.get('no_result_cooldown_until')}，"
                    f"跳过原因={result_state.get('last_skip_reason')}"
                )

            if not resource_found:
                logger.info(
                    f"未找到电影 {mediainfo.title} 的任何 115 网盘候选资源"
                )
            elif not movie_transferred:
                logger.info(
                    f"电影 {mediainfo.title} 的候选资源均无效、"
                    f"不匹配过滤条件或转存失败"
                )
        except Exception as e:
            logger.error(f"处理电影订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count


    def _load_completed_ed2k_wash_state(self) -> Dict[str, Dict[str, Any]]:
        if not self._get_data:
            return {}
        try:
            raw = self._get_data(self.COMPLETED_ED2K_WASH_DATA_KEY) or {}
        except Exception as error:
            logger.warning(f"读取已完成整季 ED2K 洗版状态失败：{error}")
            return {}
        if not isinstance(raw, dict):
            return {}

        now = datetime.datetime.now(datetime.timezone.utc)
        retained: Dict[str, Dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            updated_at = self._parse_utc_datetime(value.get("updated_at"))
            if not updated_at:
                continue
            age_days = (now - updated_at).total_seconds() / 86400
            if age_days <= self.COMPLETED_ED2K_WASH_RETENTION_DAYS:
                retained[str(key)] = value
        if len(retained) != len(raw):
            self._save_completed_ed2k_wash_state(retained)
        return retained

    def _save_completed_ed2k_wash_state(
        self,
        state: Dict[str, Dict[str, Any]],
    ) -> None:
        if not self._save_data:
            return
        try:
            self._save_data(self.COMPLETED_ED2K_WASH_DATA_KEY, state)
        except Exception as error:
            logger.warning(f"保存已完成整季 ED2K 洗版状态失败：{error}")

    @staticmethod
    def _completed_ed2k_wash_key(tmdb_id: int, season: int) -> str:
        return f"tv:{int(tmdb_id)}:S{int(season)}"

    def _completed_ed2k_wash_due(
        self,
        *,
        tmdb_id: int,
        season: int,
    ) -> bool:
        key = self._completed_ed2k_wash_key(tmdb_id, season)
        record = self._load_completed_ed2k_wash_state().get(key) or {}
        next_check = self._parse_utc_datetime(record.get("next_check_at"))
        return not next_check or datetime.datetime.now(
            datetime.timezone.utc
        ) >= next_check

    @staticmethod
    def _positive_int_list(values: Optional[List[int]]) -> List[int]:
        result: Set[int] = set()
        for value in values or []:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                result.add(number)
        return sorted(result)

    def _record_completed_ed2k_wash(
        self,
        *,
        tmdb_id: int,
        season: int,
        status: str,
        retry_days: int,
        fingerprint: Optional[str] = None,
        signature_ref: Optional[str] = None,
        score: Optional[int] = None,
        expected_episodes: Optional[List[int]] = None,
        accepted_episodes: Optional[List[int]] = None,
        failed_episodes: Optional[List[int]] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        state = self._load_completed_ed2k_wash_state()
        key = self._completed_ed2k_wash_key(tmdb_id, season)
        old = state.get(key) or {}
        now = datetime.datetime.now(datetime.timezone.utc)
        record = {
            **old,
            "tmdb_id": int(tmdb_id),
            "season": int(season),
            "status": str(status),
            "fingerprint": (
                old.get("fingerprint")
                if fingerprint is None
                else fingerprint
            ),
            "signature_ref": (
                old.get("signature_ref")
                if signature_ref is None
                else signature_ref
            ),
            "score": (
                int(old.get("score") or 0)
                if score is None
                else int(score or 0)
            ),
            "expected_episodes": (
                self._positive_int_list(old.get("expected_episodes"))
                if expected_episodes is None
                else self._positive_int_list(expected_episodes)
            ),
            "accepted_episodes": (
                self._positive_int_list(old.get("accepted_episodes"))
                if accepted_episodes is None
                else self._positive_int_list(accepted_episodes)
            ),
            "failed_episodes": (
                self._positive_int_list(old.get("failed_episodes"))
                if failed_episodes is None
                else self._positive_int_list(failed_episodes)
            ),
            "diagnostics": dict(diagnostics or {}),
            "last_checked_at": now.isoformat(),
            "next_check_at": (
                now + datetime.timedelta(days=max(1, int(retry_days)))
            ).isoformat(),
            "updated_at": now.isoformat(),
        }
        if status == "submitted":
            record["submitted_at"] = now.isoformat()
        state[key] = record
        self._save_completed_ed2k_wash_state(state)
        return record

    def process_completed_tv_ed2k_wash(
        self,
        *,
        subscribe: Any,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        scheduled_evening_refresh: bool = False,
    ) -> int:
        # 只对正式完成且开启洗版的电视剧检查完整统一 ED2K 整季批次。
        if not scheduled_evening_refresh:
            return transferred_count
        if not bool(getattr(subscribe, "best_version", False)):
            return transferred_count
        if getattr(subscribe, "type", None) != MediaType.TV.value:
            return transferred_count

        try:
            subscribe_id = int(subscribe.id)
            tmdb_id = int(subscribe.tmdbid)
            season = int(subscribe.season or 1)
            total_episode = int(subscribe.total_episode or 0)
            start_episode = max(1, int(subscribe.start_episode or 1))
        except (TypeError, ValueError, AttributeError):
            logger.warning("已完成整季 ED2K 洗版订阅缺少有效 ID/TMDB/季集信息，跳过")
            return transferred_count

        if total_episode < start_episode:
            return transferred_count
        expected = list(range(start_episode, total_episode + 1))
        if not self._completed_ed2k_wash_due(
            tmdb_id=tmdb_id,
            season=season,
        ):
            logger.info(
                f"已完成整季 ED2K 洗版尚未到复查时间："
                f"TMDB={tmdb_id}, S{season}"
            )
            return transferred_count

        state_key = self._completed_ed2k_wash_key(tmdb_id, season)
        old = self._load_completed_ed2k_wash_state().get(state_key) or {}
        old_status = str(old.get("status") or "")

        meta = MetaInfo(subscribe.name)
        meta.year = subscribe.year
        meta.begin_season = season
        meta.type = MediaType.TV
        mediainfo: MediaInfo = self._chain.recognize_media(
            meta=meta,
            mtype=MediaType.TV,
            tmdbid=tmdb_id,
            doubanid=subscribe.doubanid,
            cache=True,
        )
        if not mediainfo:
            self._record_completed_ed2k_wash(
                tmdb_id=tmdb_id,
                season=season,
                status=(
                    "submitted"
                    if old_status == "submitted"
                    else "recognize_failed"
                ),
                retry_days=self.COMPLETED_ED2K_WASH_NO_PACK_RETRY_DAYS,
                expected_episodes=expected,
                diagnostics={"last_check_result": "recognize_failed"},
            )
            return transferred_count

        self._search_handler.reset_ayclub_status()
        request_id = uuid.uuid4().hex
        logger.info(
            f"开始检查已完成电视剧完整统一 ED2K 洗版候选："
            f"{mediainfo.title_year} S{season}，"
            f"期望=E{expected[0]:02d}-E{expected[-1]:02d}"
        )
        resources = self._search_handler.search_single_source(
            source="ayclub",
            mediainfo=mediainfo,
            media_type=MediaType.TV,
            season=season,
            episodes=None,
            force_refresh=True,
            cache_only=False,
            request_id=request_id,
        )
        self._record_ayclub_query_if_real(
            tmdb_id=tmdb_id,
            season=season,
            reason="completed_season_ed2k_wash",
        )

        ed2k_resources = [
            item for item in resources or []
            if isinstance(item, dict) and self._is_ed2k_resource(item)
        ]
        subscribe_filter = SubscribeFilter(
            quality=subscribe.quality,
            resolution=subscribe.resolution,
            effect=subscribe.effect,
            strict=False,
        )
        eligible_resources: List[Dict[str, Any]] = []
        scores: Dict[str, int] = {}
        for resource in ed2k_resources:
            resource_title = str(resource.get("title") or "")
            matched = True
            score = 0
            if subscribe_filter.has_filters():
                try:
                    matched, score = subscribe_filter.match(resource_title)
                except Exception:
                    matched, score = False, 0
            if not matched:
                continue
            source_ref = self._ed2k_source_ref(str(resource.get("url") or ""))
            scores[source_ref] = int(score or 0)
            eligible_resources.append(resource)

        pack, diagnostics = select_complete_uniform_ed2k_pack(
            eligible_resources,
            season=season,
            expected_episodes=expected,
            score_func=lambda item: scores.get(
                self._ed2k_source_ref(str(item.get("url") or "")),
                0,
            ),
        )
        if not pack:
            self._record_completed_ed2k_wash(
                tmdb_id=tmdb_id,
                season=season,
                status=(
                    "submitted"
                    if old_status == "submitted"
                    else "no_complete_uniform_pack"
                ),
                retry_days=self.COMPLETED_ED2K_WASH_NO_PACK_RETRY_DAYS,
                expected_episodes=expected,
                diagnostics={
                    "last_check_result": "no_complete_uniform_pack",
                    "input_count": diagnostics.get("input_count"),
                    "valid_count": diagnostics.get("valid_count"),
                    "group_count": diagnostics.get("group_count"),
                    "complete_group_count": diagnostics.get(
                        "complete_group_count"
                    ),
                },
            )
            logger.info(
                f"未发现完整统一 ED2K 整季洗版包："
                f"{mediainfo.title_year} S{season}，"
                f"有效ED2K={diagnostics.get('valid_count', 0)}，"
                f"命名组={diagnostics.get('group_count', 0)}"
            )
            return transferred_count

        same_fingerprint = old.get("fingerprint") == pack.fingerprint
        try:
            old_score = int(old.get("score") or 0)
        except (TypeError, ValueError):
            old_score = 0

        if same_fingerprint and old_status == "submitted":
            self._record_completed_ed2k_wash(
                tmdb_id=tmdb_id,
                season=season,
                status="submitted",
                retry_days=self.COMPLETED_ED2K_WASH_RECHECK_DAYS,
                fingerprint=pack.fingerprint,
                signature_ref=pack.signature_ref,
                score=pack.score,
                expected_episodes=expected,
                accepted_episodes=expected,
            )
            logger.info(
                f"完整整季 ED2K 洗版包已提交过，跳过重复批次："
                f"{mediainfo.title_year} S{season}，pack={pack.signature_ref}"
            )
            return transferred_count

        if (
            old.get("fingerprint")
            and not same_fingerprint
            and old_status == "submitted"
            and pack.score <= old_score
        ):
            self._record_completed_ed2k_wash(
                tmdb_id=tmdb_id,
                season=season,
                status="submitted",
                retry_days=self.COMPLETED_ED2K_WASH_RECHECK_DAYS,
                fingerprint=old.get("fingerprint"),
                signature_ref=old.get("signature_ref"),
                score=old_score,
                expected_episodes=expected,
                accepted_episodes=old.get("accepted_episodes") or expected,
                diagnostics={
                    "last_check_result": "lower_or_equal_quality",
                    "candidate_signature_ref": pack.signature_ref,
                },
            )
            logger.info(
                f"新完整整季 ED2K 候选分数未高于已提交版本，跳过："
                f"{mediainfo.title_year} S{season}，"
                f"当前={old_score}，候选={pack.score}"
            )
            return transferred_count

        accepted_before: Set[int] = set()
        if same_fingerprint:
            accepted_before = {
                int(value)
                for value in (old.get("accepted_episodes") or [])
            }
        to_submit = [
            episode for episode in expected if episode not in accepted_before
        ]
        remaining_capacity = self._max_transfer_per_sync - transferred_count
        if len(to_submit) > remaining_capacity:
            self._record_completed_ed2k_wash(
                tmdb_id=tmdb_id,
                season=season,
                status="capacity_wait",
                retry_days=self.COMPLETED_ED2K_WASH_PARTIAL_RETRY_DAYS,
                fingerprint=pack.fingerprint,
                signature_ref=pack.signature_ref,
                score=pack.score,
                expected_episodes=expected,
                accepted_episodes=sorted(accepted_before),
                failed_episodes=to_submit,
                diagnostics={"remaining_capacity": remaining_capacity},
            )
            logger.warning(
                f"完整整季 ED2K 洗版要求原子预留容量，当前不足："
                f"{mediainfo.title_year} S{season}，"
                f"待提交={len(to_submit)}，剩余额度={remaining_capacity}"
            )
            return transferred_count

        media_key = self._lifecycle.media_key_from_subscribe(subscribe)
        accepted = set(accepted_before)
        newly_queued: List[int] = []
        failed: List[int] = []
        for episode in to_submit:
            resource = pack.episode_resources[episode]
            ok, duplicate = self._dispatch_ed2k_resource(
                subscribe=subscribe,
                media_key=media_key,
                resource=resource,
                mediainfo=mediainfo,
                media_type="tv",
                season=season,
                episodes=[episode],
            )
            if ok:
                accepted.add(episode)
                if not duplicate:
                    newly_queued.append(episode)
                    history.append({
                        "title": mediainfo.title,
                        "year": mediainfo.year,
                        "type": "电视剧",
                        "status": "成功",
                        "episode": episode,
                        "share_ref": (
                            "ed2k:"
                            + self._ed2k_source_ref(
                                str(resource.get("url") or "")
                            )
                        ),
                        "file_name": str(resource.get("title") or "")[:500],
                        "filter_score": pack.score,
                        "perfect_match": True,
                        "subscribe_id": subscribe_id,
                        "generation": self._lifecycle.generation(subscribe_id),
                        "media_key": media_key,
                        "stage": "pending_transfer",
                        "search_source": (
                            "ayclub_ed2k_completed_season_wash"
                        ),
                        "time": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    })
            else:
                failed.append(episode)

        complete = set(expected).issubset(accepted)
        status = "submitted" if complete else "partial"
        retry_days = (
            self.COMPLETED_ED2K_WASH_RECHECK_DAYS
            if complete
            else self.COMPLETED_ED2K_WASH_PARTIAL_RETRY_DAYS
        )
        self._record_completed_ed2k_wash(
            tmdb_id=tmdb_id,
            season=season,
            status=status,
            retry_days=retry_days,
            fingerprint=pack.fingerprint,
            signature_ref=pack.signature_ref,
            score=pack.score,
            expected_episodes=expected,
            accepted_episodes=sorted(accepted),
            failed_episodes=failed,
            diagnostics={
                "newly_queued": len(newly_queued),
                "complete_group_count": diagnostics.get(
                    "complete_group_count", 0
                ),
            },
        )
        if newly_queued:
            transferred_count += len(newly_queued)
            transfer_details.append({
                "type": "电视剧",
                "title": (
                    f"{mediainfo.title} S{season} 完整整季ED2K洗版"
                ),
                "year": mediainfo.year,
                "image": mediainfo.get_poster_image(),
                "file_name": (
                    f"E{expected[0]:02d}-E{expected[-1]:02d}，"
                    f"新提交 {len(newly_queued)}/{len(expected)}"
                ),
            })

        logger.info(
            f"完整统一 ED2K 整季洗版批次处理完成："
            f"{mediainfo.title_year} S{season}，"
            f"pack={pack.signature_ref}，分数={pack.score}，"
            f"已接受={len(accepted)}/{len(expected)}，"
            f"本次新提交={len(newly_queued)}，失败={failed}；"
            "现有媒体不会由本插件删除"
        )
        return transferred_count

    def process_tv_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        exclude_ids: Set[int],
        scheduled_evening_refresh: bool = False,
    ) -> int:
        """
        处理单个电视剧订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :param exclude_ids: 排除的订阅ID集合
        :return: 更新后的转存数量
        """
        try:
            logger.info(f"订阅信息：{subscribe.name}，开始集数：{subscribe.start_episode}, 总集数：{subscribe.total_episode}, 缺失集数：{subscribe.lack_episode}")
            logger.info(f"处理订阅：{subscribe.name} (S{subscribe.season or 1})")

            # 加载该订阅的历史积分花费（用 tmdb_id + 季数作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_S{subscribe.season or 1}" if subscribe.tmdbid else f"{subscribe.name}_S{subscribe.season or 1}"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # 始终向 MoviePilot 查询当前状态，不能只相信缓存字段 lack_episode。
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.begin_season = subscribe.season or 1
            meta.type = MediaType.TV

            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.TV,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )
            if not mediainfo:
                logger.warning(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count

            season = int(meta.begin_season or 1)
            mediakey = mediainfo.tmdb_id or mediainfo.douban_id
            media_key = self._lifecycle.media_key_from_subscribe(subscribe)
            self._lifecycle.ensure_subscription(
                int(subscribe.id), subscribe, scene="sync"
            )
            force_refresh = self._lifecycle.peek_force_refresh(int(subscribe.id))

            mp_subscribe_chain = SubscribeChain()
            try:
                exist_flag, no_exists = mp_subscribe_chain.resolve_subscribe_missing(
                    subscribe=subscribe,
                    meta=meta,
                    mediainfo=mediainfo,
                    mediakey=mediakey,
                )
            except Exception as error:
                # 兼容旧 MP：仍然只调用 MP 自己的 DownloadChain。
                logger.warning(
                    f"MP resolve_subscribe_missing 不可用，回退 get_no_exists_info：{error}"
                )
                totals = (
                    {season: subscribe.total_episode}
                    if subscribe.total_episode
                    else {}
                )
                exist_flag, no_exists = DownloadChain().get_no_exists_info(
                    meta=meta, mediainfo=mediainfo, totals=totals
                )

            mp_reported_satisfied = bool(exist_flag)
            if mp_reported_satisfied:
                logger.info(
                    f"MoviePilot 报告 {mediainfo.title_year} S{season} 已满足订阅，"
                    "将先用本地 STRM 再确认"
                )

            missing_episodes = self._extract_missing_episodes(
                no_exists=no_exists,
                mediakey=mediakey,
                season=season,
            )
            if subscribe.start_episode:
                missing_episodes = [
                    episode for episode in missing_episodes
                    if episode >= int(subscribe.start_episode)
                ]

            mp_missing_episodes = list(missing_episodes)

            # 本地 STRM 是本环境的可播放事实源。MoviePilot 的下载历史和
            # Emby 扫描可能滞后，因此在搜索前用当前 STRM 文件重建缺集。
            strm_status, strm_episodes, strm_dirs = self._scan_local_strm_episodes(
                mediainfo=mediainfo,
                season=season,
            )
            try:
                total_episode = int(subscribe.total_episode or 0)
            except (TypeError, ValueError):
                total_episode = 0
            start_episode = max(1, int(subscribe.start_episode or 1))
            if total_episode > 0:
                expected_episodes = set(range(start_episode, total_episode + 1))
                if strm_status == "ok":
                    local_missing = sorted(expected_episodes - strm_episodes)
                    if set(local_missing) != set(mp_missing_episodes):
                        logger.warning(
                            f"STRM 实时对账修正 {mediainfo.title_year} S{season} 缺集："
                            f"MP={sorted(mp_missing_episodes)}，"
                            f"STRM={local_missing}，已存在STRM={sorted(strm_episodes)}"
                        )
                    missing_episodes = local_missing
                    mp_missing_episodes = list(local_missing)
                    logger.info(
                        f"{mediainfo.title_year} S{season} STRM 对账目录："
                        f"{strm_dirs[:3]}"
                    )
                elif strm_status == "unavailable":
                    logger.error(
                        f"{mediainfo.title_year} S{season} 无法访问本地 STRM 根目录，"
                        "为避免使用陈旧订阅历史，本轮停止该订阅"
                    )
                    return transferred_count
                else:
                    # 新订阅在还没有任何已存在集时可以没有 show 目录；若 MP
                    # 声称已有剧集而 STRM 目录完全找不到，则状态冲突，安全停止。
                    mp_present = expected_episodes - set(mp_missing_episodes)
                    if mp_present:
                        logger.error(
                            f"{mediainfo.title_year} S{season} MP 声称已有集数 "
                            f"{sorted(mp_present)}，但未找到对应 STRM 目录；"
                            "本轮停止该订阅"
                        )
                        return transferred_count

            # MP 事件是主路径；插件只用在途任务避免整理期间重复投递。
            pending_episodes = self._lifecycle.pending_episodes(media_key)
            if pending_episodes:
                before = set(missing_episodes)
                missing_episodes = [
                    episode for episode in missing_episodes
                    if episode not in pending_episodes
                ]
                waiting = sorted(before & pending_episodes)
                if waiting:
                    logger.info(
                        f"{mediainfo.title_year} S{season} {waiting} 已投递并等待 MP 入库，暂不重复搜索"
                    )

            # 事件可能漏失；若 MP 已不再缺某集，补记对应在途任务完成。
            self._lifecycle.reconcile_missing(
                media_key, missing_episodes=mp_missing_episodes
            )

            if not missing_episodes:
                logger.info(
                    f"{mediainfo.title_year} S{season} 当前没有需要新投递的剧集"
                )
                if mp_reported_satisfied:
                    self._lifecycle.reconcile_missing(
                        media_key, media_satisfied=True
                    )
                    try:
                        mp_subscribe_chain.check_and_handle_existing_media(
                            subscribe=subscribe,
                            meta=meta,
                            mediainfo=mediainfo,
                            mediakey=mediakey,
                        )
                    except Exception as error:
                        logger.warning(
                            f"交由 MP 完成订阅失败，将由后续对账恢复：{error}"
                        )
                    if hasattr(self._search_handler, "clear_sub_points"):
                        self._search_handler.clear_sub_points(sub_key)
                return transferred_count

            is_best_version = bool(subscribe.best_version)
            episode_history_scores: Dict[int, int] = {}
            if is_best_version:
                for item in self._current_cycle_history(history, subscribe, media_key):
                    if item.get("type") != "电视剧" or item.get("status") != "成功":
                        continue
                    try:
                        episode = int(item.get("episode"))
                        score = int(item.get("filter_score") or 0)
                    except (TypeError, ValueError):
                        continue
                    episode_history_scores[episode] = max(
                        score, episode_history_scores.get(episode, -1)
                    )

            # 七分类目录是 MP 入库投递区，不是最终媒体库；不再扫描它判断长期存在。
            show_folder = (
                f"{mediainfo.title} ({mediainfo.year})"
                if mediainfo.year else mediainfo.title
            )
            save_dir = f"{self._save_path}/{show_folder}/Season {season}"
            existing_episodes_in_cloud: Set[int] = set()

            # 根据 TMDB 剧集播出日期决定不同来源可查询的集数。
            # AYCLUB 受发布门禁控制；其他来源仍可查询已播出或日期未知的缺失集。
            all_missing_episodes = list(missing_episodes)
            episode_air_dates: Dict[int, Optional[str]] = {}
            metadata_ok = True

            tv_gate = {
                "allow_ayclub": False,
                "ayclub_first": False,
                "probe_due": False,
                "released": False,
                "reason": "missing_tmdb_id",
                "aired_episodes": [],
                "future_episodes": [],
                "unknown_episodes": [],
                "aired_episode_frontier": None,
                "ayclub_episodes": [],
            }

            if mediainfo.tmdb_id:
                try:
                    from app.chain.tmdb import TmdbChain

                    tmdb_episodes = TmdbChain().tmdb_episodes(
                        tmdbid=mediainfo.tmdb_id,
                        season=season,
                    )

                    for episode_info in tmdb_episodes or []:
                        episode_number = getattr(
                            episode_info,
                            "episode_number",
                            None,
                        )

                        if not episode_number:
                            continue

                        episode_air_dates[int(episode_number)] = (
                            getattr(
                                episode_info,
                                "air_date",
                                None,
                            )
                        )

                    if not tmdb_episodes:
                        logger.info(
                            f"{mediainfo.title_year} S{season} "
                            f"TMDB未返回剧集信息，按播出日期未知处理"
                        )

                except Exception as error:
                    metadata_ok = False
                    logger.warning(
                        f"{mediainfo.title_year} S{season} "
                        f"查询TMDB剧集播出日期失败：{error}"
                    )

                tv_gate = self._release_gate.evaluate_tv(
                    tmdb_id=int(mediainfo.tmdb_id),
                    season=int(season),
                    missing_episodes=all_missing_episodes,
                    episode_air_dates=episode_air_dates,
                    metadata_ok=metadata_ok,
                )
            else:
                logger.info(
                    f"{mediainfo.title_year} S{season} 缺少 TMDB ID，"
                    f"本次不查询 AYCLUB"
                )

            # 普通来源只查询已经播出到的集数。
            # TMDB经常只为已播集填写air_date，
            # 使用整季已播前沿避免搜索日期未知的未来集。
            standard_search_episodes = list(
                all_missing_episodes
            )

            if mediainfo.tmdb_id and metadata_ok:
                future_episode_set = set(
                    tv_gate.get("future_episodes") or []
                )

                try:
                    aired_episode_frontier = int(
                        tv_gate.get(
                            "aired_episode_frontier"
                        ) or 0
                    )
                except (TypeError, ValueError):
                    aired_episode_frontier = 0

                if aired_episode_frontier > 0:
                    standard_search_episodes = [
                        episode
                        for episode in all_missing_episodes
                        if (
                            episode
                            <= aired_episode_frontier
                            and episode
                            not in future_episode_set
                        )
                    ]
                else:
                    standard_search_episodes = [
                        episode
                        for episode in all_missing_episodes
                        if episode
                        not in future_episode_set
                    ]

            ayclub_search_episodes = [
                episode
                for episode in all_missing_episodes
                if episode in set(
                    tv_gate.get("ayclub_episodes") or []
                )
            ]

            # 防止读取上一个订阅遗留的 AYCLUB 查询状态。
            self._search_handler.reset_ayclub_status()

            logger.info(
                f"{mediainfo.title_year} S{season} AYCLUB 发布门禁："
                f"允许={tv_gate.get('allow_ayclub')}，"
                f"优先={tv_gate.get('ayclub_first')}，"
                f"原因={tv_gate.get('reason')}，"
                f"查询集数={ayclub_search_episodes}"
            )

            logger.info(
                f"{mediainfo.title_year} S{season} "
                f"实际缺失剧集：{all_missing_episodes}；"
                f"普通来源可查询：{standard_search_episodes}"
            )
            # 创建订阅过滤条件
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"{mediainfo.title} S{season} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 成功转存的集数列表
            success_episodes = []
            ed2k_dispatched_episodes: Set[int] = set()

            # 同一部剧同一季度只需成功分类一次；
            # 后续分享复用分类目录。
            classified_target_root: Optional[str] = None
            classified_save_dir: Optional[str] = None

            # 智能回退搜索：按源迭代
            enabled_sources = self._search_handler.get_enabled_sources(
                ayclub_first=bool(
                    tv_gate.get("ayclub_first")
                ),
                allow_ayclub=bool(
                    tv_gate.get("allow_ayclub")
                ),
            )

            if not enabled_sources:
                logger.warning(
                    f"没有可用的搜索源，跳过 "
                    f"{mediainfo.title} S{season} 的搜索"
                )
                return transferred_count

            standard_episode_set = set(
                standard_search_episodes
            )
            ayclub_episode_set = set(
                ayclub_search_episodes
            )

            for source_index, source in enumerate(enabled_sources):
                if not missing_episodes:
                    logger.info(
                        f"{mediainfo.title_year} S{season} "
                        f"所有缺失剧集已转存完成，不再查询后续源"
                    )
                    break

                if transferred_count >= self._max_transfer_per_sync:
                    logger.info(
                        f"已达单次同步上限 "
                        f"{self._max_transfer_per_sync}，"
                        f"剩余 {len(missing_episodes)} 集将在下次同步处理"
                    )
                    break

                source_episode_set = (
                    ayclub_episode_set
                    if source == "ayclub"
                    else standard_episode_set
                )

                source_episodes = [
                    episode
                    for episode in missing_episodes
                    if episode in source_episode_set
                ]

                if source != "ayclub" and ed2k_dispatched_episodes:
                    source_episodes = [
                        episode
                        for episode in source_episodes
                        if episode not in ed2k_dispatched_episodes
                    ]

                if not source_episodes:
                    logger.info(
                        f"[{source.upper()}] 当前没有符合播出门禁的"
                        f"缺失剧集，跳过该来源"
                    )
                    continue

                logger.info(
                    f"[{source.upper()}] 开始搜索 "
                    f"{mediainfo.title} S{season}，"
                    f"目标集数：{source_episodes}"
                )

                ayclub_force_refresh = False
                ayclub_cache_only = False
                ayclub_query_reason = "not_ayclub"

                if source == "ayclub":
                    (
                        ayclub_force_refresh,
                        ayclub_cache_only,
                        ayclub_query_reason,
                    ) = self._ayclub_tv_query_mode(
                        tmdb_id=getattr(mediainfo, "tmdb_id", None),
                        season=int(season),
                        lifecycle_force_refresh=force_refresh,
                        scheduled_evening_refresh=scheduled_evening_refresh,
                    )
                    if ayclub_force_refresh:
                        logger.info(
                            f"{mediainfo.title_year} S{season} AYCLUB 查询模式："
                            f"真实搜索，原因={ayclub_query_reason}"
                        )
                    else:
                        logger.info(
                            f"{mediainfo.title_year} S{season} AYCLUB 查询模式："
                            f"仅缓存，原因={ayclub_query_reason}"
                        )

                # 暂不把 episodes 传给桥接，以保留整季包搜索结果；
                # 后续只匹配和转存 source_episodes 中的缺失集。
                p115_results = self._search_handler.search_single_source(
                    source=source,
                    mediainfo=mediainfo,
                    media_type=MediaType.TV,
                    season=season,
                    force_refresh=ayclub_force_refresh,
                    cache_only=ayclub_cache_only,
                )

                if source == "ayclub":
                    real_query = self._record_ayclub_query_if_real(
                        tmdb_id=getattr(mediainfo, "tmdb_id", None),
                        season=int(season),
                        reason=ayclub_query_reason,
                    )
                    if real_query:
                        logger.info(
                            f"{mediainfo.title_year} S{season} AYCLUB 今日真实搜索已记录"
                        )

                if source == "ayclub" and force_refresh:
                    ayclub_query_status = self._search_handler.get_ayclub_last_status()
                    force_honored = self._search_handler.was_ayclub_force_refresh_honored()
                    if force_honored:
                        self._lifecycle.clear_force_refresh(int(subscribe.id))
                        logger.info(
                            f"订阅 {subscribe.id} 的 AYCLUB 强制刷新已确认绕过缓存，清除一次性标记"
                        )
                    elif ayclub_query_status not in {"idle", "disabled"}:
                        logger.warning(
                            f"订阅 {subscribe.id} 已请求 AYCLUB 强制刷新，但桥接未确认绕过缓存；"
                            "保留强刷标记，待桥接升级或后续真实刷新"
                        )

                if (
                    source == "ayclub"
                    and tv_gate.get("probe_due")
                    and mediainfo.tmdb_id
                ):
                    ayclub_status = (
                        self._search_handler.get_ayclub_last_status()
                    )

                    self._release_gate.mark_tv_probe_result(
                        tmdb_id=int(mediainfo.tmdb_id),
                        season=int(season),
                        search_status=ayclub_status,
                    )

                    logger.info(
                        f"{mediainfo.title_year} S{season} "
                        f"AYCLUB 泄漏探测状态：{ayclub_status}"
                    )

                ed2k_dispatched_this_source: Set[int] = set()
                if source == "ayclub":
                    p115_results = self._prefilter_ayclub_results(
                        resources=p115_results,
                        missing_episodes=source_episodes,
                        season=season,
                    )

                    # 保留 1.8.7：AYCLUB 标题明确观察到的集数，
                    # 即使 115 分享失效，也允许普通来源继续兜底。
                    observed_episode_set: Set[int] = set()
                    source_episode_targets = set(source_episodes)
                    for observed_resource in p115_results or []:
                        observed_episode_set.update(
                            self._resource_episode_set(observed_resource)
                            & source_episode_targets
                        )

                    promoted_episodes = sorted(
                        observed_episode_set - standard_episode_set
                    )
                    if promoted_episodes:
                        standard_episode_set.update(promoted_episodes)
                        logger.info(
                            f"AYCLUB 已观察到发布集数 {promoted_episodes}；"
                            "即使分享无效，也允许后续普通来源兜底"
                        )

                    ayclub_ed2k_results = [
                        resource
                        for resource in p115_results
                        if self._is_ed2k_resource(resource)
                    ]
                    p115_results = [
                        resource
                        for resource in p115_results
                        if not self._is_ed2k_resource(resource)
                    ]

                    for resource in ayclub_ed2k_results:
                        candidate_episodes, reject_reason = (
                            self._select_tv_ed2k_candidate_episodes(
                                resource=resource,
                                source_episodes=source_episodes,
                                season=int(season),
                            )
                        )
                        if not candidate_episodes:
                            logger.warning(
                                f"跳过电视剧 ED2K：ref={self._ed2k_source_ref(str(resource.get('url') or ''))}，"
                                f"原因={reject_reason}；必须明确匹配当前季缺集"
                            )
                            continue

                        accepted, _ = self._dispatch_ed2k_resource(
                            subscribe=subscribe,
                            media_key=media_key,
                            resource=resource,
                            mediainfo=mediainfo,
                            media_type="tv",
                            season=int(season),
                            episodes=candidate_episodes,
                        )
                        if accepted:
                            ed2k_dispatched_this_source.update(candidate_episodes)
                            ed2k_dispatched_episodes.update(candidate_episodes)

                    if ed2k_dispatched_this_source:
                        logger.info(
                            f"AYCLUB ED2K 已独立提交，覆盖集数："
                            f"{sorted(ed2k_dispatched_this_source)}；"
                            "同消息中的 115 仍按原流程继续验证"
                        )

                if not p115_results:
                    if ed2k_dispatched_this_source:
                        logger.info(
                            "AYCLUB 本次只有已提交的 ED2K，"
                            "后续来源仅处理未覆盖缺集"
                        )
                    elif source == "ayclub":
                        logger.info("AYCLUB候选均与当前缺集无交集，不再打开115分享")
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 未找到资源，将尝试下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 未找到资源，已无更多可用源")
                    continue

                if source == "ayclub":
                    try:
                        aired_frontier = int(tv_gate.get("aired_episode_frontier") or 0)
                    except (TypeError, ValueError):
                        aired_frontier = 0
                    total_episode = int(subscribe.total_episode or 0)
                    season_complete = bool(
                        total_episode > 0
                        and aired_frontier >= total_episode
                        and not (tv_gate.get("future_episodes") or [])
                    )
                    p115_results = self._sort_ayclub_results(
                        resources=p115_results,
                        missing_episodes=source_episodes,
                        season=season,
                        season_complete=season_complete,
                        subscribe_filter=subscribe_filter,
                    )
                    logger.info(
                        f"AYCLUB候选已按完整整季/覆盖集数/质量排序，"
                        f"季度完结={season_complete}"
                    )

                logger.info(f"[{source.upper()}] 找到 {len(p115_results)} 个 115 网盘资源")

                # 遍历搜索结果
                for resource in p115_results:
                    if transferred_count >= self._max_transfer_per_sync:
                        logger.info(f"已达单次同步上限 {self._max_transfer_per_sync}，剩余 {len(missing_episodes)} 集将在下次同步处理")
                        break

                    share_url = resource.get("url", "")
                    resource_title = resource.get("title", "")

                    # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                    if resource.get("need_unlock") and not share_url:
                        slug = resource.get("slug")
                        if slug:
                            logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                            unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                            if not unlocked_url:
                                logger.error(f"未能解锁收费资源: {resource_title}")
                                continue
                            share_url = unlocked_url
                            # 更新当前字典以便存入历史或记录这个 url
                            resource["url"] = share_url
                            resource["need_unlock"] = False

                    if not share_url:
                        continue

                    share_ref = self._safe_share_ref(share_url)

                    logger.info(
                        f"检查分享：{resource_title} - {share_ref}"
                    )

                    try:
                        # 检查分享链接是否有效
                        share_status = self._p115_manager.check_share_status(share_url)
                        if not share_status.is_valid:
                            logger.warning(
                                f"分享链接无效：{share_ref}，"
                                f"原因：{share_status.status_text}"
                            )
                            continue

                        # 列出分享内容
                        share_files = self._p115_manager.list_share_files(
                            share_url,
                            target_season=(season if self._skip_other_season_dirs else None)
                        )
                        if not share_files:
                            logger.info(
                                f"分享链接无内容：{share_ref}"
                            )
                            continue

                        logger.info(f"分享包含 {len(share_files)} 个文件/目录")

                        # 收集该分享中所有匹配的文件。
                        # AYCLUB单集结果只核对对应集；
                        # 整季包及其他来源仍匹配全部当前缺失集。
                        matched_items = []
                        candidate_episodes = [
                            episode
                            for episode in source_episodes
                            if episode in missing_episodes
                        ]

                        if source == "ayclub":
                            resource_season = resource.get("season")
                            try:
                                if resource_season is not None and int(resource_season) != int(season):
                                    logger.info(
                                        f"AYCLUB候选季号不匹配：资源S{resource_season}，目标S{season}"
                                    )
                                    continue
                            except (TypeError, ValueError):
                                pass

                            resource_episodes = self._resource_episode_set(resource)
                            if resource_episodes:
                                candidate_episodes = [
                                    episode for episode in candidate_episodes
                                    if episode in resource_episodes
                                ]

                            complete_pack_episodes = self._complete_pack_episode_set(
                                resource=resource,
                                share_files=share_files,
                                season=season,
                                total_episode=int(subscribe.total_episode or 0),
                                missing_episodes=source_episodes,
                            )
                            if complete_pack_episodes:
                                candidate_episodes = sorted(complete_pack_episodes)
                                logger.info(
                                    f"AYCLUB 已验证完整单季包，允许同时补缺和覆盖洗版："
                                    f"S{season} E{candidate_episodes[0]:02d}-"
                                    f"E{candidate_episodes[-1]:02d}"
                                )

                        for episode in candidate_episodes:
                            matched_file = FileMatcher.match_episode_file(
                                share_files,
                                mediainfo.title,
                                season,
                                episode,
                                subscribe_filter=subscribe_filter
                            )

                            if matched_file:
                                file_name = matched_file.get('name', '')
                                logger.info(f"找到匹配文件：{file_name} -> E{episode:02d}")

                                _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                                is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                                is_upgrade = False
                                if is_best_version and episode in episode_history_scores:
                                    old_score = episode_history_scores[episode]
                                    if current_score <= old_score:
                                        logger.info(f"E{episode:02d} 已有分数 {old_score}，当前 {current_score}，跳过")
                                        continue
                                    else:
                                        logger.info(f"E{episode:02d} 洗版：旧分数 {old_score} -> 新分数 {current_score}")
                                        is_upgrade = True

                                matched_items.append({
                                    "file": matched_file,
                                    "episode": episode,
                                    "score": current_score,
                                    "is_perfect": is_perfect,
                                    "is_upgrade": is_upgrade
                                })

                        if not matched_items:
                            logger.info(f"该分享未匹配到 S{season} 的任何缺失剧集，可能是季数不匹配或文件名无法识别")
                            continue

                        # 同一TMDB和季度只在首次有效分享时
                        # 调用一次OpenClaw，避免单集间分类波动。
                        if classified_target_root is None:
                            target_root = (
                                self._resolve_target_root(
                                    share_url=share_url,
                                    media_type="tv",
                                    title=mediainfo.title,
                                    fallback_root=(
                                        self._save_path
                                    ),
                                    year=mediainfo.year,
                                    tmdb_id=(
                                        mediainfo.tmdb_id
                                    ),
                                    season=season,
                                    resource_title=(
                                        resource_title
                                    ),
                                    file_names=[
                                        item["file"].get(
                                            "name", ""
                                        )
                                        for item in matched_items
                                    ],
                                )
                            )
                            if not target_root:
                                continue

                            classified_target_root = (
                                target_root
                            )
                            classified_save_dir = (
                                f"{target_root.rstrip('/')}/"
                                f"{show_folder}/"
                                f"Season {season}"
                            )
                            logger.info(
                                "剧集分类后的转存目标路径: "
                                f"{classified_save_dir}"
                            )
                        else:
                            logger.debug(
                                "复用剧集分类目录："
                                f"{classified_save_dir}"
                            )

                        save_dir = (
                            classified_save_dir
                            or (
                                f"{classified_target_root.rstrip('/')}/"
                                f"{show_folder}/"
                                f"Season {season}"
                            )
                        )

                        # 七分类目录只是 MoviePilot 入库投递区。
                        # 不在这里判断长期已存在；防重复由 MP 缺失状态和 pending 任务负责。

                        # 检查转存配额限制
                        remaining_quota = self._max_transfer_per_sync - transferred_count
                        if len(matched_items) > remaining_quota:
                            logger.info(f"匹配 {len(matched_items)} 集，但受配额限制仅转存 {remaining_quota} 集")
                            matched_items = matched_items[:remaining_quota]

                        # 批量转存
                        file_ids = [item["file"]["id"] for item in matched_items]
                        logger.info(f"准备批量转存 {len(file_ids)} 个文件到: {save_dir}")

                        success_ids, failed_ids = self._p115_manager.transfer_files_batch(
                            share_url=share_url,
                            file_ids=file_ids,
                            save_path=save_dir,
                            batch_size=self._batch_size
                        )

                        success_id_set = set(success_ids)
                        batch_success_episodes = []

                        # 处理结果
                        for item in matched_items:
                            file_id = item["file"]["id"]
                            episode = item["episode"]
                            file_name = item["file"]["name"]
                            current_score = item["score"]
                            is_perfect = item["is_perfect"]
                            is_upgrade = item["is_upgrade"]
                            success = file_id in success_id_set

                            history_item = {
                                "title": mediainfo.title,
                                "season": season,
                                "episode": episode,
                                "type": "电视剧",
                                "status": "成功" if success else "失败",
                                "share_ref": share_ref,
                                "file_name": file_name,
                                "filter_score": current_score,
                                "perfect_match": is_perfect,
                                "subscribe_id": int(subscribe.id),
                                "generation": self._lifecycle.generation(int(subscribe.id)),
                                "media_key": media_key,
                                "stage": "pending_organize" if success else "transfer_failed",
                                "search_source": resource.get("search_source") or resource.get("source"),
                                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            history.append(history_item)

                            if success:
                                transferred_count += 1
                                episode_history_scores[episode] = current_score

                                if episode in missing_episodes:
                                    missing_episodes.remove(episode)

                                if not is_upgrade:
                                    success_episodes.append(episode)

                                score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                                upgrade_info = " [洗版升级]" if is_upgrade else ""
                                logger.info(f"成功转存：{mediainfo.title} S{season:02d}E{episode:02d} {score_info}{upgrade_info}")

                                # 收集转存详情
                                existing_detail = next(
                                    (d for d in transfer_details
                                     if d.get("title") == mediainfo.title and d.get("season") == season),
                                    None
                                )
                                if existing_detail:
                                    existing_detail["episodes"].append(episode)
                                else:
                                    transfer_details.append({
                                        "type": "电视剧",
                                        "title": mediainfo.title,
                                        "year": mediainfo.year,
                                        "season": season,
                                        "episodes": [episode],
                                        "image": mediainfo.get_poster_image()
                                    })

                                batch_success_episodes.append(episode)
                            else:
                                logger.error(f"转存失败：{mediainfo.title} S{season:02d}E{episode:02d}")

                        # 转存成功仅记录在途任务，等待 MP TransferComplete。
                        if batch_success_episodes:
                            success_items = [
                                item for item in matched_items
                                if item["file"]["id"] in success_id_set
                            ]
                            self._lifecycle.add_pending(
                                subscribe=subscribe,
                                media_key=media_key,
                                episodes=batch_success_episodes,
                                file_items=success_items,
                                share_ref=share_ref,
                                target_path=save_dir,
                                source=str(resource.get("search_source") or resource.get("source") or source),
                            )
                            logger.info(
                                f"{mediainfo.title_year} S{season} "
                                f"{sorted(batch_success_episodes)} 已投递，等待 MoviePilot 入库通知"
                            )

                        if not missing_episodes:
                            break

                    except Exception as e:
                        logger.error(
                            f"处理分享链接出错：{share_ref}，"
                            f"错误类型：{type(e).__name__}"
                        )
                        continue

                # 当前源处理完成
                if missing_episodes:
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，继续查询下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，已无更多可用源")

            # 不直接写 note/lack_episode，也不强制完成订阅。
            # MP 的 TransferComplete、订阅刷新与 SubscribeComplete 负责最终状态。

        except Exception as e:
            logger.error(f"处理订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count

    def send_transfer_notification(self, transfer_details: List[Dict[str, Any]], total_count: int):
        """
        发送转存完成通知

        :param transfer_details: 转存详情列表
        :param total_count: 转存总数
        """
        if not transfer_details or not self._post_message:
            return

        text_lines = []
        first_image = None

        for detail in transfer_details:
            if detail.get("type") == "电影":
                title = detail.get("title", "未知")
                year = detail.get("year", "")
                text_lines.append(f"{title} ({year})")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")
            else:
                title = detail.get("title", "未知")
                season = detail.get("season", 1)
                episodes = detail.get("episodes", [])
                episodes.sort()
                if len(episodes) <= 5:
                    ep_str = ", ".join([f"E{e:02d}" for e in episodes])
                else:
                    ep_str = f"E{episodes[0]:02d}-E{episodes[-1]:02d} 共{len(episodes)}集"
                text_lines.append(f"{title} S{season:02d} {ep_str}")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")

        if len(text_lines) > 10:
            text_lines = text_lines[:10]
            text_lines.append(f"... 等共 {len(transfer_details)} 项")

        self._post_message(
            mtype=NotificationType.Plugin,
            title=f"【115网盘订阅追更】已投递等待入库",
            text=(
                f"本次共向 MoviePilot 入库目录投递 {total_count} 个文件，"
                f"最终完成状态以 MoviePilot 入库通知为准。\n\n"
                + "\n".join(text_lines)
            )
        )
