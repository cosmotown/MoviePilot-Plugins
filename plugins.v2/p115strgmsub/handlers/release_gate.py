"""
AYCLUB 发布门禁状态存储。

负责持久化电影、电视剧季度的 TMDB 检查状态、
泄漏探测时间以及已经确认的发布信号。
"""
import datetime
from typing import Any, Callable, Dict, Optional

import pytz

from app.core.config import settings
from app.log import logger


class ReleaseGateStore:
    """发布门禁状态持久化管理器。"""

    DATA_KEY = "release_gate_state"
    SCHEMA_VERSION = 1
    RETENTION_DAYS = 90

    def __init__(
        self,
        get_data_func: Optional[Callable] = None,
        save_data_func: Optional[Callable] = None,
    ):
        self._get_data = get_data_func
        self._save_data = save_data_func

    @staticmethod
    def movie_key(tmdb_id: int) -> str:
        """生成电影状态键。"""
        return f"movie:{int(tmdb_id)}"

    @staticmethod
    def tv_key(tmdb_id: int, season: int) -> str:
        """生成电视剧季度状态键。"""
        return f"tv:{int(tmdb_id)}:S{int(season)}"

    @staticmethod
    def _timezone():
        """获取 MoviePilot 配置的时区。"""
        try:
            return pytz.timezone(settings.TZ)
        except Exception:
            return pytz.UTC

    def now(self) -> datetime.datetime:
        """返回 MoviePilot 时区的当前时间。"""
        return datetime.datetime.now(tz=self._timezone())

    def today(self) -> datetime.date:
        """返回 MoviePilot 时区的当前日期。"""
        return self.now().date()

    def _parse_datetime(
        self,
        value: Optional[str],
    ) -> Optional[datetime.datetime]:
        if not value:
            return None

        try:
            parsed = datetime.datetime.fromisoformat(str(value))

            if parsed.tzinfo is None:
                parsed = self._timezone().localize(parsed)

            return parsed.astimezone(self._timezone())
        except (TypeError, ValueError):
            return None

    def _load_all(self) -> Dict[str, Dict[str, Any]]:
        if not self._get_data:
            return {}

        try:
            data = self._get_data(self.DATA_KEY) or {}

            if not isinstance(data, dict):
                logger.warning(
                    "发布门禁状态格式异常，已忽略旧数据"
                )
                return {}

            return {
                str(key): value
                for key, value in data.items()
                if isinstance(value, dict)
            }
        except Exception as error:
            logger.warning(
                f"读取发布门禁状态失败：{error}"
            )
            return {}

    def _save_all(
        self,
        states: Dict[str, Dict[str, Any]],
    ) -> None:
        if not self._save_data:
            return

        try:
            self._save_data(self.DATA_KEY, states)
        except Exception as error:
            logger.warning(
                f"保存发布门禁状态失败：{error}"
            )

    def _default_state(
        self,
        media_type: str,
        tmdb_id: int,
        season: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "media_type": media_type,
            "tmdb_id": int(tmdb_id),
            "season": (
                int(season)
                if season is not None
                else None
            ),
            "released": False,
            "release_signal": None,
            "provider_countries": [],
            "provider_names": [],
            "next_known_release_date": None,
            "last_tmdb_check_date": None,
            "last_tmdb_check_status": None,
            "leak_probe_done": False,
            "last_leak_probe_at": None,
            "next_leak_probe_at": None,
            "updated_at": None,
        }

    def get_movie(
        self,
        tmdb_id: int,
    ) -> Dict[str, Any]:
        """读取电影门禁状态。"""
        key = self.movie_key(tmdb_id)
        default = self._default_state(
            media_type="movie",
            tmdb_id=tmdb_id,
        )

        stored = self._load_all().get(key)

        if stored:
            default.update(stored)

        return default

    def get_tv(
        self,
        tmdb_id: int,
        season: int,
    ) -> Dict[str, Any]:
        """读取电视剧季度门禁状态。"""
        key = self.tv_key(tmdb_id, season)
        default = self._default_state(
            media_type="tv",
            tmdb_id=tmdb_id,
            season=season,
        )

        stored = self._load_all().get(key)

        if stored:
            default.update(stored)

        return default

    def save(
        self,
        state: Dict[str, Any],
    ) -> None:
        """保存一个电影或电视剧季度状态。"""
        media_type = state.get("media_type")
        tmdb_id = state.get("tmdb_id")
        season = state.get("season")

        if media_type not in {"movie", "tv"} or not tmdb_id:
            logger.warning(
                "发布门禁状态缺少 media_type 或 tmdb_id，跳过保存"
            )
            return

        if media_type == "movie":
            key = self.movie_key(tmdb_id)
        else:
            if season is None:
                logger.warning(
                    "电视剧发布门禁状态缺少 season，跳过保存"
                )
                return

            key = self.tv_key(tmdb_id, season)

        states = self._load_all()
        states = self._cleanup(states)

        saved_state = dict(state)
        saved_state["schema_version"] = self.SCHEMA_VERSION
        saved_state["updated_at"] = self.now().isoformat()

        states[key] = saved_state
        self._save_all(states)

    def _cleanup(
        self,
        states: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """清理超过 90 天未更新的门禁状态。"""
        cutoff = self.now() - datetime.timedelta(
            days=self.RETENTION_DAYS
        )

        cleaned: Dict[str, Dict[str, Any]] = {}

        for key, state in states.items():
            updated_at = self._parse_datetime(
                state.get("updated_at")
            )

            if updated_at and updated_at < cutoff:
                logger.info(
                    f"清理过期发布门禁状态：{key}"
                )
                continue

            cleaned[key] = state

        return cleaned
