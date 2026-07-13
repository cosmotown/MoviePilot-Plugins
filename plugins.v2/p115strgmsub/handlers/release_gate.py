"""
AYCLUB 发布门禁状态存储。

负责持久化电影、电视剧季度的 TMDB 检查状态、
泄漏探测时间以及已经确认的发布信号。
"""
import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytz

from app.core.config import settings
from app.log import logger
from app.modules.themoviedb.tmdbapi import TmdbApi


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
    @staticmethod
    def _parse_tmdb_date(
        value: Any,
    ) -> Optional[datetime.date]:
        """解析 TMDB 日期或日期时间。"""
        if not value:
            return None

        text = str(value).strip()
        if not text:
            return None

        try:
            return datetime.date.fromisoformat(text[:10])
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _fetch_movie_watch_providers(
        tmdb_id: int,
    ) -> Dict[str, Any]:
        """查询电影 Watch Providers。"""
        api = TmdbApi()
        result = api.movie.watch_providers(int(tmdb_id))
        return result if isinstance(result, dict) else {}

    @staticmethod
    def _fetch_movie_release_dates(
        tmdb_id: int,
    ) -> List[Dict[str, Any]]:
        """查询电影各地区发行日期。"""
        api = TmdbApi()
        result = api.movie.release_dates(int(tmdb_id))
        return result if isinstance(result, list) else []

    @staticmethod
    def _analyze_watch_providers(
        providers: Dict[str, Any],
    ) -> Tuple[List[str], List[str]]:
        """
        分析全球 Watch Providers。

        flatrate、free、ads、rent、buy 任一类型存在，
        就视为已经出现数字可用信号。
        """
        available_buckets = (
            "flatrate",
            "free",
            "ads",
            "rent",
            "buy",
        )

        countries = set()
        provider_names = set()

        for country, detail in providers.items():
            if not isinstance(detail, dict):
                continue

            country_available = False

            for bucket in available_buckets:
                items = detail.get(bucket) or []
                if not isinstance(items, list) or not items:
                    continue

                country_available = True

                for provider in items:
                    if not isinstance(provider, dict):
                        continue

                    provider_name = provider.get("provider_name")
                    if provider_name:
                        provider_names.add(str(provider_name))

            if country_available:
                countries.add(str(country))

        return sorted(countries), sorted(provider_names)

    def _analyze_release_dates(
        self,
        release_regions: List[Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        分析电影发行日期。

        TMDB 类型：
        4 = Digital
        5 = Physical
        6 = TV
        """
        today = self.today()
        valid_types = {4, 5, 6}
        type_priority = {
            4: 0,
            5: 1,
            6: 2,
        }

        released_candidates = []
        future_dates = []

        for region in release_regions:
            if not isinstance(region, dict):
                continue

            country = str(
                region.get("iso_3166_1") or ""
            )

            for release in region.get("release_dates") or []:
                if not isinstance(release, dict):
                    continue

                try:
                    release_type = int(release.get("type"))
                except (TypeError, ValueError):
                    continue

                if release_type not in valid_types:
                    continue

                release_date = self._parse_tmdb_date(
                    release.get("release_date")
                )
                if not release_date:
                    continue

                if release_date <= today:
                    released_candidates.append((
                        type_priority[release_type],
                        release_date,
                        release_type,
                        country,
                    ))
                else:
                    future_dates.append(release_date)

        release_signal = None

        if released_candidates:
            released_candidates.sort(
                key=lambda item: (
                    item[0],
                    item[1],
                    item[3],
                )
            )

            _, signal_date, release_type, country = (
                released_candidates[0]
            )

            release_signal = (
                f"release_type_{release_type}:"
                f"{country or 'unknown'}:"
                f"{signal_date.isoformat()}"
            )

        next_release_date = (
            min(future_dates).isoformat()
            if future_dates
            else None
        )

        return release_signal, next_release_date

    def _movie_probe_due(
        self,
        state: Dict[str, Any],
    ) -> bool:
        """判断未发布电影是否到达泄漏探测时间。"""
        if state.get("released"):
            return False

        if not state.get("leak_probe_done"):
            return True

        next_probe_at = self._parse_datetime(
            state.get("next_leak_probe_at")
        )

        if next_probe_at:
            return self.now() >= next_probe_at

        last_probe_at = self._parse_datetime(
            state.get("last_leak_probe_at")
        )

        if not last_probe_at:
            return True

        return self.now() >= (
            last_probe_at + datetime.timedelta(days=14)
        )

    @staticmethod
    def _movie_decision(
        state: Dict[str, Any],
        allow_ayclub: bool,
        probe_due: bool,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "allow_ayclub": allow_ayclub,
            "ayclub_first": allow_ayclub,
            "probe_due": probe_due,
            "released": bool(state.get("released")),
            "reason": reason,
            "state": state,
        }

    def evaluate_movie(
        self,
        tmdb_id: int,
    ) -> Dict[str, Any]:
        """
        判断电影本次是否允许查询 AYCLUB。

        已确认发布后永久允许；
        未发布时只在泄漏探测到期时允许。
        """
        state = self.get_movie(tmdb_id)

        if state.get("released"):
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=False,
                reason=str(
                    state.get("release_signal")
                    or "released"
                ),
            )

        today_text = self.today().isoformat()

        already_checked_today = (
            state.get("last_tmdb_check_date") == today_text
            and state.get("last_tmdb_check_status") == "ok"
        )

        if not already_checked_today:
            providers: Dict[str, Any] = {}
            release_regions: List[Dict[str, Any]] = []
            providers_ok = False
            release_dates_ok = False

            # 两项数据始终同时检查，不因其中一项有结果而跳过另一项。
            with ThreadPoolExecutor(
                max_workers=2,
                thread_name_prefix="p115-release-gate",
            ) as executor:
                providers_future = executor.submit(
                    self._fetch_movie_watch_providers,
                    int(tmdb_id),
                )
                releases_future = executor.submit(
                    self._fetch_movie_release_dates,
                    int(tmdb_id),
                )

                try:
                    providers = providers_future.result()
                    providers_ok = True
                except Exception as error:
                    logger.warning(
                        f"电影 TMDB {tmdb_id} 查询 "
                        f"Watch Providers 失败：{error}"
                    )

                try:
                    release_regions = releases_future.result()
                    release_dates_ok = True
                except Exception as error:
                    logger.warning(
                        f"电影 TMDB {tmdb_id} 查询 "
                        f"发行日期失败：{error}"
                    )

            provider_countries: List[str] = []
            provider_names: List[str] = []

            if providers_ok:
                (
                    provider_countries,
                    provider_names,
                ) = self._analyze_watch_providers(providers)

                state["provider_countries"] = provider_countries
                state["provider_names"] = provider_names

            release_signal = None
            next_release_date = None

            if release_dates_ok:
                (
                    release_signal,
                    next_release_date,
                ) = self._analyze_release_dates(
                    release_regions
                )

                state["next_known_release_date"] = (
                    next_release_date
                )

            provider_released = bool(provider_countries)
            date_released = bool(release_signal)

            if provider_released or date_released:
                state["released"] = True

                if provider_released:
                    state["release_signal"] = "watch_provider"
                    logger.info(
                        f"电影 TMDB {tmdb_id} 已出现全球流媒体、"
                        f"租赁或购买提供商："
                        f"{', '.join(provider_names) or '未知提供商'}；"
                        f"数据由 JustWatch 通过 TMDB 提供"
                    )
                else:
                    state["release_signal"] = release_signal

                    if release_signal.startswith(
                        "release_type_6:"
                    ):
                        logger.info(
                            f"电影 TMDB {tmdb_id} 已出现 "
                            f"TMDB 电视播出发行信号；"
                            f"该信号不等同于确认流媒体上线"
                        )
                    else:
                        logger.info(
                            f"电影 TMDB {tmdb_id} 已出现 "
                            f"TMDB 数字或实体发行信号："
                            f"{release_signal}"
                        )

                state["last_tmdb_check_date"] = today_text
                state["last_tmdb_check_status"] = "ok"
                self.save(state)

                return self._movie_decision(
                    state=state,
                    allow_ayclub=True,
                    probe_due=False,
                    reason=str(state["release_signal"]),
                )

            if providers_ok and release_dates_ok:
                state["last_tmdb_check_date"] = today_text
                state["last_tmdb_check_status"] = "ok"
                self.save(state)
            else:
                # 查询不完整时不能确认“尚未发布”，
                # 不更新成功检查日期，也不消耗泄漏探测。
                state["last_tmdb_check_status"] = "error"
                self.save(state)

                return self._movie_decision(
                    state=state,
                    allow_ayclub=False,
                    probe_due=False,
                    reason="tmdb_error",
                )

        probe_due = self._movie_probe_due(state)

        return self._movie_decision(
            state=state,
            allow_ayclub=probe_due,
            probe_due=probe_due,
            reason=(
                "leak_probe_due"
                if probe_due
                else "unreleased_probe_wait"
            ),
        )

    def mark_movie_probe_result(
        self,
        tmdb_id: int,
        search_status: str,
    ) -> None:
        """
        记录电影泄漏探测结果。

        只有 AYCLUB 成功查询且明确无结果时，才消耗探测机会。
        """
        if search_status != "ok_empty":
            return

        state = self.get_movie(tmdb_id)

        if state.get("released"):
            return

        now = self.now()

        state["leak_probe_done"] = True
        state["last_leak_probe_at"] = now.isoformat()
        state["next_leak_probe_at"] = (
            now + datetime.timedelta(days=14)
        ).isoformat()

        self.save(state)

        logger.info(
            f"电影 TMDB {tmdb_id} 泄漏探测无结果，"
            f"下一次探测时间：{state['next_leak_probe_at']}"
        )
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
