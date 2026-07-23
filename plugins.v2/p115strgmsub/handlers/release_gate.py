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
from app.modules.themoviedb.tmdbv3api import Movie


class ReleaseGateStore:
    """发布门禁状态持久化管理器。"""

    DATA_KEY = "release_gate_state"
    SCHEMA_VERSION = 3
    RETENTION_DAYS = 90
    MOVIE_DAILY_SEARCH_LIMIT = 1
    MOVIE_DAILY_FAILURE_LIMIT = 2
    MOVIE_FAILURE_RETRY_HOURS = 6
    MOVIE_NO_RESULT_COOLDOWN_HOURS = 24

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
            "aired_episode_frontier": None,
            "last_tmdb_check_date": None,
            "last_tmdb_check_status": None,
            "leak_probe_done": False,
            "last_leak_probe_at": None,
            "next_leak_probe_at": None,
            # 电影真实 AYCLUB 搜索节流状态（schema v2）
            "digital_release_date": None,
            "other_release_date": None,
            "theatrical_release_date": None,
            "movie_empty_count": 0,
            "last_movie_real_search_at": None,
            "next_movie_real_search_at": None,
            "last_movie_search_status": None,
            "last_movie_search_interval_days": None,
            # 电影 AYCLUB 统一限流状态（schema v3）。
            # 普通、生命周期和定时晚间搜索共享同一份每日额度；
            # 只有明确的人工强刷入口可以绕过。
            "force_refresh_pending": False,
            "last_real_search_at": None,
            "retry_after": None,
            "daily_search_date": None,
            "daily_search_count": 0,
            "daily_failure_count": 0,
            "no_result_cooldown_until": None,
            "last_skip_reason": None,
            "last_search_trigger": None,
            "last_request_id": None,
            "updated_at": None,
        }


    def invalidate_media(
        self,
        media_type: str,
        tmdb_id: Optional[int],
        season: Optional[int] = None,
    ) -> bool:
        """在 MP 重置订阅时清除对应媒体的发布门禁缓存。"""
        if not tmdb_id:
            return False
        try:
            key = (
                self.tv_key(int(tmdb_id), int(season or 1))
                if str(media_type).lower() in {"tv", "电视剧"}
                else self.movie_key(int(tmdb_id))
            )
            states = self._load_all()
            existed = key in states
            if existed:
                states.pop(key, None)
                self._save_all(states)
                logger.info(f"已清除 AYCLUB 发布门禁缓存：{key}")
            return existed
        except Exception as error:
            logger.warning(f"清除发布门禁缓存失败：{error}")
            return False

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
        client = Movie()

        try:
            result = client.watch_providers(int(tmdb_id))
            return result if isinstance(result, dict) else {}
        finally:
            client.close()

    @staticmethod
    def _fetch_movie_release_dates(
        tmdb_id: int,
    ) -> List[Dict[str, Any]]:
        """查询电影各地区发行日期。"""
        client = Movie()

        try:
            result = client.release_dates(int(tmdb_id))
            return result if isinstance(result, list) else []
        finally:
            client.close()

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
    ) -> Tuple[
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        """
        分析电影发行日期。

        TMDB 类型：4=Digital、5=Physical、6=TV。
        返回：发布信号、下一已知发行日、最早数字发行日、最早实体/电视发行日。
        """
        today = self.today()
        valid_types = {4, 5, 6}
        type_priority = {4: 0, 5: 1, 6: 2}
        released_candidates = []
        future_dates = []
        digital_dates = []
        other_dates = []

        for region in release_regions:
            if not isinstance(region, dict):
                continue
            country = str(region.get("iso_3166_1") or "")
            for release in region.get("release_dates") or []:
                if not isinstance(release, dict):
                    continue
                try:
                    release_type = int(release.get("type"))
                except (TypeError, ValueError):
                    continue
                if release_type not in valid_types:
                    continue
                release_date = self._parse_tmdb_date(release.get("release_date"))
                if not release_date:
                    continue

                if release_type == 4:
                    digital_dates.append(release_date)
                else:
                    other_dates.append(release_date)

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
                key=lambda item: (item[0], item[1], item[3])
            )
            _, signal_date, release_type, country = released_candidates[0]
            release_signal = (
                f"release_type_{release_type}:"
                f"{country or 'unknown'}:{signal_date.isoformat()}"
            )

        return (
            release_signal,
            min(future_dates).isoformat() if future_dates else None,
            min(digital_dates).isoformat() if digital_dates else None,
            min(other_dates).isoformat() if other_dates else None,
        )

    @staticmethod
    def _movie_empty_backoff_floor_days(empty_count: int) -> int:
        """连续无结果退避：第2次至少3天、第3次7天、第4次起14天。"""
        try:
            count = max(0, int(empty_count or 0))
        except (TypeError, ValueError):
            count = 0
        if count >= 4:
            return 14
        if count == 3:
            return 7
        if count == 2:
            return 3
        return 0

    def _movie_base_interval_days(self, state: Dict[str, Any]) -> int:
        """按数字发行日优先、影院上映日兜底计算电影基础搜索间隔。"""
        today = self.today()
        digital_date = self._parse_tmdb_date(state.get("digital_release_date"))
        if digital_date and digital_date <= today:
            age = (today - digital_date).days
            if age <= 7:
                return 1
            if age <= 30:
                return 2
            if age <= 90:
                return 4
            if age <= 365:
                return 7
            return 14

        theatrical_date = self._parse_tmdb_date(
            state.get("theatrical_release_date")
        )
        if not theatrical_date:
            return 14
        age = (today - theatrical_date).days
        if age < 30:
            return 14
        if age <= 90:
            return 7
        return 14

    def _movie_search_interval_days(self, state: Dict[str, Any]) -> int:
        base = self._movie_base_interval_days(state)
        floor = self._movie_empty_backoff_floor_days(
            state.get("movie_empty_count") or 0
        )
        return max(base, floor)

    def _movie_real_search_due(self, state: Dict[str, Any]) -> bool:
        if state.get("last_movie_search_status") in {
            "timeout",
            "http_error",
            "error",
            "invalid_result",
            "pending_reply",
        }:
            retry_after = self._parse_datetime(state.get("retry_after"))
            return not retry_after or self.now() >= retry_after

        last_search = self._parse_datetime(
            state.get("last_movie_real_search_at")
        )
        if not last_search:
            return True
        interval_days = self._movie_search_interval_days(state)
        return self.now() >= (
            last_search + datetime.timedelta(days=interval_days)
        )

    def _normalize_movie_daily_state(
        self,
        state: Dict[str, Any],
    ) -> None:
        """跨日时重置电影真实搜索的每日计数。"""
        today_text = self.today().isoformat()
        if state.get("daily_search_date") == today_text:
            return
        state["daily_search_date"] = today_text
        last_search = self._parse_datetime(
            state.get("last_real_search_at")
            or state.get("last_movie_real_search_at")
        )
        state["daily_search_count"] = (
            1
            if last_search and last_search.date() == self.today()
            else 0
        )
        state["daily_failure_count"] = 0

    @staticmethod
    def _safe_count(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def _movie_automatic_skip_reason(
        self,
        state: Dict[str, Any],
    ) -> Optional[str]:
        """返回自动真实搜索应跳过的原因；人工强刷不调用此门禁。"""
        self._normalize_movie_daily_state(state)
        now = self.now()
        retry_after = self._parse_datetime(state.get("retry_after"))
        if retry_after and now < retry_after:
            return "retry_after_wait"

        cooldown_until = self._parse_datetime(
            state.get("no_result_cooldown_until")
        )
        if cooldown_until and now < cooldown_until:
            return "no_result_cooldown"

        if (
            self._safe_count(state.get("daily_failure_count"))
            >= self.MOVIE_DAILY_FAILURE_LIMIT
        ):
            return "daily_failure_limit"

        if (
            self._safe_count(state.get("daily_search_count"))
            >= self.MOVIE_DAILY_SEARCH_LIMIT
        ):
            return "daily_search_limit"

        return None

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
        *,
        force_refresh: bool = False,
        cache_only: bool = False,
        real_search_due: bool = False,
        interval_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {
            "allow_ayclub": allow_ayclub,
            "ayclub_first": allow_ayclub,
            "probe_due": probe_due,
            "released": bool(state.get("released")),
            "reason": reason,
            "force_refresh": bool(force_refresh),
            "cache_only": bool(cache_only),
            "real_search_due": bool(real_search_due),
            "interval_days": interval_days,
            "next_search_at": state.get("next_movie_real_search_at"),
            "force_refresh_pending": bool(
                state.get("force_refresh_pending")
            ),
            "last_real_search_at": (
                state.get("last_real_search_at")
                or state.get("last_movie_real_search_at")
            ),
            "retry_after": state.get("retry_after"),
            "daily_search_count": ReleaseGateStore._safe_count(
                state.get("daily_search_count")
            ),
            "no_result_cooldown_until": state.get(
                "no_result_cooldown_until"
            ),
            "skip_reason": (
                reason
                if cache_only or not allow_ayclub
                else None
            ),
            "state": state,
        }

    def evaluate_movie(
        self,
        tmdb_id: int,
        theatrical_date: Optional[str] = None,
        lifecycle_force_refresh: bool = False,
        scheduled_evening_refresh: bool = False,
        explicit_manual_force_refresh: bool = False,
        query_origin: str = "unknown",
    ) -> Dict[str, Any]:
        """
        判断电影本次 AYCLUB 查询模式。

        普通自动任务严格只读缓存；cron 当天最后一轮或明确手动/API
        全量任务到期后按电影发布与退避门禁决定是否真实查询。
        新订阅、重新订阅、MP reset：允许立即强刷。
        """
        state = self.get_movie(tmdb_id)
        parsed_theatrical = self._parse_tmdb_date(theatrical_date)
        theatrical_changed = False
        if parsed_theatrical:
            theatrical_text = parsed_theatrical.isoformat()
            theatrical_changed = (
                state.get("theatrical_release_date") != theatrical_text
            )
            state["theatrical_release_date"] = theatrical_text

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
                        f"电影 TMDB {tmdb_id} 查询 Watch Providers 失败：{error}"
                    )
                try:
                    release_regions = releases_future.result()
                    release_dates_ok = True
                except Exception as error:
                    logger.warning(
                        f"电影 TMDB {tmdb_id} 查询发行日期失败：{error}"
                    )

            provider_countries: List[str] = []
            provider_names: List[str] = []
            if providers_ok:
                provider_countries, provider_names = (
                    self._analyze_watch_providers(providers)
                )
                state["provider_countries"] = provider_countries
                state["provider_names"] = provider_names

            release_signal = None
            next_release_date = None
            digital_release_date = None
            other_release_date = None
            if release_dates_ok:
                (
                    release_signal,
                    next_release_date,
                    digital_release_date,
                    other_release_date,
                ) = self._analyze_release_dates(release_regions)
                state["next_known_release_date"] = next_release_date
                if digital_release_date:
                    state["digital_release_date"] = digital_release_date
                if other_release_date:
                    state["other_release_date"] = other_release_date

            provider_released = bool(provider_countries)
            date_released = bool(release_signal)
            if provider_released or date_released:
                state["released"] = True
                if provider_released:
                    state["release_signal"] = "watch_provider"
                    logger.info(
                        f"电影 TMDB {tmdb_id} 已出现全球流媒体、租赁或购买提供商："
                        f"{', '.join(provider_names) or '未知提供商'}"
                    )
                elif release_signal:
                    state["release_signal"] = release_signal
                    logger.info(
                        f"电影 TMDB {tmdb_id} 已出现 TMDB 数字/实体/电视发行信号："
                        f"{release_signal}"
                    )

            if providers_ok and release_dates_ok:
                state["last_tmdb_check_date"] = today_text
                state["last_tmdb_check_status"] = "ok"
            else:
                state["last_tmdb_check_status"] = "error"
            self.save(state)
        elif theatrical_changed:
            # 当天已检查过 TMDB 时，也要持久化 MoviePilot 给出的影院上映日。
            self.save(state)

        self._normalize_movie_daily_state(state)
        state["force_refresh_pending"] = bool(lifecycle_force_refresh)

        # 只有明确的人工强刷入口可以绕过每日额度、失败退避和无结果冷却。
        if explicit_manual_force_refresh:
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=not bool(state.get("released")),
                reason="explicit_manual_force_refresh",
                force_refresh=True,
                cache_only=False,
                real_search_due=True,
                interval_days=self._movie_search_interval_days(state),
            )

        automatic_skip_reason = self._movie_automatic_skip_reason(state)
        if automatic_skip_reason:
            state["last_skip_reason"] = automatic_skip_reason
            self.save(state)
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=not bool(state.get("released")),
                reason=automatic_skip_reason,
                force_refresh=False,
                cache_only=True,
                real_search_due=False,
                interval_days=self._movie_search_interval_days(state),
            )

        # 生命周期事件有独立来源授权，但仍受统一每日额度和失败退避约束。
        if lifecycle_force_refresh:
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=not bool(state.get("released")),
                reason="lifecycle_force_refresh",
                force_refresh=True,
                cache_only=False,
                real_search_due=True,
                interval_days=self._movie_search_interval_days(state),
            )

        trigger_not_authorized = bool(
            not scheduled_evening_refresh
            and query_origin != "manual_or_api_full"
        )
        real_search_authorized = bool(
            scheduled_evening_refresh
            or query_origin == "manual_or_api_full"
        )
        authorization_reason = (
            "scheduled_evening_refresh"
            if scheduled_evening_refresh
            else "explicit_manual_refresh"
        )

        if state.get("released"):
            interval_days = self._movie_search_interval_days(state)
            search_due = self._movie_real_search_due(state)
            if search_due and real_search_authorized:
                return self._movie_decision(
                    state=state,
                    allow_ayclub=True,
                    probe_due=False,
                    reason=f"released_search_due_{authorization_reason}",
                    force_refresh=True,
                    cache_only=False,
                    real_search_due=True,
                    interval_days=interval_days,
                )
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=False,
                reason=(
                    "released_cache_only_trigger_not_authorized"
                    if trigger_not_authorized
                    else "released_cache_only_backoff_wait"
                ),
                force_refresh=False,
                cache_only=True,
                real_search_due=search_due,
                interval_days=interval_days,
            )

        probe_due = self._movie_probe_due(state)
        if not probe_due:
            return self._movie_decision(
                state=state,
                allow_ayclub=False,
                probe_due=False,
                reason="unreleased_probe_wait",
                interval_days=14,
            )

        if real_search_authorized:
            return self._movie_decision(
                state=state,
                allow_ayclub=True,
                probe_due=True,
                reason=f"unreleased_probe_due_{authorization_reason}",
                force_refresh=True,
                cache_only=False,
                real_search_due=True,
                interval_days=14,
            )

        return self._movie_decision(
            state=state,
            allow_ayclub=True,
            probe_due=True,
            reason=(
                "unreleased_probe_due_cache_only_trigger_not_authorized"
                if trigger_not_authorized
                else "unreleased_probe_due_cache_only_daytime"
            ),
            force_refresh=False,
            cache_only=True,
            real_search_due=True,
            interval_days=14,
        )

    def reserve_movie_real_search(
        self,
        tmdb_id: int,
        trigger_reason: str,
        *,
        lifecycle_force_refresh: bool = False,
        explicit_manual_force_refresh: bool = False,
        request_id: Optional[str] = None,
    ) -> bool:
        """在访问桥接前持久化真实搜索额度，避免超时后重复发送消息。"""
        state = self.get_movie(tmdb_id)
        self._normalize_movie_daily_state(state)

        if not explicit_manual_force_refresh:
            skip_reason = self._movie_automatic_skip_reason(state)
            if skip_reason:
                state["last_skip_reason"] = skip_reason
                self.save(state)
                logger.info(
                    f"电影 TMDB {tmdb_id} 跳过 AYCLUB 真实搜索："
                    f"跳过原因={skip_reason}，"
                    f"force_refresh_pending={bool(lifecycle_force_refresh)}，"
                    f"last_real_search_at={state.get('last_real_search_at')}，"
                    f"retry_after={state.get('retry_after')}，"
                    f"daily_search_count={self._safe_count(state.get('daily_search_count'))}，"
                    f"no_result_cooldown_until={state.get('no_result_cooldown_until')}"
                )
                return False

        now = self.now()
        state["daily_search_count"] = (
            self._safe_count(state.get("daily_search_count")) + 1
        )
        state["last_real_search_at"] = now.isoformat()
        state["last_movie_real_search_at"] = now.isoformat()
        state["last_search_trigger"] = str(trigger_reason or "unknown")
        state["last_request_id"] = request_id
        state["last_skip_reason"] = None
        state["force_refresh_pending"] = bool(lifecycle_force_refresh)
        self.save(state)
        logger.info(
            f"电影 TMDB {tmdb_id} 已占用 AYCLUB 真实搜索额度："
            f"原因={trigger_reason}，"
            f"force_refresh_pending={state['force_refresh_pending']}，"
            f"last_real_search_at={state['last_real_search_at']}，"
            f"retry_after={state.get('retry_after')}，"
            f"daily_search_count={state['daily_search_count']}，"
            f"no_result_cooldown_until={state.get('no_result_cooldown_until')}"
        )
        return True

    def mark_movie_search_result(
        self,
        tmdb_id: int,
        search_status: str,
        cached: Optional[bool],
        force_honored: bool = False,
        usable_candidate: Optional[bool] = None,
        attempt_reserved: bool = False,
        late_reply: bool = False,
        request_id: Optional[str] = None,
    ) -> bool:
        """记录一次真实 AYCLUB 电影查询，并计算下次允许时间。

        桥接返回 ok_matched 只代表标题层面有候选。若所有 AYCLUB 分享
        均失效、无内容或没有匹配影片文件，则按本次无可用结果退避。
        """
        state = self.get_movie(tmdb_id)
        self._normalize_movie_daily_state(state)
        now = self.now()
        bridge_status = str(search_status or "")
        failure_statuses = {
            "timeout",
            "http_error",
            "error",
            "invalid_result",
            "pending_reply",
        }
        terminal_statuses = {
            "ok_empty",
            "ok_matched",
            "cached_empty",
        }
        is_failure = bridge_status in failure_statuses
        is_late_terminal = bool(late_reply and bridge_status in terminal_statuses)
        real_query = bool(
            attempt_reserved
            or force_honored
            or cached is False
        )

        if not real_query and not is_late_terminal:
            return False

        if not attempt_reserved and real_query:
            state["daily_search_count"] = (
                self._safe_count(state.get("daily_search_count")) + 1
            )
            state["last_real_search_at"] = now.isoformat()
            state["last_movie_real_search_at"] = now.isoformat()

        state["last_movie_bridge_status"] = bridge_status
        if request_id:
            state["last_request_id"] = request_id

        if is_failure:
            state["daily_failure_count"] = min(
                self.MOVIE_DAILY_FAILURE_LIMIT,
                self._safe_count(state.get("daily_failure_count")) + 1,
            )
            state["last_movie_search_status"] = bridge_status
            state["retry_after"] = (
                now + datetime.timedelta(
                    hours=self.MOVIE_FAILURE_RETRY_HOURS
                )
            ).isoformat()
            state["last_skip_reason"] = "retry_after_wait"
            self.save(state)
            logger.warning(
                f"电影 TMDB {tmdb_id} AYCLUB 真实查询失败："
                f"status={bridge_status}，"
                f"force_refresh_pending={state.get('force_refresh_pending')}，"
                f"last_real_search_at={state.get('last_real_search_at')}，"
                f"retry_after={state.get('retry_after')}，"
                f"daily_search_count={state.get('daily_search_count')}，"
                f"daily_failure_count={state.get('daily_failure_count')}，"
                f"no_result_cooldown_until={state.get('no_result_cooldown_until')}，"
                "跳过原因=后续自动任务等待 retry_after 且受每日次数限制"
            )
            return True

        if bridge_status not in terminal_statuses:
            return False

        effective_status = bridge_status
        if bridge_status == "cached_empty" and is_late_terminal:
            effective_status = "ok_empty"
        if bridge_status == "ok_matched" and usable_candidate is False:
            effective_status = "ok_empty"

        if effective_status == "ok_empty":
            try:
                empty_count = int(state.get("movie_empty_count") or 0) + 1
            except (TypeError, ValueError):
                empty_count = 1
            state["movie_empty_count"] = empty_count
        else:
            state["movie_empty_count"] = 0

        interval_days = (
            self._movie_search_interval_days(state)
            if state.get("released")
            else 14
        )
        if not state.get("last_movie_real_search_at"):
            state["last_movie_real_search_at"] = now.isoformat()
        if not state.get("last_real_search_at"):
            state["last_real_search_at"] = (
                state.get("last_movie_real_search_at")
            )
        state["last_movie_search_status"] = effective_status
        state["last_movie_search_interval_days"] = interval_days
        state["next_movie_real_search_at"] = (
            now + datetime.timedelta(days=interval_days)
        ).isoformat()
        state["retry_after"] = None
        state["daily_failure_count"] = 0
        state["last_skip_reason"] = None

        if effective_status == "ok_empty":
            cooldown_until = now + datetime.timedelta(
                hours=self.MOVIE_NO_RESULT_COOLDOWN_HOURS
            )
            interval_until = self._parse_datetime(
                state["next_movie_real_search_at"]
            )
            if interval_until and interval_until > cooldown_until:
                cooldown_until = interval_until
            next_release_date = self._parse_tmdb_date(
                state.get("next_known_release_date")
            )
            if (
                not state.get("released")
                and next_release_date
                and next_release_date > self.today()
            ):
                release_until = self._timezone().localize(
                    datetime.datetime.combine(
                        next_release_date,
                        datetime.time.min,
                    )
                )
                if release_until > cooldown_until:
                    cooldown_until = release_until
            state["no_result_cooldown_until"] = cooldown_until.isoformat()
            state["force_refresh_pending"] = False
        elif effective_status == "ok_matched":
            state["no_result_cooldown_until"] = None
            state["force_refresh_pending"] = False

        if not state.get("released") and effective_status == "ok_empty":
            state["leak_probe_done"] = True
            state["last_leak_probe_at"] = now.isoformat()
            state["next_leak_probe_at"] = state["next_movie_real_search_at"]

        self.save(state)
        logger.info(
            f"电影 TMDB {tmdb_id} AYCLUB 真实查询已记录："
            f"status={effective_status}（桥接={bridge_status}），"
            f"连续无结果={state.get('movie_empty_count', 0)}，"
            f"间隔={interval_days}天，下次允许={state['next_movie_real_search_at']}，"
            f"force_refresh_pending={state.get('force_refresh_pending')}，"
            f"last_real_search_at={state.get('last_real_search_at')}，"
            f"retry_after={state.get('retry_after')}，"
            f"daily_search_count={state.get('daily_search_count')}，"
            f"no_result_cooldown_until={state.get('no_result_cooldown_until')}"
        )
        return True

    def mark_movie_probe_result(
        self,
        tmdb_id: int,
        search_status: str,
    ) -> None:
        """兼容旧调用：仅在明确无结果时记录一次未发布电影探测。"""
        if search_status != "ok_empty":
            return
        self.mark_movie_search_result(
            tmdb_id=tmdb_id,
            search_status=search_status,
            cached=False,
            force_honored=False,
        )

    @staticmethod
    def _normalize_episode_numbers(
        episodes: List[int],
    ) -> List[int]:
        """清洗并排序集数列表。"""
        normalized = set()

        for episode in episodes or []:
            try:
                episode_number = int(episode)
            except (TypeError, ValueError):
                continue

            if episode_number > 0:
                normalized.add(episode_number)

        return sorted(normalized)

    def _tv_probe_due(
        self,
        state: Dict[str, Any],
        allow_released_unknown: bool = False,
    ) -> bool:
        """
        判断季度是否允许执行泄漏探测。

        未开播季度可以在首播前探测；
        已开播季度只对 air_date 未知的缺失集，
        按 14 天周期进行探测。
        """
        if (
            state.get("released")
            and not allow_released_unknown
        ):
            return False

        if not state.get("leak_probe_done"):
            return True

        # 未开播季度有明确下一播出日期时，
        # 到达该日期即可重新查询。
        if not state.get("released"):
            next_air_date = self._parse_tmdb_date(
                state.get("next_known_release_date")
            )

            if next_air_date:
                return self.today() >= next_air_date

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

    def evaluate_tv(
        self,
        tmdb_id: int,
        season: int,
        missing_episodes: List[int],
        episode_air_dates: Dict[int, Optional[str]],
        metadata_ok: bool = True,
    ) -> Dict[str, Any]:
        """
        根据季度剧集 air_date 判断 AYCLUB 搜索范围。

        已播出的缺失剧集立即允许查询；
        首播前允许一次泄漏探测；
        已开播但下一集尚未播出时等待 air_date；
        air_date 全部未知时每 14 天探测一次。
        """
        state = self.get_tv(tmdb_id, season)
        missing = self._normalize_episode_numbers(
            missing_episodes
        )

        base_decision = {
            "allow_ayclub": False,
            "ayclub_first": False,
            "probe_due": False,
            "released": bool(state.get("released")),
            "reason": "no_missing_episodes",
            "aired_episodes": [],
            "future_episodes": [],
            "unknown_episodes": [],
            "aired_episode_frontier": state.get(
                "aired_episode_frontier"
            ),
            "ayclub_episodes": [],
            "state": state,
        }

        if not missing:
            return base_decision

        if not metadata_ok:
            state["last_tmdb_check_status"] = "error"
            self.save(state)

            base_decision.update({
                "reason": "tmdb_error",
                "state": state,
            })
            return base_decision

        normalized_air_dates: Dict[int, Optional[str]] = {}

        for episode, air_date in (
            episode_air_dates or {}
        ).items():
            try:
                episode_number = int(episode)
            except (TypeError, ValueError):
                continue

            if episode_number > 0:
                normalized_air_dates[episode_number] = (
                    str(air_date)
                    if air_date
                    else None
                )

        today = self.today()
        aired_missing: List[int] = []
        future_missing: List[int] = []
        unknown_missing: List[int] = []
        future_dates: List[datetime.date] = []
        all_aired_dates: List[datetime.date] = []
        all_aired_episode_numbers: List[int] = []

        for episode_number, air_date_value in (
            normalized_air_dates.items()
        ):
            air_date = self._parse_tmdb_date(
                air_date_value
            )

            if air_date and air_date <= today:
                all_aired_dates.append(air_date)
                all_aired_episode_numbers.append(
                    episode_number
                )

        for episode in missing:
            air_date = self._parse_tmdb_date(
                normalized_air_dates.get(episode)
            )

            if not air_date:
                unknown_missing.append(episode)
            elif air_date <= today:
                aired_missing.append(episode)
            else:
                future_missing.append(episode)
                future_dates.append(air_date)

        state["last_tmdb_check_date"] = today.isoformat()
        state["last_tmdb_check_status"] = "ok"
        state["next_known_release_date"] = (
            min(future_dates).isoformat()
            if future_dates
            else None
        )

        if all_aired_episode_numbers:
            try:
                previous_frontier = int(
                    state.get(
                        "aired_episode_frontier"
                    ) or 0
                )
            except (TypeError, ValueError):
                previous_frontier = 0

            state["aired_episode_frontier"] = max(
                previous_frontier,
                max(all_aired_episode_numbers),
            )

        # 以整季是否已有任意一集播出判断季度是否已经开播，
        # 不能只看当前缺失的集数。
        if all_aired_dates:
            state["released"] = True
            state["release_signal"] = (
                f"episode_air_date:"
                f"{min(all_aired_dates).isoformat()}"
            )

        self.save(state)

        decision = {
            "allow_ayclub": False,
            "ayclub_first": False,
            "probe_due": False,
            "released": bool(state.get("released")),
            "reason": "",
            "aired_episodes": aired_missing,
            "future_episodes": future_missing,
            "unknown_episodes": unknown_missing,
            "aired_episode_frontier": state.get(
                "aired_episode_frontier"
            ),
            "ayclub_episodes": [],
            "state": state,
        }

        # 已播出的缺失集立即查询，不受泄漏探测周期限制。
        if aired_missing:
            decision.update({
                "allow_ayclub": True,
                "ayclub_first": True,
                "reason": "aired_missing_episodes",
                "ayclub_episodes": aired_missing,
            })
            return decision

        unknown_only_probe = bool(
            state.get("released")
            and unknown_missing
        )

        probe_due = self._tv_probe_due(
            state,
            allow_released_unknown=unknown_only_probe,
        )

        if probe_due:
            decision.update({
                "allow_ayclub": True,
                "ayclub_first": True,
                "probe_due": True,
                "reason": (
                    "unknown_air_date_probe_due"
                    if unknown_only_probe or not future_missing
                    else "preair_leak_probe_due"
                ),
                # 已开播后只探测日期未知的缺失集，
                # 不连带查询明确尚未播出的剧集。
                "ayclub_episodes": (
                    unknown_missing
                    if unknown_only_probe
                    else missing
                ),
            })
            return decision

        if future_missing:
            decision["reason"] = "waiting_next_air_date"
        elif unknown_missing:
            decision["reason"] = "unknown_air_date_probe_wait"
        else:
            decision["reason"] = "no_searchable_episode"

        return decision

    def mark_tv_probe_result(
        self,
        tmdb_id: int,
        season: int,
        search_status: str,
    ) -> None:
        """
        记录季度播出前泄漏探测结果。

        只有 AYCLUB 明确返回 ok_empty 才消耗探测机会。
        """
        if search_status != "ok_empty":
            return

        state = self.get_tv(tmdb_id, season)

        now = self.now()
        next_air_date = self._parse_tmdb_date(
            state.get("next_known_release_date")
        )

        if (
            not state.get("released")
            and next_air_date
            and next_air_date > self.today()
        ):
            next_probe_at = self._timezone().localize(
                datetime.datetime.combine(
                    next_air_date,
                    datetime.time.min,
                )
            )
        else:
            # 没有可靠的下一集日期时，每 14 天再探测一次。
            next_probe_at = (
                now + datetime.timedelta(days=14)
            )

        state["leak_probe_done"] = True
        state["last_leak_probe_at"] = now.isoformat()
        state["next_leak_probe_at"] = (
            next_probe_at.isoformat()
        )

        self.save(state)

        logger.info(
            f"电视剧 TMDB {tmdb_id} S{season} "
            f"泄漏探测无结果，下一次允许时间："
            f"{state['next_leak_probe_at']}"
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
        """仅清理超过 90 天未更新且尚未确认发布的状态。"""
        cutoff = self.now() - datetime.timedelta(
            days=self.RETENTION_DAYS
        )

        cleaned: Dict[str, Dict[str, Any]] = {}

        for key, state in states.items():
            updated_at = self._parse_datetime(
                state.get("updated_at")
            )

            if (
                not state.get("released")
                and updated_at
                and updated_at < cutoff
            ):
                logger.info(
                    f"清理过期发布门禁状态：{key}"
                )
                continue

            cleaned[key] = state

        return cleaned
