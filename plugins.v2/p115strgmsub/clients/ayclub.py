"""
AYCLUB Telegram 影视机器人桥接客户端。

通过本机 Telegram 桥接服务查询已配置的影视机器人，
按 TMDB、年份、标题、季集信息返回 115cdn 分享链接。
"""

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType


class AyclubClient:
    """AYCLUB Telegram 搜索桥接客户端。"""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:11592",
        enabled: bool = False,
        timeout: int = 120,
        max_pages: int = 5,
    ):
        self.base_url = (base_url or "").rstrip("/")
        self.enabled = bool(enabled)
        self.timeout = max(int(timeout or 120), 10)
        self.max_pages = min(max(int(max_pages or 5), 1), 10)
        # 最近一次查询状态，供发布门禁判断是否消耗探测次数
        self.last_status: str = "idle"
        self.last_error: str = ""
        self.last_cached: Optional[bool] = None
        self.last_cache_age_seconds: Optional[int] = None
        self.last_force_refresh_requested: bool = False
        self.last_force_refresh_honored: bool = False
        self.last_force_refresh_supported: Optional[bool] = None
        self.last_cache_only_requested: bool = False
        self.last_cache_only_honored: bool = False
        self.last_cache_only_supported: Optional[bool] = None

        self._session = requests.Session()

        # 本机服务禁止继承 MoviePilot / 系统代理，
        # 避免 127.0.0.1 请求被发往代理服务器。
        self._session.trust_env = False

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.base_url)
        
    @staticmethod
    def _is_allowed_share_url(share_url: str) -> bool:
        """
        只允许 HTTPS 的 115cdn.com 官方域名链接。

        防止桥接服务异常或被篡改时，把其他地址交给
        115 客户端或后续分类服务处理。
        """
        try:
            parsed = urlparse(share_url)
            hostname = (
                parsed.hostname or ""
            ).lower().rstrip(".")
            port = parsed.port
        except (TypeError, ValueError):
            return False

        return (
            parsed.scheme.lower() == "https"
            and not parsed.username
            and not parsed.password
            and port in (None, 443)
            and (
                hostname == "115cdn.com"
                or hostname.endswith(".115cdn.com")
            )
            and bool(
                parsed.path
                and parsed.path != "/"
            )
        )    

    @staticmethod
    def _is_allowed_ed2k_url(source_url: str) -> bool:
        """只接受完整 ED2K file 链接，后续提交时保留原始字符串。"""
        value = (source_url or "").strip()
        lowered = value.casefold()
        return bool(
            20 <= len(value) <= 16384
            and "\r" not in value
            and "\n" not in value
            and lowered.startswith("ed2k://|file|")
            and lowered.endswith("|/")
            and value.count("|") >= 5
        )

    def health(self) -> bool:
        """检查桥接服务是否正常且 Telegram Session 已授权。"""
        if not self.is_ready:
            return False

        try:
            response = self._session.get(
                f"{self.base_url}/health",
                timeout=10,
            )
            response.raise_for_status()

            data = response.json()
            return bool(
                data.get("ok")
                and data.get("telegram_authorized")
            )
        except Exception as error:
            logger.warning(f"AYCLUB 桥接健康检查失败：{error}")
            return False
        
    def invalidate_cache(
        self,
        *,
        tmdb_id: Optional[int],
        media_type: str,
        season: Optional[int] = None,
    ) -> bool:
        """尽力通知桥接清除指定媒体缓存；旧桥接不支持时安全降级。"""
        if not self.is_ready or not tmdb_id:
            return False
        try:
            response = self._session.post(
                f"{self.base_url}/cache/invalidate",
                json={
                    "tmdb_id": int(tmdb_id),
                    "media_type": str(media_type),
                    "season": int(season) if season is not None else None,
                },
                timeout=min(self.timeout, 15),
            )
            if response.status_code in (404, 405):
                logger.info("当前 AYCLUB 桥接暂不支持定向清缓存，将在下次查询携带 force_refresh")
                return False
            response.raise_for_status()
            data = response.json() if response.content else {}
            return bool(data.get("ok", True))
        except Exception as error:
            logger.warning(f"通知 AYCLUB 桥接清缓存失败：{error}")
            return False

    def search(
        self,
        mediainfo: MediaInfo,
        media_type: MediaType,
        season: Optional[int] = None,
        episodes: Optional[List[int]] = None,
        force_refresh: bool = False,
        cache_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        查询 AYCLUB 机器人资源。

        last_status 可能值：
        - ok_matched：成功找到有效资源
        - ok_empty：本次真实查询成功，但确实没有匹配资源
        - cached_empty：命中桥接空结果缓存，未重新查询 Telegram
        - invalid_result：接口称已匹配，但没有可用资源项
        - timeout / http_error / error：查询失败
        - disabled / invalid_request：未执行有效查询
        """
        self.last_status = "attempted"
        self.last_error = ""
        self.last_cached = None
        self.last_cache_age_seconds = None
        self.last_force_refresh_requested = bool(force_refresh)
        self.last_force_refresh_honored = False
        self.last_force_refresh_supported = None
        self.last_cache_only_requested = bool(cache_only)
        self.last_cache_only_honored = False
        self.last_cache_only_supported = None

        if not self.is_ready:
            self.last_status = "disabled"
            return []

        if not mediainfo or not mediainfo.tmdb_id:
            title = getattr(mediainfo, "title", "未知媒体")
            self.last_status = "invalid_request"
            self.last_error = "缺少 TMDB ID"
            logger.warning(
                f"{title} 缺少 TMDB ID，无法使用 AYCLUB 精确查询"
            )
            return []

        episode_numbers: List[int] = []
        for episode in episodes or []:
            try:
                episode_number = int(episode)
            except (TypeError, ValueError):
                continue

            if episode_number > 0 and episode_number not in episode_numbers:
                episode_numbers.append(episode_number)

        payload = {
            "title": mediainfo.title,
            "tmdb_id": int(mediainfo.tmdb_id),
            "media_type": (
                "movie"
                if media_type == MediaType.MOVIE
                else "tv"
            ),
            "year": mediainfo.year,
            "season": (
                int(season)
                if season is not None
                else None
            ),
            "episodes": sorted(episode_numbers),
            "max_pages": self.max_pages,
            "force_refresh": bool(force_refresh),
            "cache_only": bool(cache_only),
        }

        try:
            logger.info(
                f"使用 AYCLUB 查询：{mediainfo.title} "
                f"(TMDB ID: {mediainfo.tmdb_id}, "
                f"类型: {payload['media_type']}, "
                f"季: {season}, "
                f"集: {payload['episodes']}, "
                f"强刷: {payload['force_refresh']}, "
                f"仅缓存: {payload['cache_only']})"
            )

            fallback_used = False
            response = self._session.post(
                f"{self.base_url}/search",
                json=payload,
                timeout=self.timeout,
            )
            if response.status_code == 422 and cache_only:
                # cache_only 的安全目标是绝不访问 Telegram。旧桥接不支持时不能
                # 回退旧协议，否则会把白天缓存检查变成真实搜索。
                self.last_cache_only_supported = False
                self.last_status = "cache_only_unsupported"
                self.last_error = "桥接不支持 cache_only，已安全停止 AYCLUB 查询"
                logger.error(
                    "AYCLUB桥接不支持cache_only字段；为避免非晚间真实访问 Telegram，"
                    "本次不回退旧协议，请先升级桥接至 1.4.2"
                )
                return []

            if response.status_code == 422 and force_refresh:
                # 旧桥接请求模型可能不认识 force_refresh，自动回退旧协议。
                fallback_used = True
                self.last_force_refresh_supported = False
                legacy_payload = dict(payload)
                legacy_payload.pop("force_refresh", None)
                legacy_payload.pop("cache_only", None)
                logger.warning("AYCLUB桥接不支持force_refresh字段，已回退旧协议；本次不能保证绕过缓存")
                response = self._session.post(
                    f"{self.base_url}/search",
                    json=legacy_payload,
                    timeout=self.timeout,
                )
            response.raise_for_status()

            data = response.json()

            if data.get("ok") is False:
                error_message = str(
                    data.get("error")
                    or data.get("message")
                    or "桥接服务返回 ok=false"
                )
                self.last_status = "error"
                self.last_error = error_message
                logger.error(
                    f"AYCLUB 查询失败：{mediainfo.title}，"
                    f"桥接错误={error_message}"
                )
                return []

            cached_present = "cached" in data
            cached = bool(data.get("cached"))
            self.last_cached = cached if cached_present else None
            try:
                cache_age = data.get("cache_age_seconds", data.get("cache_age"))
                self.last_cache_age_seconds = int(cache_age) if cache_age is not None else None
            except (TypeError, ValueError):
                self.last_cache_age_seconds = None

            if force_refresh:
                explicit_honored = data.get("force_refresh_honored")
                if explicit_honored is not None:
                    self.last_force_refresh_honored = bool(explicit_honored)
                    self.last_force_refresh_supported = True
                elif not fallback_used and cached_present:
                    self.last_force_refresh_supported = True
                    self.last_force_refresh_honored = not cached
                else:
                    self.last_force_refresh_honored = False

            if cache_only:
                explicit_cache_only = data.get("cache_only_honored")
                if explicit_cache_only is not None:
                    self.last_cache_only_honored = bool(explicit_cache_only)
                    self.last_cache_only_supported = True
                else:
                    self.last_cache_only_honored = False
                    self.last_cache_only_supported = False

            bridge_status = str(data.get("status") or "")

            if not data.get("matched"):
                if bridge_status == "cache_miss":
                    self.last_status = "cache_miss"
                else:
                    self.last_status = (
                        "cached_empty"
                        if cached
                        else "ok_empty"
                    )

                logger.info(
                    f"AYCLUB 查询成功但未找到资源："
                    f"{mediainfo.title} "
                    f"(TMDB ID: {mediainfo.tmdb_id}, "
                    f"缓存命中: {self.last_cached}, "
                    f"缓存年龄: {self.last_cache_age_seconds}, "
                    f"强刷生效: {self.last_force_refresh_honored}, "
                    f"仅缓存生效: {self.last_cache_only_honored})"
                )
                return []

            results: List[Dict[str, Any]] = []

            for item in data.get("matches") or []:
                source_url = (item.get("source") or "").strip()
                resource_title = (item.get("title") or "").strip()
                source_kind = str(item.get("source_kind") or "").strip().casefold()

                if not source_kind:
                    if self._is_allowed_share_url(source_url):
                        source_kind = "115"
                    elif self._is_allowed_ed2k_url(source_url):
                        source_kind = "ed2k"

                source_valid = (
                    self._is_allowed_share_url(source_url)
                    if source_kind == "115"
                    else self._is_allowed_ed2k_url(source_url)
                    if source_kind == "ed2k"
                    else False
                )

                if not source_url or not resource_title or not source_valid:
                    continue

                results.append({
                    "url": source_url,
                    "title": resource_title,
                    "update_time": "",
                    "source": "ayclub",
                    "source_kind": source_kind,
                    "tmdb_id": item.get("tmdb_id"),
                    "year": item.get("year"),
                    "season": item.get("season"),
                    "episode": item.get("episode"),
                    "episodes": item.get("episodes"),
                    "resource_kind": item.get("resource_kind"),
                    "is_complete_season": item.get("is_complete_season"),
                    "episode_start": item.get("episode_start"),
                    "episode_end": item.get("episode_end"),
                    "resolution": item.get("resolution"),
                    "quality": item.get("quality"),
                    "codec": item.get("codec"),
                    "hdr": item.get("hdr"),
                    "title_match": item.get("title_match"),
                    "year_match": item.get("year_match"),
                })

            if results:
                self.last_status = "ok_matched"
            else:
                self.last_status = "invalid_result"
                self.last_error = (
                    "桥接服务返回 matched=true，"
                    "但没有包含有效 115/ED2K 链接和标题的资源项"
                )

            logger.info(
                f"AYCLUB 找到 {len(results)} 个精确匹配的 "
                f"资源（115/ED2K），扫描页数："
                f"{data.get('pages_scanned', 0)}，"
                f"状态：{self.last_status}，"
                f"缓存命中：{self.last_cached}，"
                f"缓存年龄：{self.last_cache_age_seconds}秒，"
                f"强刷请求：{self.last_force_refresh_requested}，"
                f"强刷生效：{self.last_force_refresh_honored}，"
                f"仅缓存请求：{self.last_cache_only_requested}，"
                f"仅缓存生效：{self.last_cache_only_honored}"
            )

            return results

        except requests.Timeout as error:
            self.last_status = "timeout"
            self.last_error = str(error) or f"查询超过 {self.timeout} 秒"
            logger.error(
                f"AYCLUB 查询超时：{mediainfo.title}，"
                f"超时设置={self.timeout}秒"
            )
            return []

        except requests.HTTPError as error:
            body = ""

            try:
                body = error.response.text[:500]
            except Exception:
                pass

            status_code = getattr(error.response, "status_code", "?")
            self.last_status = "http_error"
            self.last_error = f"HTTP {status_code}: {body}"

            logger.error(
                f"AYCLUB 查询失败：HTTP {status_code}，"
                f"响应={body}"
            )
            return []

        except Exception as error:
            self.last_status = "error"
            self.last_error = f"{type(error).__name__}: {error}"
            logger.error(
                f"AYCLUB 查询异常：{self.last_error}"
            )
            return []
            
