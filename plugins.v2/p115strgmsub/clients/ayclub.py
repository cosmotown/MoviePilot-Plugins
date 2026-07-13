"""
AYCLUB Telegram 影视机器人桥接客户端。

通过本机 tg-ayclub-bridge 服务查询 @ayclub_bot，
按 TMDB、年份、标题、季集信息返回 115cdn 分享链接。
"""

from typing import Any, Dict, List, Optional

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
        
        self._session = requests.Session()

        # 本机服务禁止继承 MoviePilot / 系统代理，
        # 避免 127.0.0.1 请求被发往代理服务器。
        self._session.trust_env = False

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.base_url)

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
        
    def search(
        self,
        mediainfo: MediaInfo,
        media_type: MediaType,
        season: Optional[int] = None,
        episodes: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        查询 AYCLUB 机器人资源。

        last_status 可能值：
        - ok_matched：成功找到有效资源
        - ok_empty：查询成功，但确实没有匹配资源
        - invalid_result：接口称已匹配，但没有可用资源项
        - timeout / http_error / error：查询失败
        - disabled / invalid_request：未执行有效查询
        """
        self.last_status = "attempted"
        self.last_error = ""

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
        }

        try:
            logger.info(
                f"使用 AYCLUB 查询：{mediainfo.title} "
                f"(TMDB ID: {mediainfo.tmdb_id}, "
                f"类型: {payload['media_type']}, "
                f"季: {season}, "
                f"集: {payload['episodes']})"
            )

            response = self._session.post(
                f"{self.base_url}/search",
                json=payload,
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

            if not data.get("matched"):
                self.last_status = "ok_empty"
                logger.info(
                    f"AYCLUB 查询成功但未找到资源：{mediainfo.title} "
                    f"(TMDB ID: {mediainfo.tmdb_id})"
                )
                return []

            results: List[Dict[str, Any]] = []

            for item in data.get("matches") or []:
                share_url = (item.get("source") or "").strip()
                resource_title = (item.get("title") or "").strip()

                if not share_url or not resource_title:
                    continue

                results.append({
                    "url": share_url,
                    "title": resource_title,
                    "update_time": "",
                    "source": "ayclub",
                    "tmdb_id": item.get("tmdb_id"),
                    "year": item.get("year"),
                    "season": item.get("season"),
                    "episode": item.get("episode"),
                    "title_match": item.get("title_match"),
                    "year_match": item.get("year_match"),
                })

            if results:
                self.last_status = "ok_matched"
            else:
                self.last_status = "invalid_result"
                self.last_error = (
                    "桥接服务返回 matched=true，"
                    "但没有包含有效链接和标题的资源项"
                )

            logger.info(
                f"AYCLUB 找到 {len(results)} 个精确匹配的 "
                f"115cdn 资源，扫描页数："
                f"{data.get('pages_scanned', 0)}，"
                f"状态：{self.last_status}"
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
            
