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
    ) -> List[Dict[str, Any]]:
        """
        查询 AYCLUB 机器人资源。

        返回统一搜索结果格式：
        [
            {
                "url": "https://115cdn.com/...",
                "title": "...",
                "update_time": "",
                ...
            }
        ]
        """
        if not self.is_ready:
            return []

        if not mediainfo or not mediainfo.tmdb_id:
            title = getattr(mediainfo, "title", "未知媒体")
            logger.warning(
                f"{title} 缺少 TMDB ID，无法使用 AYCLUB 精确查询"
            )
            return []

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
            "episodes": [],
            "max_pages": self.max_pages,
        }

        try:
            logger.info(
                f"使用 AYCLUB 查询：{mediainfo.title} "
                f"(TMDB ID: {mediainfo.tmdb_id}, "
                f"类型: {payload['media_type']}, "
                f"季: {season})"
            )

            response = self._session.post(
                f"{self.base_url}/search",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()

            data = response.json()

            if not data.get("matched"):
                logger.info(
                    f"AYCLUB 未找到资源：{mediainfo.title} "
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

            logger.info(
                f"AYCLUB 找到 {len(results)} 个精确匹配的 "
                f"115cdn 资源，扫描页数："
                f"{data.get('pages_scanned', 0)}"
            )

            return results

        except requests.Timeout:
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

            logger.error(
                f"AYCLUB 查询失败：HTTP "
                f"{getattr(error.response, 'status_code', '?')}，"
                f"响应={body}"
            )
            return []

        except Exception as error:
            logger.error(
                f"AYCLUB 查询异常："
                f"{type(error).__name__}: {error}"
            )
            return []
