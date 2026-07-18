"""SyncHandler safety overrides.

Keep resource delivery available when the optional OpenClaw classifier is
unavailable, uncertain, or returns an unusable target directory. The
classifier selects a more specific staging directory; it must never become a
hard prerequisite for a valid, TMDB-matched transfer.
"""
from typing import List, Optional

from app.log import logger

from .sync import SyncHandler as _BaseSyncHandler


class SyncHandler(_BaseSyncHandler):
    """Add fail-open directory fallback to the normal sync handler."""

    @staticmethod
    def _normalize_root(path: str) -> str:
        value = str(path or "").strip()
        if not value:
            return ""
        return "/" + value.strip("/")

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
        """Return a safe staging root even when classification cannot decide."""
        safe_root = self._normalize_root(fallback_root)
        if not safe_root:
            logger.error(
                f"缺少安全转存目录，无法继续转存：{title} - {resource_title}"
            )
            return None

        classifier = self._classifier_client
        if not classifier or not classifier.enabled:
            return safe_root

        if not classifier.is_ready:
            logger.warning(
                f"OpenClaw 分类服务不可用，回退安全目录继续转存："
                f"{title} -> {safe_root}"
            )
            return safe_root

        result = classifier.inspect_share(
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
                f"OpenClaw 分类失败或需要人工确认，回退安全目录继续转存："
                f"{title} - {resource_title} -> {safe_root}"
            )
            return safe_root

        target_root = self._normalize_root(result.get("target_dir"))
        if not target_root:
            logger.warning(
                f"OpenClaw 分类结果缺少目标目录，回退安全目录继续转存："
                f"{title} -> {safe_root}"
            )
            return safe_root

        return target_root
