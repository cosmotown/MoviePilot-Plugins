"""
OpenClaw 115 七分类服务客户端。

115 分享只调用 inspect；ED2K 使用同一认证端点执行提交。
本客户端不轮询下载，也不移动下载文件。
"""
import hashlib
from typing import Any, Dict, Iterable, Optional

import requests

from app.log import logger


class OpenClawClassifierClient:
    """调用 p115-openclaw 分类接口。"""

    MOVIE_CATEGORIES = {
        "电影",
        "特摄剧场版",
        "原盘电影",
        "原盘动画电影",
    }

    TV_CATEGORIES = {
        "电视剧",
        "动漫",
        "特摄剧",
    }

    def __init__(
        self,
        base_url: str = "",
        token: str = "",
        enabled: bool = False,
        timeout: int = 120,
    ):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.token = (token or "").strip()
        self.enabled = bool(enabled)
        self.timeout = max(15, int(timeout or 120))

        self._session = requests.Session()
        # 内网分类服务不走系统代理，避免 Fake-IP / HTTP 代理干扰。
        self._session.trust_env = False

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.base_url and self.token)


    @staticmethod
    def _is_ed2k_url(source_url: str) -> bool:
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

    @staticmethod
    def _ed2k_ref(source_url: str) -> str:
        return hashlib.sha256(
            (source_url or "").encode("utf-8")
        ).hexdigest()[:16]

    def submit_ed2k(
        self,
        source_url: str,
        media_type: str,
        title: str,
        year: Optional[int] = None,
        tmdb_id: Optional[int] = None,
        season: Optional[int] = None,
        episodes: Optional[Iterable[int]] = None,
        resource_title: str = "",
        request_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        把原始 ED2K 交给 OpenClaw/p115 后端执行。

        本客户端只提交，不轮询下载、不移动文件。
        """
        source_url = (source_url or "").strip()
        source_ref = self._ed2k_ref(source_url)

        if not self.is_ready:
            logger.warning("OpenClaw 分类/执行服务未启用或配置不完整")
            return None

        if not self._is_ed2k_url(source_url):
            logger.warning(f"拒绝提交格式不完整的 ED2K：ref={source_ref}")
            return None

        hints = [
            f"title: {title}",
            f"media_type: {media_type}",
        ]
        if year:
            hints.append(f"year: {year}")
        if tmdb_id:
            hints.append(f"tmdbid={tmdb_id}")
        if season is not None:
            hints.append(f"season: S{int(season):02d}")
        normalized_episodes = []
        for value in episodes or []:
            try:
                episode = int(value)
            except (TypeError, ValueError):
                continue
            if episode > 0 and episode not in normalized_episodes:
                normalized_episodes.append(episode)
        if normalized_episodes:
            hints.append(
                "episodes: " + ",".join(
                    f"E{episode:02d}" for episode in sorted(normalized_episodes)
                )
            )
        if resource_title:
            hints.append(f"resource_title: {resource_title}")

        try:
            response = self._session.post(
                f"{self.base_url}/api/openclaw/process",
                headers={
                    "X-OpenClaw-Token": self.token,
                    "Content-Type": "application/json",
                },
                json={
                    "source": source_url,
                    "execute": True,
                    "hint": "\n".join(hints),
                    "contract_version": 2,
                    "request_id": str(request_id or source_ref),
                    "media_type": str(media_type or "").strip().casefold(),
                    "title": str(title or "").strip(),
                    "year": int(year) if year else None,
                    "tmdb_id": int(tmdb_id) if tmdb_id else None,
                    "season": int(season) if season is not None else None,
                    "episodes": sorted(normalized_episodes),
                    "resource_title": str(resource_title or "")[:1000],
                },
                timeout=(10, self.timeout),
            )
            response.raise_for_status()
            data = response.json() if response.content else {}
        except Exception as exc:
            response_obj = getattr(exc, "response", None)
            status_code = getattr(response_obj, "status_code", None)
            suffix = f"，HTTP={status_code}" if status_code is not None else ""
            logger.error(
                f"OpenClaw ED2K 提交失败：ref={source_ref}，"
                f"错误类型={type(exc).__name__}{suffix}"
            )
            return None

        if not isinstance(data, dict) or data.get("ok") is False:
            # 不输出后端 message/error，防止其回显原始 ED2K。
            logger.warning(f"OpenClaw 拒绝 ED2K：ref={source_ref}")
            return None

        # Contract v2 is mandatory. A generic HTTP 2xx or legacy
        # ``already_queued`` response is not enough to create pending state.
        accepted_statuses = {
            "queued",
            "started",
            "processing",
            "downloading",
            "already-queued",
            "already_queued",
        }

        def iter_status_objects(value):
            if isinstance(value, dict):
                yield value
                for key in ("items", "result", "data", "job", "task", "payload"):
                    child = value.get(key)
                    if isinstance(child, list):
                        for item in child:
                            yield from iter_status_objects(item)
                    elif isinstance(child, dict):
                        yield from iter_status_objects(child)
            elif isinstance(value, list):
                for item in value:
                    yield from iter_status_objects(item)

        expected_request_id = str(request_id or source_ref)
        expected_episodes = sorted(normalized_episodes)
        accepted_item = None
        observed_statuses = []
        for item in iter_status_objects(data):
            status = str(item.get("status") or "").strip().casefold()
            if status and status not in observed_statuses:
                observed_statuses.append(status)
            if status not in accepted_statuses:
                continue
            try:
                contract_version = int(item.get("contract_version") or 0)
            except (TypeError, ValueError):
                contract_version = 0
            echoed_episodes = []
            for value in item.get("expected_episodes") or []:
                try:
                    episode = int(value)
                except (TypeError, ValueError):
                    continue
                if episode > 0 and episode not in echoed_episodes:
                    echoed_episodes.append(episode)
            echoed_episodes.sort()
            echoed_season = item.get("expected_season")
            try:
                echoed_season = int(echoed_season) if echoed_season is not None else None
            except (TypeError, ValueError):
                echoed_season = None

            if (
                contract_version == 2
                and item.get("verified") is True
                and str(item.get("request_id") or "") == expected_request_id
                and echoed_episodes == expected_episodes
                and echoed_season == (int(season) if season is not None else None)
            ):
                accepted_item = item
                break

        if not accepted_item:
            safe_statuses = ",".join(observed_statuses[:8]) or "none"
            logger.warning(
                f"OpenClaw ED2K 响应未通过 contract v2 回显校验，拒绝登记在途："
                f"ref={source_ref}，statuses={safe_statuses}"
            )
            return None

        explicit_status = str(accepted_item.get("status") or "").strip().casefold()
        logger.info(
            f"OpenClaw contract v2 已确认活动 ED2K 任务："
            f"ref={source_ref}，status={explicit_status}"
        )
        return {
            "ok": True,
            "status": explicit_status,
            "contract_version": 2,
            "job_id": str(accepted_item.get("job_id") or ""),
            "request_id": expected_request_id,
        }


    def inspect_share(
        self,
        share_url: str,
        media_type: str,
        title: str,
        year: Optional[int] = None,
        tmdb_id: Optional[int] = None,
        season: Optional[int] = None,
        resource_title: str = "",
        file_names: Optional[Iterable[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        检查分享并返回分类结果。

        返回 None 表示分类失败、置信度不足、类型冲突或服务不可用。
        """
        if not self.is_ready:
            logger.warning("OpenClaw 分类服务未启用或配置不完整")
            return None

        hints = [
            f"title: {title}",
            f"media_type: {media_type}",
        ]

        if year:
            hints.append(f"year: {year}")
        if tmdb_id:
            hints.append(f"tmdbid={tmdb_id}")
        if season:
            hints.append(f"season: S{int(season):02d}")
        if resource_title:
            hints.append(f"resource_title: {resource_title}")

        for name in list(file_names or [])[:20]:
            if name:
                hints.append(f"file: {name}")

        try:
            response = self._session.post(
                f"{self.base_url}/api/openclaw/process",
                headers={
                    "X-OpenClaw-Token": self.token,
                    "Content-Type": "application/json",
                },
                json={
                    "source": share_url,
                    "execute": False,
                    "hint": "\n".join(hints),
                },
                timeout=(10, self.timeout),
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.error(f"OpenClaw 分类服务请求失败：{exc}")
            return None

        items = data.get("items") or []
        if not items:
            logger.warning("OpenClaw 分类服务没有返回分类结果")
            return None

        item = items[0] or {}

        if item.get("status") == "error":
            logger.warning(
                f"OpenClaw 分类失败：{item.get('message') or '未知错误'}"
            )
            return None

        category = str(item.get("category") or "").strip()
        confidence = float(item.get("confidence") or 0)
        needs_confirmation = bool(item.get("needs_confirmation"))

        if needs_confirmation:
            logger.warning(
                f"OpenClaw 分类需要人工确认："
                f"category={category}, confidence={confidence:.2f}"
            )
            return None

        allowed_categories = (
            self.MOVIE_CATEGORIES
            if media_type == "movie"
            else self.TV_CATEGORIES
        )

        if category not in allowed_categories:
            logger.warning(
                f"OpenClaw 分类与订阅类型冲突："
                f"media_type={media_type}, category={category}"
            )
            return None

        target_dir = str(item.get("target_dir") or "").strip().replace("\\", "/")
        parts = [part for part in target_dir.strip("/").split("/") if part]
        all_categories = self.MOVIE_CATEGORIES | self.TV_CATEGORIES
        if (
            len(parts) < 2
            or parts[0] != "mp整理"
            or parts[1] not in all_categories
            or any(part in {".", ".."} for part in parts)
        ):
            logger.warning(
                f"OpenClaw 分类结果越界，拒绝转存：target={target_dir!r}"
            )
            return None

        item["target_dir"] = "/" + "/".join(parts)

        logger.info(
            f"OpenClaw 分类完成："
            f"{title} -> {category} "
            f"(confidence={confidence:.2f}, "
            f"target={item['target_dir']})"
        )
        return item
