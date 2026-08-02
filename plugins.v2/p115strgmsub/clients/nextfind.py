"""NextFind intelligent-agent OpenAPI client.

The client only talks to the documented ``/api/openapi`` surface. It never
logs or exports the configured API key and never touches NextFind settings.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from app.log import logger


class NextFindClient:
    """Small, fail-closed client for NextFind subscription handoff."""

    _ACTIVE_FALSE = {
        "cancelled",
        "canceled",
        "removed",
        "deleted",
        "inactive",
        "disabled",
        "已取消",
        "取消",
        "已删除",
        "删除",
        "停用",
    }

    def __init__(
        self,
        base_url: str = "",
        api_key: str = "",
        enabled: bool = False,
        timeout: int = 30,
    ) -> None:
        self.base_url = self._normalize_base_url(base_url)
        self.api_key = str(api_key or "").strip()
        self.enabled = bool(enabled)
        self.timeout = max(5, min(int(timeout or 30), 180))
        self._session = requests.Session()
        # This is normally a LAN endpoint. Do not route it through the global
        # proxy inherited by MoviePilot or the container environment.
        self._session.trust_env = False
        self.last_error: str = ""
        self.last_status_code: Optional[int] = None
        self.last_response: Any = None

    @staticmethod
    def _normalize_base_url(value: str) -> str:
        url = str(value or "").strip().rstrip("/")
        if not url:
            return ""
        try:
            parsed = urlparse(url)
        except ValueError:
            return ""
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        if not url.endswith("/api/openapi"):
            url += "/api/openapi"
        return url

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.base_url and self.api_key)

    def _reset_status(self) -> None:
        self.last_error = ""
        self.last_status_code = None
        self.last_response = None

    @staticmethod
    def _response_success(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return True
        if payload.get("ok") is False:
            return False
        status = str(payload.get("status") or "").strip().casefold()
        if status in {"error", "failed", "failure", "denied"}:
            return False
        success = payload.get("success")
        if success is False:
            return False
        return True

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Any]:
        self._reset_status()
        if not self.is_ready:
            self.last_error = "NextFind 未启用或地址/API Key 未配置完整"
            return False, None

        endpoint = f"{self.base_url}/{str(path or '').lstrip('/')}"
        try:
            response = self._session.request(
                method=method,
                url=endpoint,
                headers={
                    "X-API-Key": self.api_key,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                params=params,
                json=json,
                timeout=self.timeout,
            )
            self.last_status_code = int(response.status_code)
            try:
                payload: Any = response.json() if response.content else {}
            except ValueError:
                payload = {
                    "message": (response.text or "")[:300],
                }
            self.last_response = payload
            if not 200 <= response.status_code < 300:
                self.last_error = f"HTTP {response.status_code}"
                logger.warning(
                    f"NextFind OpenAPI 请求失败：{method.upper()} {path}，"
                    f"HTTP={response.status_code}"
                )
                return False, payload
            if not self._response_success(payload):
                self.last_error = "NextFind 返回失败状态"
                logger.warning(
                    f"NextFind OpenAPI 返回失败：{method.upper()} {path}"
                )
                return False, payload
            return True, payload
        except requests.Timeout:
            self.last_error = "请求超时"
            logger.warning(
                f"NextFind OpenAPI 请求超时：{method.upper()} {path}"
            )
        except requests.RequestException as error:
            self.last_error = type(error).__name__
            logger.warning(
                f"NextFind OpenAPI 请求异常：{method.upper()} {path}，"
                f"类型={type(error).__name__}"
            )
        except Exception as error:
            self.last_error = type(error).__name__
            logger.warning(
                f"NextFind OpenAPI 未知异常：{method.upper()} {path}，"
                f"类型={type(error).__name__}"
            )
        return False, None

    @classmethod
    def _extract_items(cls, payload: Any) -> List[Dict[str, Any]]:
        """Extract a subscription list from known and nested response shapes."""
        queue: List[Any] = [payload]
        seen: set[int] = set()
        preferred_keys = (
            "subscriptions",
            "items",
            "results",
            "list",
            "records",
            "data",
        )
        while queue:
            current = queue.pop(0)
            if id(current) in seen:
                continue
            seen.add(id(current))
            if isinstance(current, list):
                dict_items = [item for item in current if isinstance(item, dict)]
                if dict_items:
                    return dict_items
                continue
            if not isinstance(current, dict):
                continue
            # A single subscription object is also accepted.
            if cls.subscription_tmdb_id(current) is not None:
                return [current]
            queued_ids = set()
            for key in preferred_keys:
                value = current.get(key)
                if isinstance(value, (dict, list)):
                    queue.append(value)
                    queued_ids.add(id(value))
            # Some builds return a mapping keyed by TMDB/subscription ID.
            for value in current.values():
                if (
                    isinstance(value, (dict, list))
                    and id(value) not in queued_ids
                ):
                    queue.append(value)
        return []

    @staticmethod
    def _nested_values(item: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        yield item
        for key in ("media", "subscription", "item", "info", "metadata"):
            value = item.get(key)
            if isinstance(value, dict):
                yield value

    @classmethod
    def subscription_tmdb_id(cls, item: Dict[str, Any]) -> Optional[int]:
        for source in cls._nested_values(item):
            for key in ("tmdb_id", "tmdbid", "tmdbId", "tmdb"):
                value = source.get(key)
                try:
                    number = int(value)
                except (TypeError, ValueError):
                    continue
                if number > 0:
                    return number
        return None

    @classmethod
    def subscription_media_type(cls, item: Dict[str, Any]) -> str:
        for source in cls._nested_values(item):
            for key in ("media_type", "type", "mediaType"):
                value = str(source.get(key) or "").strip().casefold()
                if value in {"movie", "电影", "film"}:
                    return "movie"
                if value in {"tv", "series", "show", "剧集", "电视剧"}:
                    return "tv"
        return ""

    @classmethod
    def subscription_status(cls, item: Dict[str, Any]) -> str:
        for source in cls._nested_values(item):
            for key in ("status", "state", "subscription_status"):
                value = str(source.get(key) or "").strip().casefold()
                if value:
                    return value
        active = item.get("active")
        if active is False:
            return "inactive"
        return "active"

    @classmethod
    def is_active_subscription(cls, item: Dict[str, Any]) -> bool:
        if item.get("active") is False or item.get("enabled") is False:
            return False
        return cls.subscription_status(item) not in cls._ACTIVE_FALSE

    def list_subscriptions(self) -> Tuple[bool, List[Dict[str, Any]]]:
        ok, payload = self._request("GET", "/subscriptions")
        if not ok:
            return False, []
        return True, self._extract_items(payload)

    def find_subscription(
        self,
        *,
        tmdb_id: int,
        media_type: str,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ok, items = self.list_subscriptions()
        if not ok:
            return False, None
        wanted_type = "tv" if str(media_type).casefold() == "tv" else "movie"
        for item in items:
            if self.subscription_tmdb_id(item) != int(tmdb_id):
                continue
            item_type = self.subscription_media_type(item)
            if item_type and item_type != wanted_type:
                continue
            if self.is_active_subscription(item):
                return True, item
        return True, None

    def add_subscription(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        title: str,
    ) -> bool:
        ok, _ = self._request(
            "POST",
            "/subscriptions/add",
            json={
                "tmdb_id": str(int(tmdb_id)),
                "media_type": "tv" if str(media_type).casefold() == "tv" else "movie",
                "title": str(title or "").strip(),
            },
        )
        return ok

    def remove_subscription(
        self,
        *,
        tmdb_id: int,
        media_type: str,
    ) -> bool:
        ok, _ = self._request(
            "POST",
            "/subscriptions/remove",
            json={
                "tmdb_id": str(int(tmdb_id)),
                "media_type": "tv" if str(media_type).casefold() == "tv" else "movie",
            },
        )
        return ok

    def fill_missing(
        self,
        *,
        tmdb_id: int,
        media_type: str,
        title: str,
    ) -> bool:
        ok, _ = self._request(
            "POST",
            "/media/fill_missing",
            json={
                "tmdb_id": str(int(tmdb_id)),
                "media_type": "tv" if str(media_type).casefold() == "tv" else "movie",
                "title": str(title or "").strip(),
            },
        )
        return ok

    def subscription_info(
        self,
        *,
        tmdb_id: int,
        media_type: str,
    ) -> Tuple[bool, Any]:
        return self._request(
            "POST",
            "/subscriptions/info",
            json={
                "items": [{
                    "tmdb_id": str(int(tmdb_id)),
                    "media_type": (
                        "tv" if str(media_type).casefold() == "tv" else "movie"
                    ),
                }]
            },
        )
