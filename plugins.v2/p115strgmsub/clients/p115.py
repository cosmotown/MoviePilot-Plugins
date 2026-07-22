"""Isolated 115 transfer client backed by the p115-openclaw sidecar.

This MoviePilot plugin intentionally does not import :mod:`p115client`.
All 115 API calls run inside the independent ``p115-openclaw`` container so
P115StrgmSub cannot upgrade or replace the p115client used by P115Disk or
P115StrmHelper in MoviePilot's shared Python environment.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from app.log import logger


@dataclass
class ShareLinkStatus:
    is_valid: bool = False
    is_expired: bool = False
    is_cancelled: bool = False
    is_deleted: bool = False
    error_code: int = 0
    error_message: str = ""
    file_count: int = 0
    share_info: Dict[str, Any] = field(default_factory=dict)

    @property
    def status_text(self) -> str:
        if self.is_valid:
            return "有效"
        if self.is_expired:
            return "已过期"
        if self.is_cancelled:
            return "已取消"
        if self.is_deleted:
            return "文件已删除"
        return self.error_message or "未知状态"


class P115ClientManager:
    """Compatibility facade for the old in-process manager.

    The public methods deliberately match the former manager so the search and
    episode-selection logic stays unchanged. The implementation is HTTP-only.
    """

    CACHE_TTL_SECONDS = 60

    def __init__(
        self,
        cookies: str = "",
        *,
        base_url: str = "",
        token: str = "",
        timeout: int = 120,
        **_ignored: Any,
    ):
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.token = str(token or "").strip()
        self.timeout = max(15, int(timeout or 120))
        self._api_call_count = 0
        self._inspect_cache: Dict[Tuple[str, int, Optional[int]], Tuple[float, dict]] = {}
        self._health_cache: Tuple[float, dict] | None = None
        self._session = requests.Session()
        self._session.trust_env = False
        if cookies:
            logger.info("P115StrgmSub 1.9.5 起不再读取插件 Cookie；115 操作由独立 OpenClaw 后端执行")

    @property
    def is_ready(self) -> bool:
        return bool(self.base_url and self.token)

    def _request(self, path: str, *, payload: Optional[dict] = None, method: str = "POST") -> dict:
        if not self.is_ready:
            raise RuntimeError("OpenClaw 115 执行服务地址或 Token 未配置")
        self._api_call_count += 1
        url = f"{self.base_url}/api/openclaw/{path.lstrip('/')}"
        response = self._session.request(
            method,
            url,
            headers={
                "X-OpenClaw-Token": self.token,
                "Content-Type": "application/json",
            },
            json=payload if method.upper() != "GET" else None,
            timeout=(10, self.timeout),
        )
        response.raise_for_status()
        data = response.json() if response.content else {}
        if not isinstance(data, dict):
            raise RuntimeError("OpenClaw 115 执行服务返回格式无效")
        return data

    def _health(self, *, force: bool = False) -> dict:
        now = time.time()
        if not force and self._health_cache and now - self._health_cache[0] < 30:
            return self._health_cache[1]
        data = self._request("health", method="GET")
        self._health_cache = (now, data)
        return data

    def check_login(self) -> bool:
        try:
            data = self._health(force=True)
            if int(data.get("selective_transfer_api_version") or 0) < 1:
                logger.error("p115-openclaw 版本过旧，缺少选择性转存 API；请升级到 1.0.94+")
                return False
            if not data.get("connected"):
                logger.error("p115-openclaw 当前未连接 115 账号")
                return False
            return True
        except Exception as exc:
            logger.error(f"OpenClaw 115 执行服务自检失败：{type(exc).__name__}: {exc}")
            return False

    def _inspect(
        self,
        share_url: str,
        *,
        max_depth: int = 3,
        target_season: Optional[int] = None,
        force: bool = False,
    ) -> dict:
        key = (str(share_url), int(max_depth), int(target_season) if target_season else None)
        now = time.time()
        cached = self._inspect_cache.get(key)
        if not force and cached and now - cached[0] < self.CACHE_TTL_SECONDS:
            return cached[1]
        data = self._request(
            "share/inspect",
            payload={
                "source": share_url,
                "max_depth": max(1, min(int(max_depth), 6)),
                "target_season": int(target_season) if target_season else None,
            },
        )
        self._inspect_cache[key] = (now, data)
        return data

    @staticmethod
    def _status_from_dict(raw: Any) -> ShareLinkStatus:
        raw = raw if isinstance(raw, dict) else {}
        try:
            error_code = int(raw.get("error_code") or 0)
        except (TypeError, ValueError):
            error_code = -1
        try:
            file_count = int(raw.get("file_count") or 0)
        except (TypeError, ValueError):
            file_count = 0
        return ShareLinkStatus(
            is_valid=bool(raw.get("is_valid")),
            is_expired=bool(raw.get("is_expired")),
            is_cancelled=bool(raw.get("is_cancelled")),
            is_deleted=bool(raw.get("is_deleted")),
            error_code=error_code,
            error_message=str(raw.get("error_message") or ""),
            file_count=file_count,
            share_info=dict(raw.get("share_info") or {}),
        )

    def check_share_status(self, share_url: str) -> ShareLinkStatus:
        try:
            data = self._inspect(share_url, max_depth=1)
            return self._status_from_dict(data.get("status"))
        except Exception as exc:
            return ShareLinkStatus(
                error_code=-1,
                error_message=f"OpenClaw 分享检查失败: {type(exc).__name__}",
            )

    def is_share_valid(self, share_url: str) -> bool:
        return self.check_share_status(share_url).is_valid

    def list_share_files(
        self,
        share_url: str,
        cid: int = 0,
        max_depth: int = 3,
        target_season: Optional[int] = None,
    ) -> List[dict]:
        if cid not in (0, None):
            logger.warning("远程 115 管理器不支持从任意分享 CID 开始遍历，改为读取分享根目录")
        try:
            data = self._inspect(
                share_url,
                max_depth=max_depth,
                target_season=target_season,
            )
            status = self._status_from_dict(data.get("status"))
            if not status.is_valid:
                return []
            files = data.get("files") or []
            return files if isinstance(files, list) else []
        except Exception as exc:
            logger.error(f"通过 OpenClaw 列出分享文件失败：{type(exc).__name__}: {exc}")
            return []

    @staticmethod
    def _collect_top_ids(items: Iterable[dict]) -> List[str]:
        result: List[str] = []
        for item in items or []:
            item_id = str((item or {}).get("id") or "").strip()
            if item_id and item_id not in result:
                result.append(item_id)
        return result

    def transfer_share(self, share_url: str, save_path: str) -> bool:
        files = self.list_share_files(share_url, max_depth=1)
        file_ids = self._collect_top_ids(files)
        if not file_ids:
            return False
        success_ids, failed_ids = self.transfer_files_batch(
            share_url=share_url,
            file_ids=file_ids,
            save_path=save_path,
            batch_size=20,
        )
        return bool(success_ids) and not failed_ids

    def transfer_file(self, share_url: str, file_id: str, save_path: str) -> bool:
        success_ids, failed_ids = self.transfer_files_batch(
            share_url=share_url,
            file_ids=[str(file_id)],
            save_path=save_path,
            batch_size=1,
        )
        return bool(success_ids) and not failed_ids

    def transfer_files_batch(
        self,
        share_url: str,
        file_ids: List[str],
        save_path: str,
        batch_size: int = 20,
        batch_interval: float = 3.0,
    ) -> Tuple[List[str], List[str]]:
        del batch_interval  # Backend queue owns pacing/rate limiting.
        normalized: List[str] = []
        for value in file_ids or []:
            item_id = str(value or "").strip()
            if item_id and item_id not in normalized:
                normalized.append(item_id)
        if not normalized:
            return [], []
        try:
            data = self._request(
                "share/transfer-selected",
                payload={
                    "source": share_url,
                    "file_ids": normalized,
                    "target_dir": save_path,
                    "batch_size": max(1, min(int(batch_size or 20), 100)),
                },
            )
            success_ids = [str(value) for value in data.get("success_ids") or []]
            failed_ids = [str(value) for value in data.get("failed_ids") or []]
            self.clear_share_cache()
            return success_ids, failed_ids
        except Exception as exc:
            logger.error(f"OpenClaw 选择性转存失败：{type(exc).__name__}: {exc}")
            return [], normalized

    def list_directories(self, path: str) -> List[dict]:
        """Expose only the fixed dispatch root/categories, never arbitrary 115 paths."""
        try:
            health = self._health()
        except Exception:
            return []
        root = str(health.get("root_dir") or "mp整理").strip("/\\") or "mp整理"
        categories = [str(value) for value in health.get("categories") or []]
        normalized = "/" + str(path or "/").strip("/\\") if str(path or "/").strip("/\\") else "/"
        if normalized == "/":
            return [{"name": root, "path": f"/{root}", "cid": 0}]
        if normalized == f"/{root}":
            return [
                {"name": category, "path": f"/{root}/{category}", "cid": 0}
                for category in categories
            ]
        return []

    def clear_path_cache(self):
        self._health_cache = None

    def clear_share_cache(self):
        self._inspect_cache.clear()

    def get_api_call_count(self) -> int:
        return self._api_call_count

    def reset_api_call_count(self):
        self._api_call_count = 0
