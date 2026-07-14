"""
订阅处理模块
负责订阅状态检查、完成、站点更新等逻辑（v1.2.5）
"""
from typing import List, Callable, Dict, Any
from sqlalchemy import text

from app.chain.subscribe import SubscribeChain
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.models.site import Site
from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType


class SubscribeHandler:
    """订阅处理器"""

    def __init__(
        self,
        exclude_subscribes: List[int] = None,
        notify: bool = False,
        post_message_func: Callable = None,
        is_excluded_func: Callable[[int], bool] = None
    ):
        """
        :param exclude_subscribes: 排除的订阅ID列表（is_excluded_func 未提供时使用）
        :param is_excluded_func: 订阅过滤判断函数，支持排除/指定两种模式
        """
        self._exclude_subscribes = exclude_subscribes or []
        self._notify = notify
        self._post_message = post_message_func
        self._is_excluded_func = is_excluded_func

    def _is_excluded(self, subscribe_id: int) -> bool:
        """判断订阅是否不归本插件处理"""
        if self._is_excluded_func:
            return bool(self._is_excluded_func(subscribe_id))
        return subscribe_id in set(self._exclude_subscribes or [])

    # ------------------ 订阅完成逻辑（完整保留） ------------------

    def check_and_finish_subscribe(
        self,
        subscribe,
        mediainfo: MediaInfo,
        success_episodes: List[int]
    ):
        """
        兼容旧调用的安全入口。

        1.8.0 起不再根据插件转存结果直接写 note/lack_episode，也不 force 完成订阅。
        仅请求 MoviePilot 使用自身媒体库口径刷新电视剧进度；电影由后续 MP 入库事件
        和全量对账完成。
        """
        try:
            if getattr(subscribe, "type", None) == MediaType.TV.value:
                result = SubscribeChain().refresh_subscribe_progress(
                    subscribe=subscribe,
                    scene="p115_plugin_compat",
                )
                logger.info(
                    f"已交由 MoviePilot 刷新订阅 {getattr(subscribe, 'id', None)} 进度：{result}"
                )
            else:
                logger.info(
                    f"电影订阅 {getattr(subscribe, 'id', None)} 等待 MoviePilot 入库/完成事件"
                )
        except Exception as error:
            logger.warning(
                f"请求 MoviePilot 刷新订阅 {getattr(subscribe, 'id', None)} 失败：{error}"
            )

    # ------------------ 站点写入增强 ------------------

    @staticmethod
    def _normalize_site_names(site_names: List[str]) -> List[str]:
        """标准化站点名称列表（去重并保持顺序）"""
        if not site_names:
            return []
        # 使用 dict.fromkeys 保持顺序的同时去重
        cleaned = (str(x).strip() for x in site_names if x is not None)
        return list(dict.fromkeys(s for s in cleaned if s))

    @staticmethod
    def _get_site_ids_by_names(db, site_names: List[str]) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for name in site_names:
            row = db.execute(text("SELECT id FROM site WHERE name=:name LIMIT 1"), {"name": name}).fetchone()
            if row and row[0] is not None:
                mapping[name] = int(row[0])
            else:
                logger.warning(f"未找到站点记录：name={name}（将跳过）")
        return mapping

    @staticmethod
    def _ensure_115_site_id(db) -> int:
        row = db.execute(text("SELECT id FROM site WHERE name=:name LIMIT 1"), {"name": "115网盘"}).fetchone()
        if row and row[0] is not None:
            return int(row[0])

        # existing = Site.get(db, -1)
        row_ex = db.execute(text("SELECT id FROM site WHERE id=:i"), {"i": -1}).fetchone()
        if not row_ex:
            db.execute(
                text(
                    "INSERT INTO site (id, name, url, is_active, limit_interval, limit_count, limit_seconds, timeout) "
                    "VALUES (:id,:name,:url,:is_active,:limit_interval,:limit_count,:limit_seconds,:timeout)"
                ),
                {
                    "id": -1, "name": "115网盘", "url": "https://115.com", "is_active": True,
                    "limit_interval": 10000000, "limit_count": 1, "limit_seconds": 10000000, "timeout": 1
                }
            )
            db.commit()
            logger.info("已添加站点记录：115网盘(id=-1)")
        return -1

    @staticmethod
    def _guess_sites_storage_format_from_rows(rows: List[Any]) -> str:
        for v in rows:
            if isinstance(v, str):
                return "str"
            if isinstance(v, list):
                return "list"
        return "list"

    @staticmethod
    def _guess_sites_storage_format_for_subscribe(db, subscribe_id: int) -> str:
        """
        通过 SubscribeOper 获取订阅对象来判断 sites 字段存储格式
        使用 ORM 层可以正确处理 SQLite 中 JSON 字段的类型转换
        """
        subscribe = SubscribeOper(db=db).get(int(subscribe_id))
        if not subscribe:
            return "list"
        sites = getattr(subscribe, "sites", None)
        if isinstance(sites, str):
            return "str"
        if isinstance(sites, list):
            return "list"
        return "list"

    def apply_subscribe_sites_by_site_names(self, site_names: List[str], action_desc: str = "") -> List[int]:
        action_desc = action_desc or f"设置订阅sites={site_names}"
        site_names_norm = self._normalize_site_names(site_names)

        if not site_names_norm:
            logger.warning(f"{action_desc}：站点列表为空，跳过")
            return []

        with SessionFactory() as db:
            mapping = self._get_site_ids_by_names(db, site_names_norm)
            site_ids = []
            for nm in site_names_norm:
                if nm in mapping:
                    site_ids.append(mapping[nm])

            seen = set()
            site_ids_uniq = []
            for x in site_ids:
                if x in seen:
                    continue
                seen.add(x)
                site_ids_uniq.append(x)

            logger.info(f"{action_desc}：站点映射 name->id = {mapping}")
            logger.info(f"{action_desc}：最终写入 sites = {site_ids_uniq}")

            if not site_ids_uniq:
                logger.warning(f"{action_desc}：未解析到有效站点ID，跳过写入（保持原状）")
                return []

            # 复用 SubscribeOper 实例，避免循环中重复创建
            subscribe_oper = SubscribeOper(db=db)
            subscribes = subscribe_oper.list() or []
            sample_sites = []
            for s in subscribes[:5]:
                try:
                    sample_sites.append(getattr(s, "sites", None))
                except Exception:
                    pass
            storage = self._guess_sites_storage_format_from_rows(sample_sites)

            updated, excluded = 0, 0
            for s in subscribes:
                if self._is_excluded(s.id):
                    excluded += 1
                    continue
                value = ",".join(str(x) for x in site_ids_uniq) if storage == "str" else site_ids_uniq
                subscribe_oper.update(s.id, {"sites": value})
                updated += 1

            logger.info(f"{action_desc}：已更新 {updated} 个订阅（跳过 {excluded} 个排除订阅）")
            return site_ids_uniq

    def set_unblocked_sites(self, unblocked_site_names: List[str]) -> List[int]:
        return self.apply_subscribe_sites_by_site_names(
            unblocked_site_names,
            action_desc="已恢复系统订阅：全量订阅站点同步"
        )

    def set_blocked_sites_only_115(self) -> List[int]:
        with SessionFactory() as db:
            site_id_115 = self._ensure_115_site_id(db)

            # 复用 SubscribeOper 实例，避免循环中重复创建
            subscribe_oper = SubscribeOper(db=db)
            subscribes = subscribe_oper.list() or []
            sample_sites = []
            for s in subscribes[:5]:
                try:
                    sample_sites.append(getattr(s, "sites", None))
                except Exception:
                    pass
            storage = self._guess_sites_storage_format_from_rows(sample_sites)

            updated, excluded = 0, 0
            for s in subscribes:
                if self._is_excluded(s.id):
                    excluded += 1
                    continue
                value = str(site_id_115) if storage == "str" else [site_id_115]
                subscribe_oper.update(s.id, {"sites": value})
                updated += 1

            logger.info(f"已屏蔽系统订阅：全量订阅仅115网盘（已更新 {updated} 个，跳过 {excluded} 个排除订阅）")
            return [site_id_115]

    # ------------------ 新增订阅站点写入（事件兜底用） ------------------

    def set_sites_for_subscribe_only_115(self, subscribe_id: int) -> List[int]:
        """
        新增订阅写入：仅115
        - v1.2.5：仅用于 SubscribeAdded（新订阅兜底）
        """
        with SessionFactory() as db:
            site_id_115 = self._ensure_115_site_id(db)
            storage = self._guess_sites_storage_format_for_subscribe(db, int(subscribe_id))
            value = str(site_id_115) if storage == "str" else [site_id_115]
            SubscribeOper(db=db).update(int(subscribe_id), {"sites": value})
            logger.info(f"已屏蔽系统订阅：检测到新增订阅，准备拉回仅115（subscribe_id={subscribe_id}）")
            return [site_id_115]

    def set_sites_for_subscribe_by_names(self, subscribe_id: int, site_names: List[str]) -> List[int]:
        """
        新增订阅写入：窗口站点
        - 用于“已恢复系统订阅”状态下，新订阅保持一致
        """
        site_names_norm = self._normalize_site_names(site_names)
        if not site_names_norm:
            logger.warning(f"已恢复系统订阅：新增订阅站点列表为空（subscribe_id={subscribe_id}），跳过")
            return []

        with SessionFactory() as db:
            mapping = self._get_site_ids_by_names(db, site_names_norm)
            site_ids = []
            for nm in site_names_norm:
                if nm in mapping:
                    site_ids.append(mapping[nm])

            seen = set()
            site_ids_uniq = []
            for x in site_ids:
                if x in seen:
                    continue
                seen.add(x)
                site_ids_uniq.append(x)

            if not site_ids_uniq:
                logger.warning(f"已恢复系统订阅：新增订阅未解析到站点ID（subscribe_id={subscribe_id}），跳过")
                return []

            storage = self._guess_sites_storage_format_for_subscribe(db, int(subscribe_id))
            value = ",".join(str(x) for x in site_ids_uniq) if storage == "str" else site_ids_uniq
            SubscribeOper(db=db).update(int(subscribe_id), {"sites": value})
            logger.info(f"已恢复系统订阅：新增订阅已同步窗口站点（subscribe_id={subscribe_id}）")
            return site_ids_uniq
