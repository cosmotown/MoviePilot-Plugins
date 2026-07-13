"""
同步处理模块
负责核心的同步逻辑：处理电影订阅、处理电视剧订阅
"""
import datetime
from typing import List, Dict, Any, Set, Optional, Callable

from app.core.config import global_vars
from app.core.metainfo import MetaInfo
from app.chain.download import DownloadChain
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType, NotificationType
from app.utils.string import StringUtils

from ..utils import FileMatcher, SubscribeFilter
from .search import SearchHandler
from .subscribe import SubscribeHandler
from .release_gate import ReleaseGateStore


class SyncHandler:
    """同步处理器"""

    def __init__(
        self,
        p115_manager,
        search_handler: SearchHandler,
        subscribe_handler: SubscribeHandler,
        chain,
        save_path: str,
        movie_save_path: str,
        classifier_client=None,
        max_transfer_per_sync: int = 50,
        batch_size: int = 20,
        skip_other_season_dirs: bool = True,
        notify: bool = False,
        post_message_func: Callable = None,
        get_data_func: Callable = None,
        save_data_func: Callable = None
    ):
        """
        初始化同步处理器

        :param p115_manager: 115 客户端管理器
        :param search_handler: 搜索处理器
        :param subscribe_handler: 订阅处理器
        :param chain: MediaChain 实例
        :param save_path: 电视剧转存目录
        :param movie_save_path: 电影转存目录
        :param classifier_client: OpenClaw 七分类客户端
        :param max_transfer_per_sync: 单次同步最大转存数量
        :param batch_size: 批量转存每批文件数
        :param skip_other_season_dirs: 跳过其他季目录
        :param notify: 是否发送通知
        :param post_message_func: 发送消息的函数
        :param get_data_func: 获取数据的函数
        :param save_data_func: 保存数据的函数
        """
        self._p115_manager = p115_manager
        self._search_handler = search_handler
        self._subscribe_handler = subscribe_handler
        self._chain = chain
        self._save_path = save_path
        self._movie_save_path = movie_save_path
        self._classifier_client = classifier_client
        self._max_transfer_per_sync = max_transfer_per_sync
        self._batch_size = batch_size
        self._skip_other_season_dirs = skip_other_season_dirs
        self._notify = notify
        self._post_message = post_message_func
        self._get_data = get_data_func
        self._save_data = save_data_func
        self._release_gate = ReleaseGateStore(
            get_data_func=get_data_func,
            save_data_func=save_data_func,
        )

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
        """
        获取七分类目标根目录。

        分类服务未启用时沿用旧目录；
        分类服务已启用但分类失败时返回 None，禁止盲目转存。
        """
        if (
            not self._classifier_client
            or not self._classifier_client.enabled
        ):
            return fallback_root

        if not self._classifier_client.is_ready:
            logger.warning(
                "OpenClaw 分类服务已启用但配置不完整，跳过转存"
            )
            return None

        result = self._classifier_client.inspect_share(
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
                f"分类失败或需要人工确认，跳过转存："
                f"{title} - {resource_title}"
            )
            return None

        return result["target_dir"]

    def process_movie_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int
    ) -> int:
        """
        处理单个电影订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :return: 更新后的转存数量
        """
        try:
            logger.info(f"处理电影订阅：{subscribe.name} ({subscribe.year})")

            # 加载该订阅的历史积分花费（用 tmdb_id 作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_movie" if subscribe.tmdbid else f"{subscribe.name}_movie"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # 检查历史记录是否已成功转存
            movie_history_score = -1  # -1 表示未转存过
            movie_perfect_match = False
            for h in history:
                if (h.get("title") == subscribe.name
                        and h.get("type") == "电影"
                        and h.get("status") == "成功"):
                    score = h.get("filter_score", 0)
                    perfect = h.get("perfect_match", False)
                    if score > movie_history_score:
                        movie_history_score = score
                        movie_perfect_match = perfect

            # best_version=1 表示开启洗版（非严格模式）
            is_best_version = bool(subscribe.best_version)

            if movie_history_score >= 0:
                if not is_best_version or movie_perfect_match:
                    logger.info(f"电影 {subscribe.name} 已在历史记录中(洗版:{is_best_version}, 完美匹配:{movie_perfect_match})，跳过")
                    return transferred_count
                else:
                    logger.info(f"电影 {subscribe.name} 洗版中，历史分数 {movie_history_score}，尝试寻找更优资源")

            # 生成元数据
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.type = MediaType.MOVIE

            # 识别媒体信息
            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.MOVIE,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )
            if not mediainfo:
                logger.warn(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count            

            # 判断本次是否允许查询 AYCLUB。
            # 门禁只控制 AYCLUB，不影响 PanSou、HDHive、Nullbr。
            movie_gate = {
                "allow_ayclub": False,
                "ayclub_first": False,
                "probe_due": False,
                "released": False,
                "reason": "missing_tmdb_id",
            }

            if mediainfo.tmdb_id:
                movie_gate = self._release_gate.evaluate_movie(
                    int(mediainfo.tmdb_id)
                )
            else:
                logger.info(
                    f"电影 {mediainfo.title} 缺少 TMDB ID，"
                    f"本次不查询 AYCLUB"
                )

            logger.info(
                f"电影 {mediainfo.title} AYCLUB 发布门禁："
                f"允许={movie_gate.get('allow_ayclub')}，"
                f"优先={movie_gate.get('ayclub_first')}，"
                f"原因={movie_gate.get('reason')}"
            )

            # 防止读取到上一个订阅遗留的 AYCLUB 查询状态
            self._search_handler.reset_ayclub_status()      
            # 创建订阅过滤条件
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"电影 {subscribe.name} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 延迟逐源搜索：只有当前来源的候选资源全部不可用，
            # 才真正查询下一个来源
            movie_transferred = False
            resource_found = False

            resource_iterator = self._search_handler.iter_resources(
                mediainfo=mediainfo,
                media_type=MediaType.MOVIE,
                ayclub_first=bool(
                    movie_gate.get("ayclub_first")
                ),
                allow_ayclub=bool(
                    movie_gate.get("allow_ayclub")
                ),
            )

            for resource in resource_iterator:
                resource_found = True                

                share_url = resource.get("url", "")
                resource_title = resource.get("title", "")

                # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                if resource.get("need_unlock") and not share_url:
                    slug = resource.get("slug")
                    if slug:
                        logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                        unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                        if not unlocked_url:
                            logger.error(f"未能解锁收费资源: {resource_title}")
                            continue
                        share_url = unlocked_url
                        # 更新当前字典以便历史存入或下次能沿用这个 url
                        resource["url"] = share_url
                        resource["need_unlock"] = False

                if not share_url:
                    continue

                logger.info(f"检查分享：{resource_title} - {share_url}")

                try:
                    # 先检查分享链接是否有效
                    share_status = self._p115_manager.check_share_status(share_url)
                    if not share_status.is_valid:
                        logger.warning(f"分享链接无效：{share_url}，原因：{share_status.status_text}")
                        continue

                    share_files = self._p115_manager.list_share_files(share_url)
                    if not share_files:
                        logger.info(f"分享链接无内容：{share_url}")
                        continue

                    # 匹配电影文件
                    matched_file = FileMatcher.match_movie_file(
                        share_files, mediainfo.title,
                        subscribe_filter=subscribe_filter
                    )

                    if matched_file:
                        file_name = matched_file.get('name', '')
                        logger.info(f"找到匹配文件：{file_name}")

                        # 计算当前文件的过滤分数和是否完美匹配
                        _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                        is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                        # 洗版模式下检查是否需要升级资源
                        if is_best_version and movie_history_score >= 0:
                            if current_score <= movie_history_score:
                                logger.info(f"电影 {mediainfo.title} 已有分数 {movie_history_score}，当前 {current_score}，跳过")
                                continue
                            else:
                                logger.info(f"电影 {mediainfo.title} 洗版：旧分数 {movie_history_score} -> 新分数 {current_score}")

                        # 调用 OpenClaw 七分类服务确定目标根目录
                        target_root = self._resolve_target_root(
                            share_url=share_url,
                            media_type="movie",
                            title=mediainfo.title,
                            fallback_root=self._movie_save_path,
                            year=mediainfo.year,
                            tmdb_id=mediainfo.tmdb_id,
                            resource_title=resource_title,
                            file_names=[file_name],
                        )
                        if not target_root:
                            continue

                        # 分类根目录下继续保留 MoviePilot 标准标题 + 年份目录
                        movie_folder = (
                            f"{mediainfo.title} ({mediainfo.year})"
                            if mediainfo.year
                            else mediainfo.title
                        )
                        save_dir = f"{target_root.rstrip('/')}/{movie_folder}"
                        logger.info(f"转存目标路径: {save_dir}")

                        # 执行转存
                        success = self._p115_manager.transfer_file(
                            share_url=share_url,
                            file_id=matched_file.get("id"),
                            save_path=save_dir
                        )

                        # 记录历史
                        history_item = {
                            "title": mediainfo.title,
                            "year": mediainfo.year,
                            "type": "电影",
                            "status": "成功" if success else "失败",
                            "share_url": share_url,
                            "file_name": file_name,
                            "filter_score": current_score,
                            "perfect_match": is_perfect,
                            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        history.append(history_item)

                        if success:
                            transferred_count += 1
                            movie_transferred = True
                            movie_history_score = current_score
                            score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                            logger.info(f"成功转存电影：{mediainfo.title} {score_info}")

                            # 收集转存详情用于通知
                            transfer_details.append({
                                "type": "电影",
                                "title": mediainfo.title,
                                "year": mediainfo.year,
                                "image": mediainfo.get_poster_image(),
                                "file_name": file_name
                            })

                            # 添加下载历史记录
                            try:
                                DownloadHistoryOper().add(
                                    path=save_dir,
                                    type=mediainfo.type.value,
                                    title=mediainfo.title,
                                    year=mediainfo.year,
                                    tmdbid=mediainfo.tmdb_id,
                                    imdbid=mediainfo.imdb_id,
                                    tvdbid=mediainfo.tvdb_id,
                                    doubanid=mediainfo.douban_id,
                                    image=mediainfo.get_poster_image(),
                                    downloader="115网盘",
                                    download_hash=matched_file.get("id"),
                                    torrent_name=resource_title,
                                    torrent_description=file_name,
                                    torrent_site="115网盘",
                                    username="P115StrgmSub",
                                    date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    note={"source": f"Subscribe|{subscribe.name}", "share_url": share_url}
                                )
                                logger.debug(f"已记录电影 {mediainfo.title} 下载历史")
                            except Exception as e:
                                logger.warning(f"记录下载历史失败：{e}")

                            # 电影转存成功后完成订阅
                            self._subscribe_handler.check_and_finish_subscribe(
                                subscribe=subscribe,
                                mediainfo=mediainfo,
                                success_episodes=[1]
                            )
                            # 订阅完成，清除该订阅的历史积分记录
                            if hasattr(self._search_handler, 'clear_sub_points'):
                                self._search_handler.clear_sub_points(sub_key)

                            # 实际转存成功，立即结束资源迭代，
                            # 避免生成器继续查询后续搜索源
                            break
                        else:
                            logger.error(f"转存失败：{mediainfo.title}")

                except Exception as e:
                    logger.error(f"处理分享链接出错：{share_url}, 错误：{str(e)}")
                    continue
                    
            # 未发布电影的泄漏探测，只有 AYCLUB 明确返回
            # ok_empty 时才会消耗本次机会；超时和报错不消耗。
            if (
                movie_gate.get("probe_due")
                and mediainfo.tmdb_id
            ):
                ayclub_status = (
                    self._search_handler.get_ayclub_last_status()
                )

                self._release_gate.mark_movie_probe_result(
                    tmdb_id=int(mediainfo.tmdb_id),
                    search_status=ayclub_status,
                )

                logger.info(
                    f"电影 {mediainfo.title} AYCLUB 泄漏探测状态："
                    f"{ayclub_status}"
                )

            if not resource_found:
                logger.info(
                    f"未找到电影 {mediainfo.title} 的任何 115 网盘候选资源"
                )
            elif not movie_transferred:
                logger.info(
                    f"电影 {mediainfo.title} 的候选资源均无效、"
                    f"不匹配过滤条件或转存失败"
                )
        except Exception as e:
            logger.error(f"处理电影订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count

    def process_tv_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        exclude_ids: Set[int]
    ) -> int:
        """
        处理单个电视剧订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :param exclude_ids: 排除的订阅ID集合
        :return: 更新后的转存数量
        """
        try:
            logger.info(f"订阅信息：{subscribe.name}，开始集数：{subscribe.start_episode}, 总集数：{subscribe.total_episode}, 缺失集数：{subscribe.lack_episode}")
            logger.info(f"处理订阅：{subscribe.name} (S{subscribe.season or 1})")

            # 加载该订阅的历史积分花费（用 tmdb_id + 季数作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_S{subscribe.season or 1}" if subscribe.tmdbid else f"{subscribe.name}_S{subscribe.season or 1}"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # 早期检查：如果订阅显示没有缺失集数，跳过处理
            if subscribe.lack_episode == 0:
                logger.info(f"{subscribe.name} S{subscribe.season or 1} 订阅显示媒体库已完整(lack_episode=0)，跳过")
                return transferred_count

            # 生成元数据
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.begin_season = subscribe.season or 1
            meta.type = MediaType.TV

            # 识别媒体信息
            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.TV,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )

            if not mediainfo:
                logger.warn(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count

            # 构造总集数信息
            totals = {}
            if subscribe.season and subscribe.total_episode:
                totals = {subscribe.season: subscribe.total_episode}

            # 获取缺失剧集
            downloadchain = DownloadChain()
            exist_flag, no_exists = downloadchain.get_no_exists_info(
                meta=meta,
                mediainfo=mediainfo,
                totals=totals
            )

            if exist_flag:
                logger.info(f"{mediainfo.title_year} S{meta.begin_season} 媒体库中已完整存在")
                # 媒体库已完整，调用完成订阅逻辑
                total_ep = subscribe.total_episode or 0
                start_ep = subscribe.start_episode or 1
                if total_ep > 0:
                    all_episodes = list(range(start_ep, total_ep + 1))
                    self._subscribe_handler.check_and_finish_subscribe(
                        subscribe=subscribe,
                        mediainfo=mediainfo,
                        success_episodes=all_episodes
                    )
                elif subscribe.lack_episode != 0:
                    SubscribeOper().update(subscribe.id, {"lack_episode": 0})
                # 订阅已完整，清除历史积分记录
                if hasattr(self._search_handler, 'clear_sub_points'):
                    self._search_handler.clear_sub_points(sub_key)
                return transferred_count

            # 获取缺失的集数列表
            season = meta.begin_season or 1
            missing_episodes = []
            mediakey = mediainfo.tmdb_id or mediainfo.douban_id

            if no_exists and mediakey:
                season_info = no_exists.get(mediakey, {})
                not_exist_info = season_info.get(season)
                if not_exist_info:
                    missing_episodes = not_exist_info.episodes or []
                    if not missing_episodes and not_exist_info.total_episode:
                        start_ep = not_exist_info.start_episode or 1
                        missing_episodes = list(range(start_ep, not_exist_info.total_episode + 1))

            if not missing_episodes:
                logger.info(f"{mediainfo.title_year} S{season} 没有缺失剧集信息")
                return transferred_count

            # 过滤掉小于开始集数的剧集
            if subscribe.start_episode:
                original_count = len(missing_episodes)
                missing_episodes = [ep for ep in missing_episodes if ep >= subscribe.start_episode]
                if len(missing_episodes) < original_count:
                    logger.info(f"根据订阅设置，过滤掉小于 {subscribe.start_episode} 的剧集")

            # best_version=1 表示开启洗版
            is_best_version = bool(subscribe.best_version)

            # 从历史记录中排除已成功转存的集数
            transferred_episodes = set()
            episode_history_scores: Dict[int, int] = {}
            for h in history:
                if (h.get("title") == mediainfo.title
                        and h.get("season") == season
                        and h.get("status") == "成功"):
                    ep = h.get("episode")
                    score = h.get("filter_score", 0)
                    perfect = h.get("perfect_match", False)

                    if not is_best_version:
                        transferred_episodes.add(ep)
                    else:
                        if perfect:
                            transferred_episodes.add(ep)
                        else:
                            if ep not in episode_history_scores or score > episode_history_scores[ep]:
                                episode_history_scores[ep] = score

            # 构建转存路径（标题 + 年份，格式如 "权力的游戏 (2011)"）
            show_folder = f"{mediainfo.title} ({mediainfo.year})" if mediainfo.year else mediainfo.title
            save_dir = f"{self._save_path}/{show_folder}/Season {season}"

            # 启用七分类时，实际目录要等分享分类后才能确定
            if self._classifier_client and self._classifier_client.enabled:
                existing_episodes_in_cloud = set()
            else:
                existing_episodes_in_cloud = FileMatcher.check_existing_episodes(
                    self._p115_manager, mediainfo, season, save_dir
                )

            # 合并已存在的集数
            all_existing = transferred_episodes | existing_episodes_in_cloud

            # 洗版模式下，需要升级的集数不应该被排除
            if is_best_version and episode_history_scores:
                episodes_to_upgrade = set(episode_history_scores.keys())
                all_existing = all_existing - episodes_to_upgrade
                if episodes_to_upgrade:
                    logger.info(f"{mediainfo.title_year} S{season} 洗版模式：{len(episodes_to_upgrade)} 集待升级")

            if all_existing:
                missing_episodes = [ep for ep in missing_episodes if ep not in all_existing]
                logger.info(
                    f"{mediainfo.title_year} S{season} 跳过已存在的 {len(all_existing)} 集 "
                    f"(历史记录:{len(transferred_episodes)}, 网盘:{len(existing_episodes_in_cloud)})"
                )

            if not missing_episodes:
                logger.info(f"{mediainfo.title_year} S{season} 所有缺失剧集已存在于网盘")
                # 网盘中已存在所有缺失集数，更新订阅状态
                if existing_episodes_in_cloud:
                    self._subscribe_handler.check_and_finish_subscribe(
                        subscribe=subscribe,
                        mediainfo=mediainfo,
                        success_episodes=list(existing_episodes_in_cloud)
                    )
                    # 缺失集数已全部补齐，清除历史积分记录
                    if hasattr(self._search_handler, 'clear_sub_points'):
                        self._search_handler.clear_sub_points(sub_key)
                return transferred_count

            # 根据 TMDB 剧集播出日期决定不同来源可查询的集数。
            # AYCLUB 受发布门禁控制；其他来源仍可查询已播出或日期未知的缺失集。
            all_missing_episodes = list(missing_episodes)
            episode_air_dates: Dict[int, Optional[str]] = {}
            metadata_ok = True

            tv_gate = {
                "allow_ayclub": False,
                "ayclub_first": False,
                "probe_due": False,
                "released": False,
                "reason": "missing_tmdb_id",
                "aired_episodes": [],
                "future_episodes": [],
                "unknown_episodes": [],
                "ayclub_episodes": [],
            }

            if mediainfo.tmdb_id:
                try:
                    from app.chain.tmdb import TmdbChain

                    tmdb_episodes = TmdbChain().tmdb_episodes(
                        tmdbid=mediainfo.tmdb_id,
                        season=season,
                    )

                    for episode_info in tmdb_episodes or []:
                        episode_number = getattr(
                            episode_info,
                            "episode_number",
                            None,
                        )

                        if not episode_number:
                            continue

                        episode_air_dates[int(episode_number)] = (
                            getattr(
                                episode_info,
                                "air_date",
                                None,
                            )
                        )

                    if not tmdb_episodes:
                        logger.info(
                            f"{mediainfo.title_year} S{season} "
                            f"TMDB未返回剧集信息，按播出日期未知处理"
                        )

                except Exception as error:
                    metadata_ok = False
                    logger.warning(
                        f"{mediainfo.title_year} S{season} "
                        f"查询TMDB剧集播出日期失败：{error}"
                    )

                tv_gate = self._release_gate.evaluate_tv(
                    tmdb_id=int(mediainfo.tmdb_id),
                    season=int(season),
                    missing_episodes=all_missing_episodes,
                    episode_air_dates=episode_air_dates,
                    metadata_ok=metadata_ok,
                )
            else:
                logger.info(
                    f"{mediainfo.title_year} S{season} 缺少 TMDB ID，"
                    f"本次不查询 AYCLUB"
                )

            # 普通来源不搜索有明确未来播出日期的集数；
            # 日期未知的集数保持原有行为，可以继续查询。
            standard_search_episodes = list(
                all_missing_episodes
            )

            if mediainfo.tmdb_id and metadata_ok:
                future_episode_set = set(
                    tv_gate.get("future_episodes") or []
                )

                standard_search_episodes = [
                    episode
                    for episode in all_missing_episodes
                    if episode not in future_episode_set
                ]

            ayclub_search_episodes = [
                episode
                for episode in all_missing_episodes
                if episode in set(
                    tv_gate.get("ayclub_episodes") or []
                )
            ]

            # 防止读取上一个订阅遗留的 AYCLUB 查询状态。
            self._search_handler.reset_ayclub_status()

            logger.info(
                f"{mediainfo.title_year} S{season} AYCLUB 发布门禁："
                f"允许={tv_gate.get('allow_ayclub')}，"
                f"优先={tv_gate.get('ayclub_first')}，"
                f"原因={tv_gate.get('reason')}，"
                f"查询集数={ayclub_search_episodes}"
            )

            logger.info(
                f"{mediainfo.title_year} S{season} "
                f"实际缺失剧集：{all_missing_episodes}；"
                f"普通来源可查询：{standard_search_episodes}"
            )
            # 创建订阅过滤条件
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"{mediainfo.title} S{season} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 成功转存的集数列表
            success_episodes = []

            # 智能回退搜索：按源迭代
            enabled_sources = self._search_handler.get_enabled_sources(
                ayclub_first=bool(
                    tv_gate.get("ayclub_first")
                ),
                allow_ayclub=bool(
                    tv_gate.get("allow_ayclub")
                ),
            )

            if not enabled_sources:
                logger.warning(
                    f"没有可用的搜索源，跳过 "
                    f"{mediainfo.title} S{season} 的搜索"
                )
                return transferred_count

            standard_episode_set = set(
                standard_search_episodes
            )
            ayclub_episode_set = set(
                ayclub_search_episodes
            )

            for source_index, source in enumerate(enabled_sources):
                if not missing_episodes:
                    logger.info(
                        f"{mediainfo.title_year} S{season} "
                        f"所有缺失剧集已转存完成，不再查询后续源"
                    )
                    break

                if transferred_count >= self._max_transfer_per_sync:
                    logger.info(
                        f"已达单次同步上限 "
                        f"{self._max_transfer_per_sync}，"
                        f"剩余 {len(missing_episodes)} 集将在下次同步处理"
                    )
                    break

                source_episode_set = (
                    ayclub_episode_set
                    if source == "ayclub"
                    else standard_episode_set
                )

                source_episodes = [
                    episode
                    for episode in missing_episodes
                    if episode in source_episode_set
                ]

                if not source_episodes:
                    logger.info(
                        f"[{source.upper()}] 当前没有符合播出门禁的"
                        f"缺失剧集，跳过该来源"
                    )
                    continue

                logger.info(
                    f"[{source.upper()}] 开始搜索 "
                    f"{mediainfo.title} S{season}，"
                    f"目标集数：{source_episodes}"
                )

                # 暂不把 episodes 传给桥接，以保留整季包搜索结果；
                # 后续只匹配和转存 source_episodes 中的缺失集。
                p115_results = self._search_handler.search_single_source(
                    source=source,
                    mediainfo=mediainfo,
                    media_type=MediaType.TV,
                    season=season,
                )

                if (
                    source == "ayclub"
                    and tv_gate.get("probe_due")
                    and mediainfo.tmdb_id
                ):
                    ayclub_status = (
                        self._search_handler.get_ayclub_last_status()
                    )

                    self._release_gate.mark_tv_probe_result(
                        tmdb_id=int(mediainfo.tmdb_id),
                        season=int(season),
                        search_status=ayclub_status,
                    )

                    logger.info(
                        f"{mediainfo.title_year} S{season} "
                        f"AYCLUB 泄漏探测状态：{ayclub_status}"
                    )

                if not p115_results:
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 未找到资源，将尝试下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 未找到资源，已无更多可用源")
                    continue

                logger.info(f"[{source.upper()}] 找到 {len(p115_results)} 个 115 网盘资源")

                # 遍历搜索结果
                for resource in p115_results:
                    if transferred_count >= self._max_transfer_per_sync:
                        logger.info(f"已达单次同步上限 {self._max_transfer_per_sync}，剩余 {len(missing_episodes)} 集将在下次同步处理")
                        break

                    share_url = resource.get("url", "")
                    resource_title = resource.get("title", "")

                    # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                    if resource.get("need_unlock") and not share_url:
                        slug = resource.get("slug")
                        if slug:
                            logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                            unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                            if not unlocked_url:
                                logger.error(f"未能解锁收费资源: {resource_title}")
                                continue
                            share_url = unlocked_url
                            # 更新当前字典以便存入历史或记录这个 url
                            resource["url"] = share_url
                            resource["need_unlock"] = False

                    if not share_url:
                        continue

                    logger.info(f"检查分享：{resource_title} - {share_url}")

                    try:
                        # 检查分享链接是否有效
                        share_status = self._p115_manager.check_share_status(share_url)
                        if not share_status.is_valid:
                            logger.warning(f"分享链接无效：{share_url}，原因：{share_status.status_text}")
                            continue

                        # 列出分享内容
                        share_files = self._p115_manager.list_share_files(
                            share_url,
                            target_season=(season if self._skip_other_season_dirs else None)
                        )
                        if not share_files:
                            logger.info(f"分享链接无内容：{share_url}")
                            continue

                        logger.info(f"分享包含 {len(share_files)} 个文件/目录")

                        # 收集该分享中所有匹配的文件
                        matched_items = []

                        for episode in [
                            episode
                            for episode in source_episodes
                            if episode in missing_episodes
                        ]:
                            matched_file = FileMatcher.match_episode_file(
                                share_files,
                                mediainfo.title,
                                season,
                                episode,
                                subscribe_filter=subscribe_filter
                            )

                            if matched_file:
                                file_name = matched_file.get('name', '')
                                logger.info(f"找到匹配文件：{file_name} -> E{episode:02d}")

                                _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                                is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                                is_upgrade = False
                                if is_best_version and episode in episode_history_scores:
                                    old_score = episode_history_scores[episode]
                                    if current_score <= old_score:
                                        logger.info(f"E{episode:02d} 已有分数 {old_score}，当前 {current_score}，跳过")
                                        continue
                                    else:
                                        logger.info(f"E{episode:02d} 洗版：旧分数 {old_score} -> 新分数 {current_score}")
                                        is_upgrade = True

                                matched_items.append({
                                    "file": matched_file,
                                    "episode": episode,
                                    "score": current_score,
                                    "is_perfect": is_perfect,
                                    "is_upgrade": is_upgrade
                                })

                        if not matched_items:
                            logger.info(f"该分享未匹配到 S{season} 的任何缺失剧集，可能是季数不匹配或文件名无法识别")
                            continue

                        # 调用 OpenClaw 七分类服务确定剧集目标根目录
                        target_root = self._resolve_target_root(
                            share_url=share_url,
                            media_type="tv",
                            title=mediainfo.title,
                            fallback_root=self._save_path,
                            year=mediainfo.year,
                            tmdb_id=mediainfo.tmdb_id,
                            season=season,
                            resource_title=resource_title,
                            file_names=[
                                item["file"].get("name", "")
                                for item in matched_items
                            ],
                        )
                        if not target_root:
                            continue

                        # 分类根目录下保留标题、年份和季度目录
                        save_dir = (
                            f"{target_root.rstrip('/')}/"
                            f"{show_folder}/Season {season}"
                        )
                        logger.info(f"剧集分类后的转存目标路径: {save_dir}")

                        # 检查分类后的真实目录中是否已有剧集
                        classified_existing = FileMatcher.check_existing_episodes(
                            self._p115_manager,
                            mediainfo,
                            season,
                            save_dir,
                        )

                        if classified_existing:
                            existing_episodes_in_cloud |= classified_existing

                            # 洗版模式下，需要升级的剧集不能因文件已存在而跳过
                            skip_existing = set(classified_existing)
                            if is_best_version and episode_history_scores:
                                skip_existing -= set(episode_history_scores.keys())

                            if skip_existing:
                                matched_items = [
                                    item
                                    for item in matched_items
                                    if item["episode"] not in skip_existing
                                ]
                                missing_episodes = [
                                    episode
                                    for episode in missing_episodes
                                    if episode not in skip_existing
                                ]
                                logger.info(
                                    f"分类目录中已存在 {len(skip_existing)} 集，已跳过"
                                )

                                if not matched_items:
                                    continue

                        # 检查转存配额限制
                        remaining_quota = self._max_transfer_per_sync - transferred_count
                        if len(matched_items) > remaining_quota:
                            logger.info(f"匹配 {len(matched_items)} 集，但受配额限制仅转存 {remaining_quota} 集")
                            matched_items = matched_items[:remaining_quota]

                        # 批量转存
                        file_ids = [item["file"]["id"] for item in matched_items]
                        logger.info(f"准备批量转存 {len(file_ids)} 个文件到: {save_dir}")

                        success_ids, failed_ids = self._p115_manager.transfer_files_batch(
                            share_url=share_url,
                            file_ids=file_ids,
                            save_path=save_dir,
                            batch_size=self._batch_size
                        )

                        success_id_set = set(success_ids)
                        batch_success_episodes = []

                        # 处理结果
                        for item in matched_items:
                            file_id = item["file"]["id"]
                            episode = item["episode"]
                            file_name = item["file"]["name"]
                            current_score = item["score"]
                            is_perfect = item["is_perfect"]
                            is_upgrade = item["is_upgrade"]
                            success = file_id in success_id_set

                            history_item = {
                                "title": mediainfo.title,
                                "season": season,
                                "episode": episode,
                                "type": "电视剧",
                                "status": "成功" if success else "失败",
                                "share_url": share_url,
                                "file_name": file_name,
                                "filter_score": current_score,
                                "perfect_match": is_perfect,
                                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            history.append(history_item)

                            if success:
                                transferred_count += 1
                                episode_history_scores[episode] = current_score

                                if episode in missing_episodes:
                                    missing_episodes.remove(episode)

                                if not is_upgrade:
                                    success_episodes.append(episode)

                                score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                                upgrade_info = " [洗版升级]" if is_upgrade else ""
                                logger.info(f"成功转存：{mediainfo.title} S{season:02d}E{episode:02d} {score_info}{upgrade_info}")

                                # 收集转存详情
                                existing_detail = next(
                                    (d for d in transfer_details
                                     if d.get("title") == mediainfo.title and d.get("season") == season),
                                    None
                                )
                                if existing_detail:
                                    existing_detail["episodes"].append(episode)
                                else:
                                    transfer_details.append({
                                        "type": "电视剧",
                                        "title": mediainfo.title,
                                        "year": mediainfo.year,
                                        "season": season,
                                        "episodes": [episode],
                                        "image": mediainfo.get_poster_image()
                                    })

                                batch_success_episodes.append(episode)
                            else:
                                logger.error(f"转存失败：{mediainfo.title} S{season:02d}E{episode:02d}")

                        # 记录下载历史
                        if batch_success_episodes:
                            try:
                                episodes_str = StringUtils.format_ep(batch_success_episodes)
                                DownloadHistoryOper().add(
                                    path=save_dir,
                                    type=mediainfo.type.value,
                                    title=mediainfo.title,
                                    year=mediainfo.year,
                                    tmdbid=mediainfo.tmdb_id,
                                    imdbid=mediainfo.imdb_id,
                                    tvdbid=mediainfo.tvdb_id,
                                    doubanid=mediainfo.douban_id,
                                    seasons=f"S{season:02d}",
                                    episodes=episodes_str,
                                    image=mediainfo.get_poster_image(),
                                    downloader="115网盘",
                                    download_hash=share_url,
                                    torrent_name=resource_title,
                                    torrent_site="115网盘",
                                    username="P115StrgmSub",
                                    date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    note={"source": f"Subscribe|{subscribe.name}", "share_url": share_url}
                                )
                                logger.debug(f"已记录 {mediainfo.title} S{season:02d} {episodes_str} 下载历史")
                            except Exception as e:
                                logger.warning(f"记录下载历史失败：{e}")

                        if not missing_episodes:
                            break

                    except Exception as e:
                        logger.error(f"处理分享链接出错：{share_url}, 错误：{str(e)}")
                        continue

                # 当前源处理完成
                if missing_episodes:
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，继续查询下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，已无更多可用源")

            # 更新订阅状态
            # 将网盘已存在的集数和本次成功转存的集数合并
            all_success_episodes = list(set(success_episodes) | existing_episodes_in_cloud)
            if all_success_episodes:
                self._subscribe_handler.check_and_finish_subscribe(
                    subscribe=subscribe,
                    mediainfo=mediainfo,
                    success_episodes=all_success_episodes
                )
                # 如果订阅已完成（缺失集数归零），清除该订阅的历史积分记录
                total_ep = subscribe.total_episode or 0
                start_ep = subscribe.start_episode or 1
                if total_ep > 0:
                    expected = set(range(start_ep, total_ep + 1))
                    downloaded = set(subscribe.note or []).union(set(all_success_episodes))
                    if not (expected - downloaded):
                        if hasattr(self._search_handler, 'clear_sub_points'):
                            self._search_handler.clear_sub_points(sub_key)

        except Exception as e:
            logger.error(f"处理订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count

    def send_transfer_notification(self, transfer_details: List[Dict[str, Any]], total_count: int):
        """
        发送转存完成通知

        :param transfer_details: 转存详情列表
        :param total_count: 转存总数
        """
        if not transfer_details or not self._post_message:
            return

        text_lines = []
        first_image = None

        for detail in transfer_details:
            if detail.get("type") == "电影":
                title = detail.get("title", "未知")
                year = detail.get("year", "")
                text_lines.append(f"{title} ({year})")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")
            else:
                title = detail.get("title", "未知")
                season = detail.get("season", 1)
                episodes = detail.get("episodes", [])
                episodes.sort()
                if len(episodes) <= 5:
                    ep_str = ", ".join([f"E{e:02d}" for e in episodes])
                else:
                    ep_str = f"E{episodes[0]:02d}-E{episodes[-1]:02d} 共{len(episodes)}集"
                text_lines.append(f"{title} S{season:02d} {ep_str}")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")

        if len(text_lines) > 10:
            text_lines = text_lines[:10]
            text_lines.append(f"... 等共 {len(transfer_details)} 项")

        self._post_message(
            mtype=NotificationType.Plugin,
            title=f"【115网盘订阅追更】转存完成",
            text=f"本次共转存 {total_count} 个文件\n\n" + "\n".join(text_lines)
        )
