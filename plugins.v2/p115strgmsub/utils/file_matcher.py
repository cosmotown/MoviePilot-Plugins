"""
文件匹配模块
负责剧集文件的匹配和网盘已存在集数的检查
"""
import re
from pathlib import Path
from typing import List, Optional, Set, Tuple
from app.core.metainfo import MetaInfo
from app.schemas import MediaInfo
from app.log import logger


class SubscribeFilter:
    """订阅过滤条件"""

    def __init__(self, quality: str = None, resolution: str = None, effect: str = None, strict: bool = True):
        """
        初始化过滤条件

        :param quality: 质量正则表达式，如 "WEB-?DL|WEB-?RIP"
        :param resolution: 分辨率正则表达式，如 "4K|2160p|x2160"
        :param effect: 特效正则表达式
        :param strict: 是否严格匹配，False 时不符合条件的资源也会被接受但分数较低
        """
        self.quality = quality
        self.resolution = resolution
        self.effect = effect
        self.strict = strict

    def has_filters(self) -> bool:
        """是否有任何过滤条件"""
        return bool(self.quality or self.resolution or self.effect)

    def match(self, file_name: str) -> Tuple[bool, int]:
        """
        检查文件名是否符合过滤条件

        :param file_name: 文件名
        :return: (是否匹配, 匹配分数) - 分数越高越优先
                 严格模式下不匹配返回 (False, 0)
                 非严格模式下不匹配返回 (True, 较低分数)
        """
        if not self.has_filters():
            return True, 0

        score = 0
        matched_count = 0
        total_rules = 0

        # 检查质量
        if self.quality:
            total_rules += 1
            if re.search(self.quality, file_name, re.IGNORECASE):
                score += 100  # 质量匹配加 100 分
                matched_count += 1
                logger.info(f"文件 {file_name} 匹配质量规则: {self.quality}")
            else:
                logger.info(f"文件 {file_name} 不匹配质量规则: {self.quality}")
                if self.strict:
                    return False, 0

        # 检查分辨率
        if self.resolution:
            total_rules += 1
            if re.search(self.resolution, file_name, re.IGNORECASE):
                score += 100  # 分辨率匹配加 100 分
                matched_count += 1
                logger.info(f"文件 {file_name} 匹配分辨率规则: {self.resolution}")
            else:
                logger.info(f"文件 {file_name} 不匹配分辨率规则: {self.resolution}")
                if self.strict:
                    return False, 0

        # 检查特效
        if self.effect:
            total_rules += 1
            if re.search(self.effect, file_name, re.IGNORECASE):
                score += 100  # 特效匹配加 100 分
                matched_count += 1
                logger.info(f"文件 {file_name} 匹配特效规则: {self.effect}")
            else:
                logger.info(f"文件 {file_name} 不匹配特效规则: {self.effect}")
                if self.strict:
                    return False, 0

        # 非严格模式下，即使不完全匹配也返回 True，但分数较低
        # 完全匹配的资源分数更高，便于后续替换
        return True, score

    def is_perfect_match(self, file_name: str) -> bool:
        """
        检查文件是否完全匹配所有过滤条件
        用于判断是否需要替换已有资源
        """
        if not self.has_filters():
            return True

        if self.quality and not re.search(self.quality, file_name, re.IGNORECASE):
            return False
        if self.resolution and not re.search(self.resolution, file_name, re.IGNORECASE):
            return False
        if self.effect and not re.search(self.effect, file_name, re.IGNORECASE):
            return False
        return True


class FileMatcher:
    """文件匹配器类"""

    # 视频文件扩展名
    VIDEO_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.rmvb', '.wmv', '.flv', '.ts', '.m2ts'}
    
    @staticmethod
    def _contains_other_season(file_name: str, target_season: int) -> bool:
        """
        检查文件名是否明确包含其他季的标识

        :param file_name: 文件名
        :param target_season: 目标季号
        :return: 是否包含其他季标识
        """
        # 匹配 S01、S02 等格式，检查是否为其他季
        season_match = re.search(r'[Ss](\d{1,2})[Ee]', file_name)
        if season_match:
            found_season = int(season_match.group(1))
            if found_season != target_season:
                return True

        # 匹配 "第X季" 格式
        cn_season_match = re.search(r'第\s*(\d{1,2})\s*季', file_name)
        if cn_season_match:
            found_season = int(cn_season_match.group(1))
            if found_season != target_season:
                return True

        # 匹配 Season X 格式
        en_season_match = re.search(r'[Ss]eason\s*(\d{1,2})', file_name, re.IGNORECASE)
        if en_season_match:
            found_season = int(en_season_match.group(1))
            if found_season != target_season:
                return True

        return False

    @staticmethod
    def _matches_target_season(file_name: str, target_season: int) -> bool:
        """
        检查文件名是否明确匹配目标季

        :param file_name: 文件名
        :param target_season: 目标季号
        :return: 是否匹配目标季
        """
        # 匹配 S01、S02 等格式
        season_match = re.search(r'[Ss](\d{1,2})[Ee]', file_name)
        if season_match:
            found_season = int(season_match.group(1))
            return found_season == target_season

        # 匹配 "第X季" 格式
        cn_season_match = re.search(r'第\s*(\d{1,2})\s*季', file_name)
        if cn_season_match:
            found_season = int(cn_season_match.group(1))
            return found_season == target_season

        # 匹配 Season X 格式
        en_season_match = re.search(r'[Ss]eason\s*(\d{1,2})', file_name, re.IGNORECASE)
        if en_season_match:
            found_season = int(en_season_match.group(1))
            return found_season == target_season

        return False

    @staticmethod
    def _extract_episode_from_sxex(file_name: str) -> Optional[Tuple[int, int]]:
        """
        从文件名中提取 SxxExx 格式的季号和集号

        :param file_name: 文件名
        :return: (季号, 集号) 或 None
        """
        # 匹配 S01E01、S1E1、S01E175 等格式（支持1-4位集数）
        match = re.search(r'[Ss](\d{1,2})[Ee](\d{1,4})', file_name)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    @staticmethod
    def match_episode_file(
        files: List[dict],
        title: str,
        season: int,
        episode: int,
        subscribe_filter: 'SubscribeFilter' = None
    ) -> Optional[dict]:
        """
        匹配剧集文件

        :param files: 文件列表
        :param title: 剧集标题
        :param season: 季号
        :param episode: 集号
        :param subscribe_filter: 订阅过滤条件（质量、分辨率、特效）
        :return: 匹配的文件信息
        """
        # 宽松模式：不包含季号的匹配模式（需要额外验证）
        loose_patterns = [
            # 第1集、第175集 格式
            rf'第\s*{episode}\s*集',
            # EP01、EP175 格式
            rf'[Ee][Pp]{episode}(?!\d)',
            # E01格式（开头或特定位置）
            rf'[\[\(\s\.\-_][Ee]0?{episode}[\]\)\s\.\-_]',
        ]

        # 最宽松模式：纯数字匹配（风险较高，仅作为最后手段）
        # 仅当文件名没有 SxxExx 格式且明确匹配目标季或无季号标识时使用
        loosest_patterns = [
            # .01. 格式
            rf'[\.\s\-_]0?{episode}[\.\s\-_]',
        ]

        # 收集候选文件，按匹配优先级排序
        # 每个元素是 (file, filter_score)，filter_score 越高越优先
        strict_matches = []
        loose_matches = []
        loosest_matches = []

        # 诊断统计
        stats = {
            "total_files": 0,
            "non_video": 0,
            "other_season": 0,
            "filter_rejected": 0,
            "episode_mismatch": 0,
            "directories": 0,
        }

        for file in files:
            file_name = file.get("name", "")
            is_dir = file.get("is_dir", False)

            # 跳过目录（但可以递归处理子文件）
            if is_dir:
                stats["directories"] += 1
                sub_files = file.get("children", [])
                if sub_files:
                    matched = FileMatcher.match_episode_file(sub_files, title, season, episode, subscribe_filter)
                    if matched:
                        return matched
                continue

            stats["total_files"] += 1

            # 检查文件扩展名
            ext = Path(file_name).suffix.lower()
            if ext not in FileMatcher.VIDEO_EXTENSIONS:
                stats["non_video"] += 1
                continue

            # 如果明确包含其他季的标识，直接跳过
            if FileMatcher._contains_other_season(file_name, season):
                stats["other_season"] += 1
                logger.info(f"文件 {file_name} 属于其他季，跳过（目标: S{season}）")
                continue

            # 应用订阅过滤条件
            filter_score = 0
            if subscribe_filter and subscribe_filter.has_filters():
                matched, filter_score = subscribe_filter.match(file_name)
                if not matched:
                    stats["filter_rejected"] += 1
                    logger.info(f"文件 {file_name} 不符合订阅过滤条件，跳过")
                    continue

            # 优先检查 SxxExx 格式（最准确）
            sxex_info = FileMatcher._extract_episode_from_sxex(file_name)
            if sxex_info:
                found_season, found_episode = sxex_info
                # 如果有明确的 SxxExx 格式，必须精确匹配，不再使用其他模式
                if found_season == season and found_episode == episode:
                    strict_matches.append((file, filter_score))
                else:
                    stats["episode_mismatch"] += 1
                    # logger.info(f"文件 {file_name} 集数不匹配（找到: S{found_season}E{found_episode}，目标: S{season}E{episode}）")
                # 不匹配则跳过这个文件，不再尝试其他模式
                continue

            # 没有 SxxExx 格式时，使用宽松模式匹配
            for pattern in loose_patterns:
                if re.search(pattern, file_name, re.IGNORECASE):
                    # 额外检查：如果是第一季，或者文件名明确匹配目标季
                    if season == 1 or FileMatcher._matches_target_season(file_name, season):
                        loose_matches.append((file, filter_score))
                    # 如果文件名没有任何季号标识，也接受（可能是单季剧）
                    elif not re.search(r'[Ss]\d+[Ee]|第\s*\d+\s*季|[Ss]eason\s*\d+', file_name, re.IGNORECASE):
                        loose_matches.append((file, filter_score))
                    break
            else:
                # 最宽松模式：仅当文件名明确匹配目标季时使用
                if FileMatcher._matches_target_season(file_name, season):
                    for pattern in loosest_patterns:
                        if re.search(pattern, file_name, re.IGNORECASE):
                            loosest_matches.append((file, filter_score))
                            break

        # 按优先级返回匹配结果（同级别内按 filter_score 降序排序）
        if strict_matches:
            strict_matches.sort(key=lambda x: x[1], reverse=True)
            return strict_matches[0][0]
        if loose_matches:
            loose_matches.sort(key=lambda x: x[1], reverse=True)
            return loose_matches[0][0]
        if loosest_matches:
            loosest_matches.sort(key=lambda x: x[1], reverse=True)
            return loosest_matches[0][0]

        # 没有匹配时，输出诊断信息
        if stats["total_files"] > 0:
            reasons = []
            if stats["other_season"] > 0:
                reasons.append(f"季数不匹配:{stats['other_season']}个")
            if stats["episode_mismatch"] > 0:
                reasons.append(f"集数不匹配:{stats['episode_mismatch']}个")
            if stats["filter_rejected"] > 0:
                reasons.append(f"过滤条件不符:{stats['filter_rejected']}个")
            if stats["non_video"] > 0:
                reasons.append(f"非视频文件:{stats['non_video']}个")
            
            if reasons:
                logger.info(f"S{season}E{episode} 无匹配 - 视频文件{stats['total_files']}个, {', '.join(reasons)}")

        return None

    @staticmethod
    def _normalize_movie_text(value: str) -> str:
        """电影名比较用规范化：保留中英文、数字，忽略空格和标点。"""
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(value or "").lower())

    @staticmethod
    def _movie_title_matches(file_name: str, title: str) -> bool:
        """避免把《片名2/3》误当作不带序号的第一部。"""
        file_norm = FileMatcher._normalize_movie_text(Path(file_name).stem)
        title_norm = FileMatcher._normalize_movie_text(title)
        if not file_norm or not title_norm:
            return False
        index = file_norm.find(title_norm)
        if index < 0:
            return False
        suffix = file_norm[index + len(title_norm):]
        # 命中标题后仍紧跟数字，通常是另一续集编号（如“片名2”误中“片名20”）。
        if suffix[:1].isdigit():
            return False
        return True

    @staticmethod
    def _movie_quality_rank(file_name: str) -> Tuple[int, int, int, int, int]:
        """按分辨率、片源、HDR、音轨、编码生成稳定的电影质量排序。"""
        name = str(file_name or "").lower()

        if re.search(r"(?:2160p|4k|uhd)", name):
            resolution = 4
        elif re.search(r"1080[pi]", name):
            resolution = 3
        elif re.search(r"720p", name):
            resolution = 2
        else:
            resolution = 1

        if "remux" in name:
            source = 6
        elif re.search(r"(?:uhd[ ._-]*blu-?ray|blu-?ray[ ._-]*uhd)", name):
            source = 5
        elif re.search(r"blu-?ray|bdrip|bdremux", name):
            source = 4
        elif re.search(r"web[ ._-]*dl", name):
            source = 3
        elif "webrip" in name:
            source = 2
        elif "hdtv" in name:
            source = 1
        else:
            source = 0

        if re.search(r"dolby[ ._-]*vision|\bdv\b", name):
            hdr = 4
        elif re.search(r"hdr10\+|hdr10plus", name):
            hdr = 3
        elif re.search(r"hdr10|\bhdr\b", name):
            hdr = 2
        else:
            hdr = 0

        if "atmos" in name:
            audio = 5
        elif "truehd" in name:
            audio = 4
        elif re.search(r"dts[ ._-]*hd[ ._-]*ma|dts-hd", name):
            audio = 3
        elif re.search(r"ddp|eac3|dolby[ ._-]*digital[ ._-]*plus", name):
            audio = 2
        elif re.search(r"\bdts\b|\bac3\b", name):
            audio = 1
        else:
            audio = 0

        if re.search(r"av1", name):
            codec = 3
        elif re.search(r"h[ ._-]*265|hevc|x265", name):
            codec = 2
        elif re.search(r"h[ ._-]*264|avc|x264", name):
            codec = 1
        else:
            codec = 0

        return resolution, source, hdr, audio, codec

    @staticmethod
    def match_movie_file(
        files: List[dict],
        title: str,
        year: int = None,
        min_size_mb: int = 500,
        subscribe_filter: 'SubscribeFilter' = None
    ) -> Optional[dict]:
        """
        匹配目标电影并在同片多版本中只选择质量最高的一份。

        :param files: 文件列表
        :param title: 电影标题
        :param year: 目标年份；合集分享中用于排除续集/其他影片
        :param min_size_mb: 最小文件大小（MB），用于过滤小文件
        :param subscribe_filter: 订阅过滤条件（质量、分辨率、特效）
        :return: 唯一最佳匹配文件
        """
        # 候选：(file, filter_score, title_match, quality_rank)
        raw_candidates = []
        min_size_bytes = min_size_mb * 1024 * 1024
        target_year = None
        try:
            target_year = int(year) if year else None
        except (TypeError, ValueError):
            target_year = None

        def collect_video_files(file_list: List[dict]):
            """递归收集视频文件，但始终只返回具体文件，不转存父目录。"""
            for file in file_list:
                file_name = file.get("name", "")
                is_dir = file.get("is_dir", False)

                if is_dir:
                    sub_files = file.get("children", [])
                    if sub_files:
                        collect_video_files(sub_files)
                    continue

                ext = Path(file_name).suffix.lower()
                if ext not in FileMatcher.VIDEO_EXTENSIONS:
                    continue

                file_size = int(file.get("size", 0) or 0)
                if file_size < min_size_bytes:
                    continue

                years = {
                    int(value)
                    for value in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", file_name)
                }
                title_match = FileMatcher._movie_title_matches(file_name, title)

                # 文件明确写了其他年份时直接排除；写了目标年份时可作为合集安全匹配。
                if target_year and years and target_year not in years:
                    logger.info(
                        f"电影文件 {file_name} 年份不匹配（目标:{target_year}），跳过"
                    )
                    continue
                if target_year and target_year in years:
                    identity_match = True
                else:
                    identity_match = title_match
                if not identity_match:
                    logger.info(f"电影文件 {file_name} 片名/年份不匹配目标 {title}，跳过")
                    continue

                filter_score = 0
                if subscribe_filter and subscribe_filter.has_filters():
                    matched, filter_score = subscribe_filter.match(file_name)
                    if not matched:
                        logger.info(f"电影文件 {file_name} 不符合订阅过滤条件，跳过")
                        continue

                raw_candidates.append((
                    file,
                    filter_score,
                    1 if title_match else 0,
                    FileMatcher._movie_quality_rank(file_name),
                ))

        collect_video_files(files)

        if not raw_candidates:
            return None

        # 订阅过滤分优先；再按画质层级，片名直接匹配仅作为同画质辅助。
        raw_candidates.sort(
            key=lambda item: (
                item[1],
                item[3],
                item[2],
                int(item[0].get("size", 0) or 0),
            ),
            reverse=True,
        )
        selected = raw_candidates[0][0]

        if len(raw_candidates) > 1:
            rejected = [item[0].get("name", "") for item in raw_candidates[1:]]
            logger.info(
                f"电影同片多版本择优：共 {len(raw_candidates)} 个候选，"
                f"选择 {selected.get('name', '')}"
            )
            logger.info(f"电影同片多版本已跳过：{rejected}")

        return selected

    @staticmethod
    def check_existing_episodes(
        p115_manager,
        mediainfo: MediaInfo,
        season: int,
        save_dir: str
    ) -> Set[int]:
        """
        检查115网盘目录中已存在的剧集集数

        :param p115_manager: 115客户端管理器
        :param mediainfo: 媒体信息
        :param season: 季号
        :param save_dir: 网盘保存目录
        :return: 已存在的集数集合
        """
        existing_episodes = set()

        if not p115_manager:
            return existing_episodes

        try:
            # 优化：先检查目录是否存在，避免无效的 list_files 调用
            dir_id = p115_manager.get_pid_by_path(save_dir, mkdir=False)
            if dir_id == -1:
                logger.info(f"网盘目录不存在，跳过检查: {save_dir}")
                return existing_episodes

            # 列出网盘目录中的文件
            files = p115_manager.list_files(save_dir)
            if not files:
                logger.info(f"网盘目录为空: {save_dir}")
                return existing_episodes

            logger.info(f"检查网盘目录 {save_dir}，共 {len(files)} 个文件")

            # DEBUG: 打印前3个文件的完整结构
            # if files:
            #     for i, f in enumerate(files[:3]):
            #         logger.info(f"[DEBUG] 文件样本 {i+1}: {f}")

            # 使用MetaInfo识别每个文件的集数
            for file_info in files:
                # fs_files API 返回 'n' 作为文件名字段，而非 'name'
                file_name = file_info.get("n") or file_info.get("name", "")
                # fid 字段: 0 表示目录，非0 表示文件
                # 注意: 使用 None 作为默认值，避免将没有 fid 字段的文件误判为目录
                fid = file_info.get("fid")
                is_dir = (fid == 0 or fid == "0")
                
                # DEBUG: 显示每个文件的 fid 和判断结果
                # logger.info(f"文件: {file_name}, fid={fid}, is_dir={is_dir}")

                # 跳过目录
                if is_dir:
                    continue

                # 检查是否为视频文件
                file_ext = Path(file_name).suffix.lower()
                if file_ext not in FileMatcher.VIDEO_EXTENSIONS:
                    continue

                # 检查是否包含其他季的标识，如果是则跳过
                if FileMatcher._contains_other_season(file_name, season):
                    logger.info(f"跳过其他季文件: {file_name}")
                    continue

                # 使用MetaInfo识别文件信息
                meta = MetaInfo(file_name)

                # 检查季号是否匹配
                # 情况1: 文件名包含季号且匹配目标季
                # 情况2: 文件名无季号（meta.begin_season 为 None），视为当前目录对应的季
                #        因为 save_dir 已经是 Season X 目录，文件应该属于该季
                season_matches = (
                    (meta.begin_season is not None and meta.begin_season == season) or
                    (meta.begin_season is None and not FileMatcher._contains_other_season(file_name, season))
                )

                if season_matches and meta.begin_episode:
                    existing_episodes.add(meta.begin_episode)
                    logger.info(f"识别到已存在集数: {file_name} -> S{season:02d}E{meta.begin_episode:02d}")

                    # 如果是剧集范围（如E01-E03），添加所有集数
                    if meta.end_episode and meta.end_episode != meta.begin_episode:
                        for ep in range(meta.begin_episode, meta.end_episode + 1):
                            existing_episodes.add(ep)

            if existing_episodes:
                logger.info(f"{mediainfo.title} S{season} 网盘已存在 {len(existing_episodes)} 集: {sorted(existing_episodes)}")
            else:
                logger.info(f"{mediainfo.title} S{season} 网盘目录中未找到该季剧集")

        except Exception as e:
            logger.error(f"检查网盘目录失败: {e}")

        return existing_episodes
