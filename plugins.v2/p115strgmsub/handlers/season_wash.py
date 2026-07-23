"""
已完成电视剧的 AYCLUB ED2K 完整整季洗版候选识别。

本模块不依赖 MoviePilot，负责纯数据校验：
- 只接受标准 ED2K file 链接；
- 真实文件名中的 SxxExxx 是季集唯一事实源；
- 只有同一命名指纹完整、无缺集、无重复、无越季时才形成整季候选；
- 指纹和日志引用均不可逆，不持久化完整 ED2K。
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import re
import unicodedata
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote


_EPISODE_TOKEN = re.compile(r"S0*(\d{1,3})E0*(\d{1,3})(?!\d)", re.IGNORECASE)


@dataclass
class CompleteSeasonEd2kPack:
    season: int
    expected_episodes: Tuple[int, ...]
    signature: str
    signature_ref: str
    fingerprint: str
    score: int
    episode_resources: Dict[int, Dict[str, Any]]

    @property
    def episodes(self) -> List[int]:
        return list(self.expected_episodes)

    def ordered_resources(self) -> List[Tuple[int, Dict[str, Any]]]:
        return [
            (episode, self.episode_resources[episode])
            for episode in self.expected_episodes
        ]


def is_ed2k_file_url(value: str) -> bool:
    source_url = str(value or "").strip()
    lowered = source_url.casefold()
    return bool(
        20 <= len(source_url) <= 16384
        and "\r" not in source_url
        and "\n" not in source_url
        and lowered.startswith("ed2k://|file|")
        and lowered.endswith("|/")
        and source_url.count("|") >= 5
    )


def ed2k_filename(source_url: str) -> str:
    if not is_ed2k_file_url(source_url):
        return ""
    parts = str(source_url).split("|")
    if len(parts) < 4:
        return ""
    try:
        return unquote(parts[2]).strip()
    except Exception:
        return str(parts[2] or "").strip()


def filename_identity(filename: str) -> Optional[Tuple[int, int, str]]:
    normalized = unicodedata.normalize("NFKC", str(filename or "")).strip()
    matches = list(_EPISODE_TOKEN.finditer(normalized))
    identities = {
        (int(match.group(1)), int(match.group(2)))
        for match in matches
        if 0 < int(match.group(1)) <= 999 and 0 < int(match.group(2)) <= 999
    }
    if len(identities) != 1 or len(matches) != 1:
        return None

    season, episode = next(iter(identities))
    signature = _EPISODE_TOKEN.sub(
        f"S{season:02d}E{{EPISODE}}",
        normalized,
        count=1,
    )
    signature = " ".join(signature.casefold().split())
    return season, episode, signature


def _structured_episode_set(resource: Dict[str, Any]) -> Set[int]:
    result: Set[int] = set()
    values = resource.get("episodes") or []
    if not isinstance(values, (list, tuple, set)):
        values = []
    for value in values:
        try:
            episode = int(value)
        except (TypeError, ValueError):
            continue
        if 0 < episode <= 999:
            result.add(episode)
    try:
        episode = int(resource.get("episode"))
        if 0 < episode <= 999:
            result.add(episode)
    except (TypeError, ValueError):
        pass
    return result


def _resource_score(
    resource: Dict[str, Any],
    score_func: Optional[Callable[[Dict[str, Any]], int]],
) -> int:
    if not score_func:
        return 0
    try:
        return int(score_func(resource) or 0)
    except Exception:
        return 0


def select_complete_uniform_ed2k_pack(
    resources: Iterable[Dict[str, Any]],
    *,
    season: int,
    expected_episodes: Sequence[int],
    score_func: Optional[Callable[[Dict[str, Any]], int]] = None,
) -> Tuple[Optional[CompleteSeasonEd2kPack], Dict[str, Any]]:
    target_season = int(season)
    expected_values: Set[int] = set()
    for value in expected_episodes:
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            expected_values.add(number)
    expected = tuple(sorted(expected_values))
    expected_set = set(expected)
    diagnostics: Dict[str, Any] = {
        "input_count": 0,
        "valid_count": 0,
        "invalid_count": 0,
        "group_count": 0,
        "complete_group_count": 0,
        "rejections": [],
    }
    if not expected:
        diagnostics["rejections"].append("empty_expected_episode_set")
        return None, diagnostics

    groups: Dict[str, Dict[str, Any]] = {}
    for original in resources or []:
        diagnostics["input_count"] += 1
        if not isinstance(original, dict):
            diagnostics["invalid_count"] += 1
            continue
        resource = dict(original)
        source_url = str(resource.get("url") or "").strip()
        filename = ed2k_filename(source_url)
        identity = filename_identity(filename)
        if not identity:
            diagnostics["invalid_count"] += 1
            continue
        parsed_season, episode, signature = identity
        if parsed_season != target_season or episode not in expected_set:
            diagnostics["invalid_count"] += 1
            continue

        explicit_season = resource.get("season")
        try:
            if explicit_season is not None and int(explicit_season) != target_season:
                diagnostics["invalid_count"] += 1
                continue
        except (TypeError, ValueError):
            diagnostics["invalid_count"] += 1
            continue

        structured = _structured_episode_set(resource)
        if structured and structured != {episode}:
            diagnostics["invalid_count"] += 1
            continue

        diagnostics["valid_count"] += 1
        group = groups.setdefault(
            signature,
            {
                "episode_resources": {},
                "duplicate_episodes": set(),
                "scores": [],
            },
        )
        old = group["episode_resources"].get(episode)
        if old:
            if str(old.get("url") or "") != source_url:
                group["duplicate_episodes"].add(episode)
            continue
        resource["_ed2k_filename"] = filename
        group["episode_resources"][episode] = resource
        group["scores"].append(_resource_score(resource, score_func))

    diagnostics["group_count"] = len(groups)
    candidates: List[CompleteSeasonEd2kPack] = []
    for signature, group in groups.items():
        duplicate_episodes = set(group["duplicate_episodes"])
        episode_resources = dict(group["episode_resources"])
        actual = set(episode_resources)
        signature_ref = sha256(signature.encode("utf-8")).hexdigest()[:16]
        if duplicate_episodes:
            diagnostics["rejections"].append({
                "signature_ref": signature_ref,
                "reason": "duplicate_episode",
                "episodes": sorted(duplicate_episodes),
            })
            continue
        if actual != expected_set:
            diagnostics["rejections"].append({
                "signature_ref": signature_ref,
                "reason": "incomplete_coverage",
                "missing": sorted(expected_set - actual),
                "extra": sorted(actual - expected_set),
            })
            continue

        fingerprint_payload = "\n".join(
            f"{episode}:{sha256(str(episode_resources[episode].get('url') or '').encode('utf-8')).hexdigest()}"
            for episode in expected
        )
        fingerprint = sha256(
            f"{target_season}|{signature}|{fingerprint_payload}".encode("utf-8")
        ).hexdigest()
        scores = list(group["scores"] or [0])
        score = min(int(value or 0) for value in scores)
        candidates.append(CompleteSeasonEd2kPack(
            season=target_season,
            expected_episodes=expected,
            signature=signature,
            signature_ref=signature_ref,
            fingerprint=fingerprint,
            score=score,
            episode_resources=episode_resources,
        ))

    diagnostics["complete_group_count"] = len(candidates)
    if not candidates:
        return None, diagnostics

    candidates.sort(
        key=lambda item: (item.score, item.signature_ref, item.fingerprint),
        reverse=True,
    )
    return candidates[0], diagnostics
