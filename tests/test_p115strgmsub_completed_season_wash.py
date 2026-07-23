from __future__ import annotations

import importlib.util
from pathlib import Path
import random
import sys
import unittest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "plugins.v2/p115strgmsub/handlers/season_wash.py"
)
spec = importlib.util.spec_from_file_location("season_wash", MODULE_PATH)
season_wash = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = season_wash
spec.loader.exec_module(season_wash)


def resource(
    episode: int,
    *,
    season: int = 1,
    group: str = "GROUP",
    quality: str = "2160p",
):
    filename = (
        f"[示例].Example.2026.S{season:02d}E{episode:02d}."
        f"{quality}.WEB-DL.HEVC.HDR-{group}.mkv"
    )
    url = f"ed2k://|file|{filename}|{1000000 + episode}|{'a' * 32}|/"
    return {
        "url": url,
        "title": filename,
        "source_kind": "ed2k",
        "season": season,
        "episode": episode,
        "episodes": [episode],
        "score": 100 if quality == "2160p" else 20,
    }


class CompleteSeasonWashTests(unittest.TestCase):
    def test_uniform_28_episode_pack_is_accepted(self):
        pack, diagnostics = season_wash.select_complete_uniform_ed2k_pack(
            [resource(ep) for ep in range(1, 29)],
            season=1,
            expected_episodes=range(1, 29),
            score_func=lambda item: item["score"],
        )
        self.assertIsNotNone(pack)
        self.assertEqual(pack.episodes, list(range(1, 29)))
        self.assertEqual(diagnostics["complete_group_count"], 1)

    def test_missing_episode_is_rejected(self):
        pack, _ = season_wash.select_complete_uniform_ed2k_pack(
            [resource(ep) for ep in range(1, 28)],
            season=1,
            expected_episodes=range(1, 29),
        )
        self.assertIsNone(pack)

    def test_mixed_release_groups_are_not_combined(self):
        items = [resource(ep, group="A") for ep in range(1, 15)]
        items += [resource(ep, group="B") for ep in range(15, 29)]
        pack, _ = season_wash.select_complete_uniform_ed2k_pack(
            items,
            season=1,
            expected_episodes=range(1, 29),
        )
        self.assertIsNone(pack)

    def test_duplicate_episode_with_different_url_rejects_group(self):
        items = [resource(ep) for ep in range(1, 29)]
        duplicate = resource(5)
        duplicate["url"] = duplicate["url"].replace("a" * 32, "b" * 32)
        items.append(duplicate)
        pack, diagnostics = season_wash.select_complete_uniform_ed2k_pack(
            items,
            season=1,
            expected_episodes=range(1, 29),
        )
        self.assertIsNone(pack)
        self.assertTrue(any(
            isinstance(item, dict) and item.get("reason") == "duplicate_episode"
            for item in diagnostics["rejections"]
        ))

    def test_wrong_season_is_rejected(self):
        pack, _ = season_wash.select_complete_uniform_ed2k_pack(
            [resource(ep, season=2) for ep in range(1, 29)],
            season=1,
            expected_episodes=range(1, 29),
        )
        self.assertIsNone(pack)

    def test_resolution_digits_are_not_episode_numbers(self):
        item = resource(17)
        identity = season_wash.filename_identity(
            season_wash.ed2k_filename(item["url"])
        )
        self.assertEqual(identity[:2], (1, 17))

    def test_fingerprint_is_stable_across_input_order(self):
        items = [resource(ep) for ep in range(1, 29)]
        first, _ = season_wash.select_complete_uniform_ed2k_pack(
            items, season=1, expected_episodes=range(1, 29)
        )
        random.Random(42).shuffle(items)
        second, _ = season_wash.select_complete_uniform_ed2k_pack(
            items, season=1, expected_episodes=range(1, 29)
        )
        self.assertEqual(first.fingerprint, second.fingerprint)

    def test_higher_score_complete_pack_wins(self):
        items = [
            resource(ep, group="LOW", quality="1080p")
            for ep in range(1, 29)
        ]
        items += [
            resource(ep, group="HIGH", quality="2160p")
            for ep in range(1, 29)
        ]
        pack, _ = season_wash.select_complete_uniform_ed2k_pack(
            items,
            season=1,
            expected_episodes=range(1, 29),
            score_func=lambda item: item["score"],
        )
        self.assertIsNotNone(pack)
        self.assertIn("2160p", pack.signature)


if __name__ == "__main__":
    unittest.main(verbosity=2)
