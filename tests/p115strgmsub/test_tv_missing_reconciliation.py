#!/usr/bin/env python3
from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import List, Set


REPO = Path(__file__).resolve().parents[2]
SYNC_PATH = REPO / "plugins.v2" / "p115strgmsub" / "handlers" / "sync.py"


def load_static_methods():
    source = SYNC_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(SYNC_PATH))
    wanted = {
        "_resolve_tv_missing_signal",
        "_tv_missing_strm_conflict",
        "_select_tv_delivery_episodes",
    }
    functions = []
    tv_source = ""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "SyncHandler":
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == "process_tv_subscribe":
                    tv_source = ast.get_source_segment(source, item) or ""
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in wanted:
                    item.decorator_list = []
                    functions.append(item)
    found = {item.name for item in functions}
    if found != wanted:
        raise RuntimeError(f"missing methods: {sorted(wanted - found)}")
    module = ast.Module(body=functions, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"List": List, "Set": Set}
    exec(compile(module, str(SYNC_PATH), "exec"), namespace)
    if not tv_source:
        raise RuntimeError("process_tv_subscribe source not found")
    return source, tv_source, namespace


SOURCE, TV_SOURCE, METHODS = load_static_methods()
resolve_signal = METHODS["_resolve_tv_missing_signal"]
strm_conflict = METHODS["_tv_missing_strm_conflict"]
select_delivery = METHODS["_select_tv_delivery_episodes"]


class TVMissingReconciliationTests(unittest.TestCase):
    def test_new_full_season_empty_detail_searches_all_episodes(self):
        expected = set(range(1, 11))
        missing, state = resolve_signal(
            mp_reported_satisfied=False,
            parsed_missing_episodes=[],
            reported_lack=10,
            expected_episodes=expected,
            is_best_version=False,
        )
        self.assertEqual(state, "reported_full_missing")
        self.assertEqual(missing, list(range(1, 11)))
        stop, present, reason = strm_conflict(
            expected_episodes=expected,
            mp_missing_episodes=missing,
            mp_missing_state=state,
        )
        self.assertFalse(stop)
        self.assertEqual(present, [])
        self.assertEqual(reason, "none")
        self.assertEqual(
            select_delivery(
                is_best_version=False,
                mp_target_episodes=missing,
                local_missing_episodes=missing,
            ),
            list(range(1, 11)),
        )
        resolve_pos = TV_SOURCE.index("mp_missing_episodes, mp_missing_state")
        gate_pos = TV_SOURCE.index("self._nextfind.gate_before_search")
        ayclub_loop_pos = TV_SOURCE.index("for source_index, source in enumerate")
        handoff_pos = TV_SOURCE.index("self._nextfind.handoff_after_ayclub")
        self.assertLess(resolve_pos, gate_pos)
        self.assertLess(gate_pos, ayclub_loop_pos)
        self.assertLess(ayclub_loop_pos, handoff_pos)

    def test_mp_explicit_satisfied_without_strm_still_stops(self):
        expected = set(range(1, 11))
        missing, state = resolve_signal(
            mp_reported_satisfied=True,
            parsed_missing_episodes=[],
            reported_lack=0,
            expected_episodes=expected,
            is_best_version=False,
        )
        self.assertEqual(state, "satisfied")
        stop, present, reason = strm_conflict(
            expected_episodes=expected,
            mp_missing_episodes=missing,
            mp_missing_state=state,
        )
        self.assertTrue(stop)
        self.assertEqual(present, list(range(1, 11)))
        self.assertEqual(reason, "mp_claimed_present")

    def test_partial_missing_keeps_only_3_5_7(self):
        expected = set(range(1, 11))
        missing, state = resolve_signal(
            mp_reported_satisfied=False,
            parsed_missing_episodes=[3, 5, 7],
            reported_lack=3,
            expected_episodes=expected,
            is_best_version=False,
        )
        self.assertEqual(state, "known_missing")
        self.assertEqual(missing, [3, 5, 7])
        self.assertEqual(
            select_delivery(
                is_best_version=False,
                mp_target_episodes=missing,
                local_missing_episodes=[3, 5, 7],
            ),
            [3, 5, 7],
        )

    def test_existing_strm_reconciliation_overrides_normal_subscription(self):
        self.assertEqual(
            select_delivery(
                is_best_version=False,
                mp_target_episodes=[3, 5, 7],
                local_missing_episodes=[2, 4],
            ),
            [2, 4],
        )

    def test_unavailable_strm_root_still_returns_before_search(self):
        marker = 'elif strm_status == "unavailable":'
        marker_pos = SOURCE.index(marker)
        return_pos = SOURCE.index("return transferred_count", marker_pos)
        select_pos = SOURCE.index(
            "missing_episodes = self._select_tv_delivery_episodes", return_pos
        )
        self.assertLess(marker_pos, return_pos)
        self.assertLess(return_pos, select_pos)

    def test_wash_subscription_never_uses_full_season_count_fallback(self):
        expected = set(range(1, 11))
        missing, state = resolve_signal(
            mp_reported_satisfied=False,
            parsed_missing_episodes=[],
            reported_lack=10,
            expected_episodes=expected,
            is_best_version=True,
        )
        self.assertEqual(state, "unknown")
        self.assertEqual(missing, [])
        self.assertEqual(
            select_delivery(
                is_best_version=True,
                mp_target_episodes=missing,
                local_missing_episodes=list(range(1, 11)),
            ),
            [],
        )

    def test_unknown_partial_count_without_detail_safely_stops(self):
        expected = set(range(1, 11))
        missing, state = resolve_signal(
            mp_reported_satisfied=False,
            parsed_missing_episodes=[],
            reported_lack=3,
            expected_episodes=expected,
            is_best_version=False,
        )
        self.assertEqual(state, "unknown")
        stop, present, reason = strm_conflict(
            expected_episodes=expected,
            mp_missing_episodes=missing,
            mp_missing_state=state,
        )
        self.assertTrue(stop)
        self.assertEqual(present, [])
        self.assertEqual(reason, "unknown_missing_detail")

    def test_partial_mp_claim_without_strm_keeps_existing_safety_stop(self):
        expected = set(range(1, 11))
        stop, present, reason = strm_conflict(
            expected_episodes=expected,
            mp_missing_episodes=[3, 5, 7],
            mp_missing_state="known_missing",
        )
        self.assertTrue(stop)
        self.assertEqual(present, [1, 2, 4, 6, 8, 9, 10])
        self.assertEqual(reason, "mp_claimed_present")


if __name__ == "__main__":
    unittest.main(verbosity=2)
