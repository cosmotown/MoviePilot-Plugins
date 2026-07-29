from __future__ import annotations

import ast
import json
from pathlib import Path
import types
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
INIT_PATH = REPO_ROOT / "plugins.v2/p115strgmsub/__init__.py"
SYNC_PATH = REPO_ROOT / "plugins.v2/p115strgmsub/handlers/sync.py"
LIFECYCLE_PATH = REPO_ROOT / "plugins.v2/p115strgmsub/handlers/lifecycle.py"
PACKAGE_PATH = REPO_ROOT / "package.v2.json"


def _method_node(path: Path, class_name: str, method_name: str):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    class_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return next(
        node for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    )


def _decorator_names(path: Path, class_name: str, method_name: str):
    method = _method_node(path, class_name, method_name)
    names = []
    for decorator in method.decorator_list:
        if isinstance(decorator, ast.Name):
            names.append(decorator.id)
        elif isinstance(decorator, ast.Attribute):
            names.append(decorator.attr)
        else:
            names.append(ast.dump(decorator))
    return names


def _extract_method(path: Path, class_name: str, method_name: str):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    class_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    method = next(
        node for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    )
    method.decorator_list = []
    method.returns = None
    for argument in list(method.args.args) + list(method.args.kwonlyargs):
        argument.annotation = None
    module = ast.Module(body=[method], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, str(path), "exec"), namespace)
    return namespace[method_name]


class WashAndPtGuardTests(unittest.TestCase):
    def test_decorators_are_preserved_around_insertions(self):
        self.assertEqual(
            _decorator_names(
                INIT_PATH, "P115StrgmSub", "_on_plugin_pending_created"
            ),
            [],
        )
        self.assertEqual(
            _decorator_names(
                INIT_PATH, "P115StrgmSub", "_pt_gate_task_label"
            ),
            ["staticmethod"],
        )
        self.assertEqual(
            _decorator_names(
                SYNC_PATH, "SyncHandler", "_select_tv_delivery_episodes"
            ),
            ["staticmethod"],
        )
        self.assertEqual(
            _decorator_names(
                SYNC_PATH, "SyncHandler", "_resource_episode_set"
            ),
            ["staticmethod"],
        )

    def test_unfiltered_wash_is_not_marked_terminal_quality(self):
        source = SYNC_PATH.read_text(encoding="utf-8")
        self.assertIn("ed2k_perfect = not is_best_version", source)
        self.assertIn("else not is_best_version", source)

    def test_wash_targets_use_mp_not_local_strm(self):
        selector = _extract_method(SYNC_PATH, "SyncHandler", "_select_tv_delivery_episodes")
        self.assertEqual(
            selector(
                is_best_version=True,
                mp_target_episodes=[1, 2, 3],
                local_missing_episodes=[],
            ),
            [1, 2, 3],
        )

    def test_normal_targets_still_use_local_strm(self):
        selector = _extract_method(SYNC_PATH, "SyncHandler", "_select_tv_delivery_episodes")
        self.assertEqual(
            selector(
                is_best_version=False,
                mp_target_episodes=[1, 2, 3],
                local_missing_episodes=[3],
            ),
            [3],
        )

    def test_mp_facts_are_never_overwritten_by_local_missing(self):
        source = SYNC_PATH.read_text(encoding="utf-8")
        self.assertNotIn("mp_missing_episodes = list(local_missing)", source)
        self.assertIn("mp_target_episodes=mp_missing_episodes", source)
        self.assertIn("local_missing_episodes=local_missing_episodes", source)

    def test_active_wash_candidates_require_perfect_match(self):
        source = SYNC_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(SYNC_PATH))
        conditions = [
            ast.unparse(node.test)
            for node in ast.walk(tree)
            if isinstance(node, ast.If)
        ]

        def contains_condition(*markers):
            return any(
                all(marker in condition for marker in markers)
                for condition in conditions
            )

        self.assertTrue(
            contains_condition(
                "is_best_version",
                "subscribe_filter.has_filters()",
                "not is_perfect",
            )
        )
        self.assertTrue(
            contains_condition(
                "is_best_version",
                "subscribe_filter.has_filters()",
                "not ed2k_perfect",
            )
        )

    def test_pending_creation_notifies_pt_guard(self):
        source = SYNC_PATH.read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("self._notify_pending_created("), 3)
        init_source = INIT_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "on_pending_created_func=self._on_plugin_pending_created",
            init_source,
        )

    def test_pt_reblock_callback_closes_and_reschedules(self):
        method = _extract_method(
            INIT_PATH,
            "P115StrgmSub",
            "_on_plugin_pending_created",
        )

        class Stub:
            _enabled = True
            _block_system_subscribe = False

            def __init__(self):
                self.events = []

            def _window_enabled(self):
                return True

            def _enter_blocked(self, reason):
                self.events.append(("blocked", reason))
                self._block_system_subscribe = True

            def _schedule_unblock_after_delay(self, _base_time):
                self.events.append(("gate", True))

        namespace = method.__globals__
        namespace["datetime"] = types.SimpleNamespace(
            datetime=types.SimpleNamespace(now=lambda tz=None: "now")
        )
        namespace["pytz"] = types.SimpleNamespace(timezone=lambda _name: None)
        namespace["settings"] = types.SimpleNamespace(TZ="Asia/Shanghai")
        namespace["logger"] = types.SimpleNamespace(
            warning=lambda *_args, **_kwargs: None,
            error=lambda *_args, **_kwargs: None,
        )

        stub = Stub()
        method(stub, subscribe_id=321, task_ids=["a"], source="ayclub")
        self.assertEqual(stub.events[0][0], "blocked")
        self.assertEqual(stub.events[1], ("gate", True))

    def test_lifecycle_persists_wash_quality_metadata(self):
        source = LIFECYCLE_PATH.read_text(encoding="utf-8")
        self.assertIn('"perfect_match": bool(item.get("is_perfect"))', source)
        self.assertIn('"best_version": bool(', source)
        self.assertIn("def best_version_terminal_episodes(", source)

    def test_transfer_complete_backfills_official_quality_facts(self):
        source = INIT_PATH.read_text(encoding="utf-8")
        self.assertIn("backfill_existing_episodes(", source)
        self.assertIn("priority=100", source)
        self.assertIn('{"current_priority": 100}', source)
        self.assertIn("check_and_handle_existing_media(", source)

    def test_status_only_guard_is_retained(self):
        source = INIT_PATH.read_text(encoding="utf-8")
        self.assertIn('if scene.strip().casefold() == "status":', source)
        self.assertIn("status-only 事件仅记录", source)

    def test_version_and_history_are_synchronized(self):
        source = INIT_PATH.read_text(encoding="utf-8")
        self.assertIn('plugin_version = "1.9.12"', source)
        package = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
        plugin = package["P115StrgmSub"]
        self.assertEqual(plugin["version"], "1.9.12")
        self.assertIn("v1.9.12", plugin["history"])
        self.assertIn("PT", plugin["history"]["v1.9.12"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
