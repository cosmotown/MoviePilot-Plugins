from __future__ import annotations

import ast
import copy
import json
from pathlib import Path
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_PATH = REPO_ROOT / "plugins.v2/p115strgmsub/__init__.py"
PACKAGE_PATH = REPO_ROOT / "package.v2.json"


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(str(message))


class _Lifecycle:
    def __init__(self):
        self.calls = []

    def on_modified(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return {"generation": 7}


def _load_exact_handler():
    source = PLUGIN_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(PLUGIN_PATH))
    plugin_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "P115StrgmSub"
    )
    method = next(
        node
        for node in plugin_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "on_subscribe_modified"
    )
    method = copy.deepcopy(method)
    method.decorator_list = []
    method.returns = None
    for argument in list(method.args.args) + list(method.args.kwonlyargs):
        argument.annotation = None

    module = ast.Module(body=[method], type_ignores=[])
    ast.fix_missing_locations(module)
    logger = _Logger()
    namespace = {"logger": logger}
    exec(compile(module, str(PLUGIN_PATH), "exec"), namespace)
    return namespace["on_subscribe_modified"], logger


class _Plugin:
    def __init__(self, handler, logger, *, sid=312, excluded=False):
        self.on_subscribe_modified = types.MethodType(handler, self)
        self._sid = sid
        self._excluded = excluded
        self._logger = logger
        self._lifecycle_store = _Lifecycle()
        self.init_count = 0
        self.schedules = []
        self.invalidations = []

    def _get_subscribe_id_from_event(self, _event):
        return self._sid

    def _is_subscribe_excluded(self, _sid):
        return self._excluded

    def _init_lifecycle_store(self):
        self.init_count += 1

    def _schedule_lifecycle_sync(self, reason, subscribe_ids=None):
        self.schedules.append((reason, subscribe_ids))

    def _invalidate_subscribe_caches(self, subscribe_info):
        self.invalidations.append(subscribe_info)


class StatusEventGuardTests(unittest.TestCase):
    def setUp(self):
        self.handler, self.logger = _load_exact_handler()

    def _run(self, scene, fields=None):
        plugin = _Plugin(self.handler, self.logger)
        event = types.SimpleNamespace(
            event_data={
                "subscribe_id": 312,
                "scene": scene,
                "fields": list(fields or []),
                "subscribe_info": {"id": 312, "state": "R"},
            }
        )
        plugin.on_subscribe_modified(event)
        return plugin

    def test_status_event_does_not_touch_lifecycle_or_schedule(self):
        plugin = self._run("status", ["state"])
        self.assertEqual(plugin.init_count, 0)
        self.assertEqual(plugin._lifecycle_store.calls, [])
        self.assertEqual(plugin.schedules, [])
        self.assertTrue(
            any("status-only" in message for message in self.logger.messages)
        )

    def test_status_matching_is_case_and_whitespace_insensitive(self):
        for scene in ("STATUS", " status ", "\tStatus\n"):
            with self.subTest(scene=scene):
                handler, logger = _load_exact_handler()
                plugin = _Plugin(handler, logger)
                event = types.SimpleNamespace(
                    event_data={
                        "subscribe_id": 312,
                        "scene": scene,
                        "fields": ["state"],
                    }
                )
                plugin.on_subscribe_modified(event)
                self.assertEqual(plugin.init_count, 0)
                self.assertEqual(plugin._lifecycle_store.calls, [])
                self.assertEqual(plugin.schedules, [])

    def test_normal_update_keeps_existing_targeted_sync(self):
        plugin = self._run("update", ["quality"])
        self.assertEqual(plugin.init_count, 1)
        self.assertEqual(len(plugin._lifecycle_store.calls), 1)
        self.assertEqual(
            plugin.schedules,
            [("订阅修改 312/update", [312])],
        )

    def test_reset_keeps_existing_refresh_path(self):
        plugin = self._run("reset", ["state"])
        self.assertEqual(plugin.init_count, 1)
        self.assertEqual(len(plugin._lifecycle_store.calls), 1)
        self.assertEqual(
            plugin.schedules,
            [("订阅修改 312/reset", [312])],
        )
        self.assertEqual(
            plugin.invalidations,
            [{"id": 312, "state": "R"}],
        )

    def test_excluded_subscription_remains_ignored(self):
        plugin = _Plugin(
            self.handler,
            self.logger,
            sid=312,
            excluded=True,
        )
        event = types.SimpleNamespace(
            event_data={"subscribe_id": 312, "scene": "update"}
        )
        plugin.on_subscribe_modified(event)
        self.assertEqual(plugin.init_count, 0)
        self.assertEqual(plugin._lifecycle_store.calls, [])
        self.assertEqual(plugin.schedules, [])

    def test_missing_subscription_id_remains_ignored(self):
        plugin = _Plugin(
            self.handler,
            self.logger,
            sid=None,
        )
        event = types.SimpleNamespace(event_data={"scene": "update"})
        plugin.on_subscribe_modified(event)
        self.assertEqual(plugin.init_count, 0)
        self.assertEqual(plugin._lifecycle_store.calls, [])
        self.assertEqual(plugin.schedules, [])

    def test_version_and_market_history_are_synchronized(self):
        source = PLUGIN_PATH.read_text(encoding="utf-8")
        self.assertIn('plugin_version = "1.9.11"', source)
        package = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
        plugin = package["P115StrgmSub"]
        self.assertEqual(plugin["version"], "1.9.11")
        self.assertIn("v1.9.11", plugin["history"])
        self.assertIn("status-only", plugin["history"]["v1.9.11"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
