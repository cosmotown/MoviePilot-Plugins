import datetime
import importlib.util
import sys
import types
import unittest
from pathlib import Path


class _UtcZone(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def localize(self, value):
        return value.replace(tzinfo=self)


UTC = _UtcZone()


class _Logger:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class _Movie:
    def watch_providers(self, tmdb_id):
        return {}

    def release_dates(self, tmdb_id):
        return []

    def close(self):
        return None


def _install_stubs():
    pytz = types.ModuleType("pytz")
    pytz.UTC = UTC
    pytz.timezone = lambda name: UTC
    sys.modules["pytz"] = pytz

    app = types.ModuleType("app")
    app_core = types.ModuleType("app.core")
    app_config = types.ModuleType("app.core.config")
    app_config.settings = types.SimpleNamespace(TZ="UTC")
    app_log = types.ModuleType("app.log")
    app_log.logger = _Logger()
    app_modules = types.ModuleType("app.modules")
    app_tmdb = types.ModuleType("app.modules.themoviedb")
    app_tmdb_api = types.ModuleType("app.modules.themoviedb.tmdbv3api")
    app_tmdb_api.Movie = _Movie
    sys.modules.update({
        "app": app,
        "app.core": app_core,
        "app.core.config": app_config,
        "app.log": app_log,
        "app.modules": app_modules,
        "app.modules.themoviedb": app_tmdb,
        "app.modules.themoviedb.tmdbv3api": app_tmdb_api,
    })


def _load_release_gate():
    _install_stubs()
    path = (
        Path(__file__).resolve().parents[1]
        / "plugins.v2"
        / "p115strgmsub"
        / "handlers"
        / "release_gate.py"
    )
    spec = importlib.util.spec_from_file_location(
        "p115strgmsub_release_gate_test",
        path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ReleaseGateStore


ReleaseGateStore = _load_release_gate()


class FixedClockReleaseGate(ReleaseGateStore):
    def __init__(self, current):
        self.data = {}
        self.current = current
        super().__init__(
            get_data_func=lambda key: self.data.get(key),
            save_data_func=lambda key, value: self.data.__setitem__(key, value),
        )

    def now(self):
        return self.current

    def set_time(self, hour, minute=0, *, day=1):
        self.current = datetime.datetime(
            2026,
            7,
            day,
            hour,
            minute,
            tzinfo=UTC,
        )


class MovieLifecycleBackoffTest(unittest.TestCase):
    TMDB_ID = 123456

    def setUp(self):
        self.gate = FixedClockReleaseGate(
            datetime.datetime(2026, 7, 1, 0, 44, tzinfo=UTC)
        )
        state = self.gate.get_movie(self.TMDB_ID)
        state["last_tmdb_check_date"] = "2026-07-01"
        state["last_tmdb_check_status"] = "ok"
        state["released"] = True
        state["digital_release_date"] = "2026-07-01"
        self.gate.save(state)

    def test_0044_failure_blocks_0500_and_1300_telegram_search(self):
        first = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
        )
        self.assertTrue(first["force_refresh"])
        self.assertEqual(first["reason"], "lifecycle_force_refresh")
        self.assertTrue(
            self.gate.reserve_movie_real_search(
                self.TMDB_ID,
                first["reason"],
                lifecycle_force_refresh=True,
                request_id="request-0044",
            )
        )
        self.gate.mark_movie_search_result(
            self.TMDB_ID,
            search_status="http_error",
            cached=None,
            attempt_reserved=True,
            request_id="request-0044",
        )

        self.gate.set_time(5)
        at_0500 = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
        )
        self.assertFalse(at_0500["force_refresh"])
        self.assertTrue(at_0500["cache_only"])
        self.assertEqual(at_0500["reason"], "retry_after_wait")

        self.gate.set_time(13)
        at_1300 = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
            scheduled_evening_refresh=True,
        )
        self.assertFalse(at_1300["force_refresh"])
        self.assertTrue(at_1300["cache_only"])
        self.assertEqual(at_1300["reason"], "daily_search_limit")
        self.assertEqual(at_1300["daily_search_count"], 1)
        self.assertFalse(
            self.gate.reserve_movie_real_search(
                self.TMDB_ID,
                "scheduled_evening_refresh",
                lifecycle_force_refresh=True,
                request_id="request-1300",
            )
        )

        self.gate.set_time(21, 30, day=2)
        next_day = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
            scheduled_evening_refresh=True,
        )
        self.assertTrue(next_day["force_refresh"])
        self.assertEqual(next_day["daily_search_count"], 0)

    def test_successful_no_result_sets_cooldown_and_clears_pending(self):
        decision = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
        )
        self.gate.reserve_movie_real_search(
            self.TMDB_ID,
            decision["reason"],
            lifecycle_force_refresh=True,
            request_id="request-empty",
        )
        self.gate.mark_movie_search_result(
            self.TMDB_ID,
            search_status="ok_empty",
            cached=False,
            force_honored=True,
            attempt_reserved=True,
            request_id="request-empty",
        )
        state = self.gate.get_movie(self.TMDB_ID)
        self.assertFalse(state["force_refresh_pending"])
        self.assertIsNone(state["retry_after"])
        self.assertIsNotNone(state["no_result_cooldown_until"])
        self.assertGreaterEqual(
            self.gate._parse_datetime(state["no_result_cooldown_until"]),
            self.gate.now() + datetime.timedelta(hours=24),
        )

    def test_late_empty_cache_is_terminal_without_new_search(self):
        state = self.gate.get_movie(self.TMDB_ID)
        state["force_refresh_pending"] = True
        state["daily_search_date"] = "2026-07-01"
        state["daily_search_count"] = 1
        state["last_real_search_at"] = self.gate.now().isoformat()
        self.gate.save(state)

        recorded = self.gate.mark_movie_search_result(
            self.TMDB_ID,
            search_status="cached_empty",
            cached=True,
            late_reply=True,
            request_id="request-late",
        )
        self.assertTrue(recorded)
        state = self.gate.get_movie(self.TMDB_ID)
        self.assertFalse(state["force_refresh_pending"])
        self.assertEqual(state["daily_search_count"], 1)
        self.assertIsNotNone(state["no_result_cooldown_until"])

    def test_unreleased_movie_uses_later_release_date_for_cooldown(self):
        state = self.gate.get_movie(self.TMDB_ID)
        state["released"] = False
        state["release_signal"] = None
        state["next_known_release_date"] = "2026-07-20"
        self.gate.save(state)
        self.gate.reserve_movie_real_search(
            self.TMDB_ID,
            "lifecycle_force_refresh",
            lifecycle_force_refresh=True,
            request_id="request-unreleased",
        )
        self.gate.mark_movie_search_result(
            self.TMDB_ID,
            search_status="ok_empty",
            cached=False,
            attempt_reserved=True,
            request_id="request-unreleased",
        )
        state = self.gate.get_movie(self.TMDB_ID)
        self.assertEqual(
            self.gate._parse_datetime(
                state["no_result_cooldown_until"]
            ).date(),
            datetime.date(2026, 7, 20),
        )

    def test_explicit_manual_force_can_bypass_daily_limit(self):
        state = self.gate.get_movie(self.TMDB_ID)
        state["daily_search_date"] = "2026-07-01"
        state["daily_search_count"] = 1
        state["retry_after"] = (
            self.gate.now() + datetime.timedelta(hours=6)
        ).isoformat()
        self.gate.save(state)

        decision = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
            explicit_manual_force_refresh=True,
        )
        self.assertTrue(decision["force_refresh"])
        self.assertEqual(decision["reason"], "explicit_manual_force_refresh")
        self.assertTrue(
            self.gate.reserve_movie_real_search(
                self.TMDB_ID,
                decision["reason"],
                lifecycle_force_refresh=True,
                explicit_manual_force_refresh=True,
                request_id="request-manual",
            )
        )

    def test_movie_real_search_requires_manual_full_authorization(self):
        # 明确手动授权与旧 22:00-24:00 窗口无关，白天同样可用。
        self.gate.set_time(13)

        targeted = self.gate.evaluate_movie(
            self.TMDB_ID,
            query_origin="targeted_lifecycle",
        )
        self.assertFalse(targeted["force_refresh"])
        self.assertTrue(targeted["cache_only"])
        self.assertEqual(
            targeted["reason"],
            "released_cache_only_trigger_not_authorized",
        )

        manual_full = self.gate.evaluate_movie(
            self.TMDB_ID,
            query_origin="manual_or_api_full",
        )
        self.assertTrue(manual_full["force_refresh"])
        self.assertFalse(manual_full["cache_only"])
        self.assertEqual(
            manual_full["reason"],
            "released_search_due_explicit_manual_refresh",
        )

    def test_movie_targeted_lifecycle_force_still_has_priority(self):
        self.gate.set_time(13)

        decision = self.gate.evaluate_movie(
            self.TMDB_ID,
            lifecycle_force_refresh=True,
            query_origin="targeted_lifecycle",
        )

        self.assertTrue(decision["force_refresh"])
        self.assertFalse(decision["cache_only"])
        self.assertEqual(decision["reason"], "lifecycle_force_refresh")


if __name__ == "__main__":
    unittest.main()
