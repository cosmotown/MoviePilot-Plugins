import ast
import copy
import datetime as std_datetime
import threading
import types
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional


class _UtcZone(std_datetime.tzinfo):
    def utcoffset(self, dt):
        return std_datetime.timedelta(0)

    def dst(self, dt):
        return std_datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def localize(self, value):
        return value.replace(tzinfo=self)


UTC = _UtcZone()


class _FrozenDateTime(std_datetime.datetime):
    current = std_datetime.datetime(2026, 7, 23, 21, 0, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


class _CronExpression:
    def __init__(self, expression, timezone):
        fields = str(expression or "").split()
        if len(fields) != 5:
            raise ValueError("invalid cron expression")
        try:
            self.minute = int(fields[0])
            self.hours = sorted(int(value) for value in fields[1].split(","))
        except (TypeError, ValueError) as error:
            raise ValueError("unsupported cron expression") from error
        self.timezone = timezone

    def get_next_fire_time(self, previous_fire_time, now):
        current = now.astimezone(self.timezone)
        for day_offset in range(3):
            date_value = current.date() + std_datetime.timedelta(days=day_offset)
            for hour in self.hours:
                candidate = std_datetime.datetime(
                    date_value.year,
                    date_value.month,
                    date_value.day,
                    hour,
                    self.minute,
                    tzinfo=self.timezone,
                )
                if candidate > current:
                    return candidate
        return None


class _CronTrigger:
    @classmethod
    def from_crontab(cls, expression, timezone):
        return _CronExpression(expression, timezone)


class _Logger:
    def __init__(self):
        self.messages = []

    def __getattr__(self, name):
        return lambda message, *args, **kwargs: self.messages.append(str(message))


LOGGER = _Logger()
FAKE_DATETIME = types.SimpleNamespace(
    datetime=_FrozenDateTime,
    timedelta=std_datetime.timedelta,
)
PYTZ = types.SimpleNamespace(timezone=lambda name: UTC)
SETTINGS = types.SimpleNamespace(TZ="UTC")


def _source_method(path: Path, method_name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for member in node.body:
            if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if member.name == method_name:
                    return copy.deepcopy(member)
    raise AssertionError(f"method not found: {path}:{method_name}")


def _load_real_methods():
    root = Path(__file__).resolve().parents[1]
    plugin_path = root / "plugins.v2" / "p115strgmsub" / "__init__.py"
    sync_path = (
        root
        / "plugins.v2"
        / "p115strgmsub"
        / "handlers"
        / "sync.py"
    )
    namespace = {
        "Any": Any,
        "CronTrigger": _CronTrigger,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "datetime": FAKE_DATETIME,
        "lock": threading.Lock(),
        "logger": LOGGER,
        "pytz": PYTZ,
        "settings": SETTINGS,
    }
    method_sources = (
        (plugin_path, "_is_last_run_today"),
        (plugin_path, "scheduled_sync_subscribes"),
        (plugin_path, "get_service"),
        (plugin_path, "sync_subscribes"),
        (sync_path, "_ayclub_tv_query_mode"),
    )
    for path, method_name in method_sources:
        method = _source_method(path, method_name)
        method.decorator_list = []
        module = ast.Module(body=[method], type_ignores=[])
        ast.fix_missing_locations(module)
        exec(compile(module, str(path), "exec"), namespace)
    return namespace


REAL_METHODS = _load_real_methods()


class _ScheduledEntryHarness:
    _MIN_INTERVAL_HOURS = 2

    _is_last_run_today = REAL_METHODS["_is_last_run_today"]
    scheduled_sync_subscribes = REAL_METHODS["scheduled_sync_subscribes"]
    get_service = REAL_METHODS["get_service"]
    sync_subscribes = REAL_METHODS["sync_subscribes"]
    _ayclub_tv_query_mode = REAL_METHODS["_ayclub_tv_query_mode"]

    def __init__(self):
        self._enabled = True
        self._cron = "0 5,13,21 * * *"
        self._block_system_subscribe = False
        self._unblock_delay_minutes = 0
        self.sync_calls = []

    def _cron_interval_ge_min_hours(self, expression, minimum_hours):
        return True

    def _do_sync(
        self,
        target_subscribe_ids=None,
        trigger_reason="",
        scheduled_evening_refresh=False,
        query_origin="unknown",
    ):
        self.sync_calls.append({
            "target_subscribe_ids": target_subscribe_ids,
            "trigger_reason": trigger_reason,
            "scheduled_evening_refresh": scheduled_evening_refresh,
            "query_origin": query_origin,
        })
        return True

    def _window_enabled(self):
        return False

    def _enter_blocked(self, reason):
        raise AssertionError(f"unexpected PT-window mutation: {reason}")

    def _schedule_unblock_after_delay(self, now):
        raise AssertionError("unexpected PT-window schedule")


class ScheduledServiceEntryTest(unittest.TestCase):
    def setUp(self):
        LOGGER.messages.clear()
        self.plugin = _ScheduledEntryHarness()

    def set_time(self, hour, minute=0):
        _FrozenDateTime.current = std_datetime.datetime(
            2026,
            7,
            23,
            hour,
            minute,
            tzinfo=UTC,
        )

    def test_2100_registered_service_without_business_kwargs_is_last_run(self):
        self.set_time(21)
        service = self.plugin.get_service()[0]

        self.assertNotIn("kwargs", service)
        service["func"]()

        self.assertEqual(
            self.plugin.sync_calls[-1],
            {
                "target_subscribe_ids": None,
                "trigger_reason": "scheduled_cron",
                "scheduled_evening_refresh": True,
                "query_origin": "scheduled_cron",
            },
        )
        self.assertTrue(
            any(
                "scheduled_evening_refresh=True" in message
                for message in LOGGER.messages
            )
        )

    def test_1300_registered_service_remains_daytime_cache_round(self):
        self.set_time(13)
        self.plugin.get_service()[0]["func"]()

        self.assertEqual(
            self.plugin.sync_calls[-1]["trigger_reason"],
            "scheduled_cron",
        )
        self.assertFalse(
            self.plugin.sync_calls[-1]["scheduled_evening_refresh"]
        )
        self.assertEqual(
            self.plugin.sync_calls[-1]["query_origin"],
            "scheduled_cron",
        )

    def test_changed_cron_moves_real_search_to_its_new_last_round(self):
        self.plugin._cron = "30 6,14,22 * * *"

        self.set_time(14, 30)
        self.plugin.get_service()[0]["func"]()
        daytime = self.plugin.sync_calls[-1]
        self.assertFalse(daytime["scheduled_evening_refresh"])
        self.assertEqual(daytime["query_origin"], "scheduled_cron")

        self.set_time(22, 30)
        self.plugin.get_service()[0]["func"]()
        last_round = self.plugin.sync_calls[-1]
        self.assertTrue(last_round["scheduled_evening_refresh"])
        self.assertEqual(last_round["query_origin"], "scheduled_cron")

    def test_invalid_cron_never_falls_back_to_fixed_2300_refresh(self):
        self.plugin._cron = "invalid cron"
        self.set_time(23)

        service = self.plugin.get_service()[0]
        self.assertEqual(service["trigger"], "interval")
        service["func"]()

        decision = self.plugin.sync_calls[-1]
        self.assertFalse(decision["scheduled_evening_refresh"])
        self.assertEqual(decision["query_origin"], "scheduled_cron")

    def test_tv_last_round_is_real_search_and_daytime_is_cache_only(self):
        self.plugin._ayclub_local_now = lambda: _FrozenDateTime.current
        self.plugin._ayclub_daily_refresh_due = lambda **kwargs: True

        evening = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=True,
            query_origin="scheduled_cron",
        )
        daytime = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=False,
            query_origin="scheduled_cron",
        )

        self.assertEqual(
            evening,
            (True, False, "scheduled_evening_refresh"),
        )
        self.assertEqual(
            daytime,
            (False, True, "cache_only_trigger_not_authorized"),
        )

    def test_manual_and_onlyonce_sync_do_not_impersonate_cron(self):
        self.set_time(21)

        self.plugin.sync_subscribes()
        self.plugin.sync_subscribes(trigger_reason="manual_once")

        self.assertFalse(
            self.plugin.sync_calls[-2]["scheduled_evening_refresh"]
        )
        self.assertFalse(
            self.plugin.sync_calls[-1]["scheduled_evening_refresh"]
        )
        self.assertEqual(
            self.plugin.sync_calls[-2]["query_origin"],
            "manual_or_api_full",
        )
        self.assertEqual(
            self.plugin.sync_calls[-1]["query_origin"],
            "manual_or_api_full",
        )

    def test_targeted_lifecycle_sync_does_not_impersonate_last_round(self):
        self.set_time(21)
        self.plugin.sync_subscribes(
            target_subscribe_ids=[123],
            trigger_reason="新增/重新订阅 123",
        )

        self.assertEqual(
            self.plugin.sync_calls[-1]["target_subscribe_ids"],
            [123],
        )
        self.assertFalse(
            self.plugin.sync_calls[-1]["scheduled_evening_refresh"]
        )
        self.assertEqual(
            self.plugin.sync_calls[-1]["query_origin"],
            "targeted_lifecycle",
        )

    def test_interval_fallback_keeps_trigger_kwargs_and_uses_wrapper(self):
        self.plugin._cron_interval_ge_min_hours = (
            lambda expression, minimum_hours: False
        )
        service = self.plugin.get_service()[0]

        self.assertEqual(service["trigger"], "interval")
        self.assertEqual(service["kwargs"], {"hours": 8})
        self.assertEqual(
            service["func"].__func__,
            _ScheduledEntryHarness.scheduled_sync_subscribes,
        )

    def _configure_tv_query_gate(self, *, due=True):
        self.plugin._ayclub_local_now = lambda: _FrozenDateTime.current
        self.plugin._ayclub_daily_refresh_due = lambda **kwargs: due

    def test_2346_status_targeted_sync_is_cache_only(self):
        self.set_time(23, 46)
        self._configure_tv_query_gate()

        self.plugin.sync_subscribes(
            target_subscribe_ids=[312],
            trigger_reason="订阅修改 312/status",
        )
        origin = self.plugin.sync_calls[-1]
        decision = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=origin["scheduled_evening_refresh"],
            query_origin=origin["query_origin"],
        )

        self.assertEqual(
            decision,
            (False, True, "cache_only_trigger_not_authorized"),
        )

    def test_manual_full_sync_is_explicitly_authorized(self):
        self.set_time(23, 46)
        self._configure_tv_query_gate()

        self.plugin.sync_subscribes()
        origin = self.plugin.sync_calls[-1]
        decision = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=origin["scheduled_evening_refresh"],
            query_origin=origin["query_origin"],
        )

        self.assertEqual(
            decision,
            (True, False, "explicit_manual_refresh"),
        )

    def test_daytime_manual_full_does_not_depend_on_old_window(self):
        self.set_time(13)
        self._configure_tv_query_gate()

        decision = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=False,
            query_origin="manual_or_api_full",
        )

        self.assertEqual(
            decision,
            (True, False, "explicit_manual_refresh"),
        )

    def test_daytime_lifecycle_force_refresh_remains_authorized(self):
        self.set_time(13)
        self._configure_tv_query_gate()

        decision = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=True,
            scheduled_evening_refresh=False,
            query_origin="targeted_lifecycle",
        )

        self.assertEqual(
            decision,
            (True, False, "lifecycle_force_refresh"),
        )

    def test_same_media_second_manual_query_uses_daily_cache(self):
        self.set_time(23, 46)
        self._configure_tv_query_gate(due=False)

        decision = self.plugin._ayclub_tv_query_mode(
            tmdb_id=123456,
            season=1,
            lifecycle_force_refresh=False,
            scheduled_evening_refresh=False,
            query_origin="manual_or_api_full",
        )

        self.assertEqual(
            decision,
            (False, True, "cache_only_already_refreshed_today"),
        )

    def test_different_media_status_targeted_sync_is_also_cache_only(self):
        self.set_time(23, 46)
        self._configure_tv_query_gate()

        for tmdb_id in (123456, 654321):
            decision = self.plugin._ayclub_tv_query_mode(
                tmdb_id=tmdb_id,
                season=1,
                lifecycle_force_refresh=False,
                scheduled_evening_refresh=False,
                query_origin="targeted_lifecycle",
            )
            self.assertEqual(
                decision,
                (False, True, "cache_only_trigger_not_authorized"),
            )


if __name__ == "__main__":
    unittest.main()
