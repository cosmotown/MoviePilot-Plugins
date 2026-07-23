import copy
from types import SimpleNamespace
import unittest


try:
    from app.plugins.p115strgmsub.handlers import sync as sync_module
    from app.plugins.p115strgmsub.handlers.lifecycle import LifecycleStore
    from app.plugins.p115strgmsub.handlers.sync import SyncHandler
    from app.schemas.types import MediaType
except Exception as error:
    raise unittest.SkipTest(
        f"MoviePilot plugin runtime is unavailable: {type(error).__name__}"
    )


def ed2k_resource(episode, *, season=1, group="GROUP"):
    filename = (
        f"Example.2026.S{season:02d}E{episode:02d}."
        f"2160p.WEB-DL.HEVC.HDR-{group}.mkv"
    )
    return {
        "url": (
            f"ed2k://|file|{filename}|{1000000 + episode}|"
            f"{episode:032x}|/"
        ),
        "title": filename,
        "source_kind": "ed2k",
        "season": season,
        "episode": episode,
        "episodes": [episode],
    }


class _MediaInfo:
    title = "Completed Season"
    year = "2026"
    tmdb_id = 123456
    title_year = "Completed Season (2026)"

    @staticmethod
    def get_poster_image():
        return ""


class _Chain:
    @staticmethod
    def recognize_media(**kwargs):
        return _MediaInfo()


class _SearchHandler:
    def __init__(self, resources):
        self.resources = resources
        self.search_count = 0

    @staticmethod
    def reset_ayclub_status():
        return None

    def search_single_source(self, **kwargs):
        self.search_count += 1
        return copy.deepcopy(self.resources)


class CompletedSeasonWashFlowTest(unittest.TestCase):
    def setUp(self):
        self._original_meta_info = sync_module.MetaInfo
        sync_module.MetaInfo = lambda title: SimpleNamespace(
            title=title,
            year=None,
            begin_season=None,
            type=None,
        )

    def tearDown(self):
        sync_module.MetaInfo = self._original_meta_info

    def make_handler(self, *, capacity=50, failed_episodes=None):
        state = {}
        resources = [ed2k_resource(ep) for ep in range(1, 29)]
        search_handler = _SearchHandler(resources)
        lifecycle = LifecycleStore(
            get_data_func=lambda key: state.get(key),
            save_data_func=lambda key, value: state.__setitem__(
                key,
                copy.deepcopy(value),
            ),
        )
        lifecycle.ensure_subscription(
            77,
            {
                "id": 77,
                "type": MediaType.TV.value,
                "tmdbid": 123456,
                "season": 1,
                "name": "Completed Season",
                "year": "2026",
            },
        )

        handler = SyncHandler.__new__(SyncHandler)
        handler._get_data = lambda key: state.get(key)
        handler._save_data = lambda key, value: state.__setitem__(
            key,
            copy.deepcopy(value),
        )
        handler._max_transfer_per_sync = capacity
        handler._chain = _Chain()
        handler._search_handler = search_handler
        handler._lifecycle = lifecycle
        handler._completed_ed2k_wash_due = lambda **kwargs: True
        handler._record_ayclub_query_if_real = lambda **kwargs: True

        dispatch_calls = []
        failures = set(failed_episodes or [])

        def dispatch(**kwargs):
            episode = int(kwargs["episodes"][0])
            dispatch_calls.append(episode)
            return episode not in failures, False

        handler._dispatch_ed2k_resource = dispatch
        subscribe = SimpleNamespace(
            id=77,
            name="Completed Season",
            year="2026",
            type=MediaType.TV.value,
            tmdbid=123456,
            doubanid=None,
            season=1,
            total_episode=28,
            start_episode=1,
            best_version=1,
            quality=None,
            resolution=None,
            effect=None,
        )
        return (
            handler,
            subscribe,
            state,
            search_handler,
            dispatch_calls,
            failures,
        )

    @staticmethod
    def run_wash(handler, subscribe, transferred_count=0):
        history = []
        details = []
        result = handler.process_completed_tv_ed2k_wash(
            subscribe=subscribe,
            history=history,
            transfer_details=details,
            transferred_count=transferred_count,
            scheduled_evening_refresh=True,
        )
        return result, history, details

    def test_complete_28_episode_pack_dispatches_every_episode_once(self):
        handler, subscribe, state, search, calls, _ = self.make_handler()

        count, history, details = self.run_wash(handler, subscribe)

        self.assertEqual(count, 28)
        self.assertEqual(calls, list(range(1, 29)))
        self.assertEqual(len(history), 28)
        self.assertEqual(len(details), 1)
        record = state[handler.COMPLETED_ED2K_WASH_DATA_KEY]["tv:123456:S1"]
        self.assertEqual(record["status"], "submitted")
        self.assertEqual(record["accepted_episodes"], list(range(1, 29)))
        self.assertNotIn("ed2k://", repr(record))
        self.assertEqual(search.search_count, 1)

    def test_submitted_fingerprint_prevents_duplicate_dispatch(self):
        handler, subscribe, state, search, calls, _ = self.make_handler()
        self.run_wash(handler, subscribe)
        submitted = copy.deepcopy(
            state[handler.COMPLETED_ED2K_WASH_DATA_KEY]["tv:123456:S1"]
        )
        calls.clear()

        count, history, details = self.run_wash(handler, subscribe)

        self.assertEqual(count, 0)
        self.assertEqual(calls, [])
        self.assertEqual(history, [])
        self.assertEqual(details, [])

        # 临时空结果不能抹掉已提交指纹，否则同一批次恢复后会重复提交。
        original_resources = copy.deepcopy(search.resources)
        search.resources = []
        self.run_wash(handler, subscribe)
        after_empty = state[handler.COMPLETED_ED2K_WASH_DATA_KEY][
            "tv:123456:S1"
        ]
        self.assertEqual(after_empty["status"], "submitted")
        self.assertEqual(after_empty["fingerprint"], submitted["fingerprint"])

        search.resources = original_resources
        calls.clear()
        self.run_wash(handler, subscribe)
        self.assertEqual(calls, [])

        # 分数不高于已提交版本的其他指纹也必须持续跳过。
        search.resources = [
            ed2k_resource(ep, group="OTHER") for ep in range(1, 29)
        ]
        self.run_wash(handler, subscribe)
        self.assertEqual(calls, [])
        self.run_wash(handler, subscribe)
        self.assertEqual(calls, [])

    def test_partial_failure_retries_only_unaccepted_episodes(self):
        handler, subscribe, state, search, calls, failures = self.make_handler(
            failed_episodes={5, 11}
        )

        first_count, _, _ = self.run_wash(handler, subscribe)
        first_record = state[handler.COMPLETED_ED2K_WASH_DATA_KEY][
            "tv:123456:S1"
        ]
        self.assertEqual(first_count, 26)
        self.assertEqual(first_record["status"], "partial")
        self.assertEqual(first_record["failed_episodes"], [5, 11])

        original_resources = copy.deepcopy(search.resources)
        search.resources = []
        calls.clear()
        empty_count, _, _ = self.run_wash(handler, subscribe)
        self.assertEqual(empty_count, 0)
        self.assertEqual(calls, [])

        search.resources = original_resources
        failures.clear()
        calls.clear()
        second_count, _, _ = self.run_wash(handler, subscribe)
        second_record = state[handler.COMPLETED_ED2K_WASH_DATA_KEY][
            "tv:123456:S1"
        ]
        self.assertEqual(second_count, 2)
        self.assertEqual(calls, [5, 11])
        self.assertEqual(second_record["status"], "submitted")
        self.assertEqual(
            second_record["accepted_episodes"],
            list(range(1, 29)),
        )

    def test_capacity_shortage_prevents_first_dispatch(self):
        handler, subscribe, state, _, calls, _ = self.make_handler(
            capacity=27
        )

        count, history, details = self.run_wash(handler, subscribe)

        self.assertEqual(count, 0)
        self.assertEqual(calls, [])
        self.assertEqual(history, [])
        self.assertEqual(details, [])
        record = state[handler.COMPLETED_ED2K_WASH_DATA_KEY]["tv:123456:S1"]
        self.assertEqual(record["status"], "capacity_wait")
        self.assertEqual(record["diagnostics"]["remaining_capacity"], 27)

    def test_best_version_false_skips_search_and_dispatch(self):
        handler, subscribe, _, search, calls, _ = self.make_handler()
        subscribe.best_version = 0

        count, _, _ = self.run_wash(handler, subscribe)

        self.assertEqual(count, 0)
        self.assertEqual(search.search_count, 0)
        self.assertEqual(calls, [])

    def test_completed_wash_rejects_non_final_round(self):
        handler, subscribe, _, search, calls, _ = self.make_handler()
        history = []
        details = []

        count = handler.process_completed_tv_ed2k_wash(
            subscribe=subscribe,
            history=history,
            transfer_details=details,
            transferred_count=0,
            scheduled_evening_refresh=False,
        )

        self.assertEqual(count, 0)
        self.assertEqual(search.search_count, 0)
        self.assertEqual(calls, [])
        self.assertEqual(history, [])
        self.assertEqual(details, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
