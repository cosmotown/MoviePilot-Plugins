import unittest


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.db.models.subscribe import Subscribe
    from app.db.models.subscribehistory import SubscribeHistory
    from app.db.subscribe_oper import SubscribeOper
    from app.plugins.p115strgmsub import P115StrgmSub
    from app.plugins.p115strgmsub.handlers.lifecycle import LifecycleStore
except Exception as error:
    raise unittest.SkipTest(
        f"MoviePilot runtime is unavailable: {type(error).__name__}"
    )


class MoviePilotCompletedSubscriptionPersistenceTest(unittest.TestCase):
    def test_completed_row_moves_to_history_with_wash_fields(self):
        engine = create_engine("sqlite:///:memory:")
        Subscribe.__table__.create(engine)
        SubscribeHistory.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        self.addCleanup(engine.dispose)
        self.addCleanup(session.close)

        subscribe = Subscribe(
            name="Completed Season",
            year="2026",
            type="电视剧",
            tmdbid=123456,
            season=1,
            total_episode=28,
            start_episode=1,
            state="R",
            best_version=1,
            quality="WEB-DL",
            resolution="2160p",
            effect="HDR",
        )
        session.add(subscribe)
        session.commit()
        session.refresh(subscribe)
        subscribe_id = int(subscribe.id)

        oper = SubscribeOper(db=session)
        oper.add_history(**subscribe.to_dict())
        oper.delete(subscribe_id)

        self.assertIsNone(oper.get(subscribe_id))
        history = (
            session.query(SubscribeHistory)
            .filter(
                SubscribeHistory.tmdbid == 123456,
                SubscribeHistory.season == 1,
            )
            .order_by(SubscribeHistory.date.desc())
            .first()
        )
        self.assertIsNotNone(history)
        self.assertEqual(history.best_version, 1)
        self.assertEqual(history.total_episode, 28)
        self.assertEqual(history.start_episode, 1)
        self.assertEqual(history.resolution, "2160p")

        plugin = P115StrgmSub.__new__(P115StrgmSub)
        restored = plugin._resolve_completed_wash_subscribe(
            session,
            {
                "subscribe_id": subscribe_id,
                "status": "completed",
                "name": "Completed Season",
                "type": "电视剧",
                "tmdbid": 123456,
                "season": 1,
            },
        )
        self.assertIsNotNone(restored)
        self.assertEqual(restored.id, subscribe_id)
        self.assertEqual(restored.best_version, 1)
        self.assertEqual(restored.total_episode, 28)
        self.assertEqual(restored.start_episode, 1)

    def test_completion_event_persists_required_wash_snapshot(self):
        state = {}
        lifecycle = LifecycleStore(
            get_data_func=lambda key: state.get(key),
            save_data_func=lambda key, value: state.__setitem__(key, value),
        )
        lifecycle.on_complete(
            88,
            {
                "id": 88,
                "name": "Snapshot Season",
                "year": "2026",
                "type": "电视剧",
                "tmdbid": 654321,
                "doubanid": None,
                "season": 1,
                "total_episode": 28,
                "start_episode": 1,
                "best_version": 1,
                "quality": "WEB-DL",
                "resolution": "2160p",
                "effect": "HDR",
            },
        )

        records = lifecycle.completed_subscription_records(max_age_days=30)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["subscribe_id"], 88)
        self.assertEqual(records[0]["best_version"], 1)
        self.assertEqual(records[0]["total_episode"], 28)
        self.assertEqual(records[0]["resolution"], "2160p")


if __name__ == "__main__":
    unittest.main(verbosity=2)
