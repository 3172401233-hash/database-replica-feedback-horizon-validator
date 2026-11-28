"""feedback_horizon 的单元测试（python -m unittest）。"""

import unittest

from feedback_horizon import (
    CleanupEvent,
    FeedbackEvent,
    LongTransaction,
    RecoveryEvent,
    Scenario,
    SnapshotEvent,
    run_scenario,
)


class FeedbackDelayTest(unittest.TestCase):
    # 反馈延迟导致主库清理越过副本仍需要的版本

    def test_delayed_feedback_causes_over_cleanup(self):
        report = run_scenario(Scenario(
            snapshots=[SnapshotEvent(time=0, replica="r1", xmin=100)],
            feedbacks=[FeedbackEvent(time=1, replica="r1", epoch=1, xmin=100)],
            cleanups=[CleanupEvent(time=5, requested_horizon=200)],
            replication_delay=10,  # feedback 在 t=11 才送达，晚于 t=5 的清理
        ))
        rec = report.cleanups[0]
        self.assertEqual(rec.applied_horizon, 200)   # 无约束，按目标水位清理
        self.assertEqual(rec.safe_horizon, 100)      # 真实安全边界
        self.assertTrue(rec.over_cleaned)
        self.assertEqual(rec.blocked_replicas, ["r1"])
        self.assertEqual(rec.erroneous_ranges, [(100, 200)])
        # 迟到的反馈被标记：送达时主库已清理越过 xmin=100
        self.assertEqual(len(report.late_feedback), 1)
        self.assertEqual(report.late_feedback[0].xmin, 100)

    def test_timely_feedback_protects_snapshot(self):
        report = run_scenario(Scenario(
            snapshots=[SnapshotEvent(time=0, replica="r1", xmin=100)],
            feedbacks=[FeedbackEvent(time=1, replica="r1", epoch=1, xmin=100)],
            cleanups=[CleanupEvent(time=5, requested_horizon=200)],
            replication_delay=2,  # t=3 送达，早于清理
        ))
        rec = report.cleanups[0]
        self.assertEqual(rec.applied_horizon, 100)
        self.assertFalse(rec.over_cleaned)
        self.assertEqual(rec.blocked_replicas, [])
        self.assertEqual(rec.erroneous_ranges, [])
        self.assertEqual(rec.protected_floor, 100)
        self.assertEqual(report.late_feedback, [])


class ReplicaRestartTest(unittest.TestCase):
    # 副本重启：旧 epoch 反馈晚到必须丢弃，新 epoch 反馈重建约束

    def test_restart_discards_stale_feedback(self):
        old_fb = FeedbackEvent(time=1, replica="r1", epoch=1, xmin=50)
        report = run_scenario(Scenario(
            snapshots=[
                SnapshotEvent(time=0, replica="r1", xmin=50),
                SnapshotEvent(time=11, replica="r1", xmin=80),  # 重启后的新快照
            ],
            feedbacks=[
                old_fb,  # 延迟 100，重启后才送达 -> stale
                FeedbackEvent(time=12, replica="r1", epoch=2, xmin=80),
            ],
            recoveries=[RecoveryEvent(time=10, replica="r1", new_epoch=2)],
            cleanups=[CleanupEvent(time=20, requested_horizon=100)],
            per_replica_delay={"r1": 100},
        ))
        # 旧反馈 t=101 送达，epoch 过旧被丢弃
        self.assertIn(old_fb, report.stale_feedback)
        self.assertNotIn(old_fb, report.accepted_feedback)

    def test_restart_window_and_rebuild(self):
        report = run_scenario(Scenario(
            snapshots=[
                SnapshotEvent(time=0, replica="r1", xmin=50),
                SnapshotEvent(time=11, replica="r1", xmin=80),
            ],
            feedbacks=[
                FeedbackEvent(time=1, replica="r1", epoch=1, xmin=50),   # stale
                FeedbackEvent(time=12, replica="r1", epoch=2, xmin=80),  # 重建
            ],
            recoveries=[RecoveryEvent(time=10, replica="r1", new_epoch=2)],
            cleanups=[
                CleanupEvent(time=10.5, requested_horizon=100),  # 危险窗口
                CleanupEvent(time=20, requested_horizon=100),    # 新视图未送达
            ],
            per_replica_delay={"r1": 100},
        ))
        first, second = report.cleanups
        # 危险窗口：连接断开约束失效，旧快照已随重启失效
        self.assertEqual(first.applied_horizon, 100)
        self.assertEqual(first.blocked_replicas, [])
        # 新 epoch 反馈 t=112 才送达，t=20 时仍无约束
        self.assertEqual(second.applied_horizon, 100)
        # 旧 epoch 反馈在 t=101 送达被丢弃；新反馈 t=112 送达被接受
        self.assertEqual([fb.epoch for fb in report.stale_feedback], [1])
        self.assertEqual(report.final_views["r1"], (2, 80))

    def test_higher_epoch_replaces_lower_xmin(self):
        # 重启后 xmin 可以比旧 epoch 更小（单调性按 epoch 重置）
        report = run_scenario(Scenario(
            feedbacks=[
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=200),
                FeedbackEvent(time=1, replica="r1", epoch=2, xmin=30),
            ],
            cleanups=[CleanupEvent(time=5, requested_horizon=500)],
        ))
        self.assertEqual(report.final_views["r1"], (2, 30))
        self.assertEqual(report.cleanups[0].applied_horizon, 30)


class OutOfOrderTest(unittest.TestCase):
    # 同一 epoch 内 feedback 乱序：取最大 xmin，避免水位回退

    def test_out_of_order_same_epoch_keeps_max_xmin(self):
        report = run_scenario(Scenario(
            snapshots=[SnapshotEvent(time=0, replica="r1", xmin=90, duration=2)],
            feedbacks=[
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=90),
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=120),
            ],
            cleanups=[CleanupEvent(time=10, requested_horizon=200)],
            replication_delay=1,
        ))
        # 两条消息同时送达，引擎按最大 xmin 合并，与到达顺序无关
        self.assertEqual(report.final_views["r1"], (1, 120))
        self.assertEqual(report.cleanups[0].applied_horizon, 120)
        self.assertEqual(report.cleanups[0].erroneous_ranges, [])

    def test_reordered_delivery_does_not_regress(self):
        # xmin=120 先到、xmin=90 后到：视图不得回退到 90
        report = run_scenario(Scenario(
            snapshots=[SnapshotEvent(time=0, replica="r1", xmin=90, duration=2)],
            feedbacks=[
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=120),  # t=1 送达
                FeedbackEvent(time=1, replica="r1", epoch=1, xmin=90),   # t=6 送达
            ],
            cleanups=[CleanupEvent(time=10, requested_horizon=200)],
            replication_delay=1,
        ))
        self.assertEqual(report.final_views["r1"], (1, 120))
        self.assertEqual(report.cleanups[0].applied_horizon, 120)


class MultiReplicaTest(unittest.TestCase):
    # 多副本：清理水位取所有副本反馈的最小值

    def test_min_watermark_across_replicas(self):
        report = run_scenario(Scenario(
            snapshots=[
                SnapshotEvent(time=0, replica="r1", xmin=150),
                SnapshotEvent(time=0, replica="r2", xmin=300),
            ],
            feedbacks=[
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=150),
                FeedbackEvent(time=0, replica="r2", epoch=1, xmin=300),
            ],
            cleanups=[CleanupEvent(time=5, requested_horizon=1000)],
            replication_delay=1,
        ))
        self.assertEqual(report.cleanups[0].applied_horizon, 150)

    def test_min_watermark_moves_when_laggard_advances(self):
        report = run_scenario(Scenario(
            snapshots=[
                SnapshotEvent(time=0, replica="r1", xmin=150, duration=6),
                SnapshotEvent(time=0, replica="r2", xmin=300),
            ],
            feedbacks=[
                FeedbackEvent(time=0, replica="r1", epoch=1, xmin=150),
                FeedbackEvent(time=0, replica="r2", epoch=1, xmin=300),
                FeedbackEvent(time=7, replica="r1", epoch=1, xmin=400),
            ],
            cleanups=[CleanupEvent(time=10, requested_horizon=1000)],
            replication_delay=1,
        ))
        # r1 推进到 400 后，最小水位由 r2 的 300 决定
        self.assertEqual(report.cleanups[0].applied_horizon, 300)
        self.assertEqual(report.cleanups[0].blocked_replicas, [])


class CleanupRaceTest(unittest.TestCase):
    # 清理与在途反馈竞争：清理后的迟到反馈用于下一次决策

    def test_cleanup_race_with_inflight_feedback(self):
        report = run_scenario(Scenario(
            snapshots=[SnapshotEvent(time=0, replica="r1", xmin=100)],
            feedbacks=[FeedbackEvent(time=4, replica="r1", epoch=1, xmin=100)],
            cleanups=[
                CleanupEvent(time=5, requested_horizon=200),   # 反馈在途
                CleanupEvent(time=20, requested_horizon=200),  # 反馈已送达
            ],
            replication_delay=10,  # t=14 送达
        ))
        first, second = report.cleanups
        self.assertEqual(first.applied_horizon, 200)
        self.assertEqual(first.blocked_replicas, ["r1"])
        self.assertEqual(first.erroneous_ranges, [(100, 200)])
        self.assertEqual(second.applied_horizon, 100)
        self.assertEqual(second.blocked_replicas, [])
        self.assertEqual(len(report.late_feedback), 1)
        # 汇总视图
        self.assertEqual(report.blocked_replicas, ["r1"])
        self.assertEqual(report.erroneous_ranges, [(100, 200)])


class LongTransactionTest(unittest.TestCase):
    # 主库长事务同样约束清理水位

    def test_long_transaction_bounds_horizon(self):
        report = run_scenario(Scenario(
            long_transactions=[LongTransaction(xmin=70, start=0, end=50)],
            cleanups=[
                CleanupEvent(time=10, requested_horizon=500),
                CleanupEvent(time=60, requested_horizon=500),
            ],
        ))
        self.assertEqual(report.cleanups[0].applied_horizon, 70)
        self.assertEqual(report.cleanups[1].applied_horizon, 500)
        self.assertEqual(report.blocked_replicas, [])


if __name__ == "__main__":
    unittest.main()
