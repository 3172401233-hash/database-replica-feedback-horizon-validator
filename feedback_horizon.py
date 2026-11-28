"""Hot standby feedback 地平线验证器（纯内存仿真，不连接任何数据库）。

模型说明
--------
- 版本用单调递增的 xid 表示；清理水位（cleanup horizon）含义为：
  所有 xid < horizon 的旧版本允许被主库 vacuum 清理。
- 副本通过 feedback 消息向主库通告自己仍需的最老 xid（xmin）。
  feedback 消息携带 (epoch, xmin)：epoch 在副本重启（recovery）时递增，
  同一 epoch 内副本通告的 xmin 单调不减。
- 主库按以下规则合并 feedback：
    * epoch 小于已见 epoch        -> 旧反馈晚到，丢弃（stale）；
    * epoch 大于已见 epoch        -> 副本已重启，整体替换；
    * epoch 相同                  -> 取 xmin 的最大值（容忍乱序）。
- 副本重启时主库连接断开，该副本的约束立即失效，直到新 epoch 的
  feedback 到达为止（危险窗口）。
- 清理决策同时受主库自身长事务的 xmin 约束；多副本时取最小水位。
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------- 输入事件

@dataclass(frozen=True)
class SnapshotEvent:
    """副本在 time 时刻拍下快照，需要 xmin 及之后的版本。"""
    time: float
    replica: str
    xmin: int
    duration: Optional[float] = None  # None 表示一直持有直到副本重启


@dataclass(frozen=True)
class FeedbackEvent:
    """副本在 time 时刻发出 feedback（送达时间 = time + 复制延迟）。"""
    time: float
    replica: str
    epoch: int
    xmin: int


@dataclass(frozen=True)
class RecoveryEvent:
    """副本在 time 时刻重启，进入 new_epoch。"""
    time: float
    replica: str
    new_epoch: int


@dataclass(frozen=True)
class CleanupEvent:
    """主库在 time 时刻执行清理，requested_horizon 为 vacuum 目标水位。"""
    time: float
    requested_horizon: int


@dataclass(frozen=True)
class LongTransaction:
    """主库长事务，活跃期间其 xmin 约束清理水位。"""
    xmin: int
    start: float = 0.0
    end: Optional[float] = None

    def active_at(self, t: float) -> bool:
        return self.start <= t and (self.end is None or t < self.end)


@dataclass
class Scenario:
    snapshots: List[SnapshotEvent] = field(default_factory=list)
    feedbacks: List[FeedbackEvent] = field(default_factory=list)
    recoveries: List[RecoveryEvent] = field(default_factory=list)
    cleanups: List[CleanupEvent] = field(default_factory=list)
    long_transactions: List[LongTransaction] = field(default_factory=list)
    replication_delay: float = 0.0
    per_replica_delay: Dict[str, float] = field(default_factory=dict)

    def delay_for(self, replica: str) -> float:
        return self.per_replica_delay.get(replica, self.replication_delay)


# ---------------------------------------------------------------- 输出报告

@dataclass
class CleanupRecord:
    time: float
    requested_horizon: int
    applied_horizon: int                 # 主库实际采用的清理水位
    safe_horizon: int                    # 基于副本真实需求的安全边界
    erroneous_ranges: List[Tuple[int, int]]  # 被误清理但仍被需要的版本区间 [lo, hi)
    blocked_replicas: List[str]          # 因误清理而阻塞（快照过旧）的副本

    @property
    def protected_floor(self) -> int:
        """xid >= protected_floor 的版本在本次清理后仍受保护。"""
        return self.applied_horizon

    @property
    def over_cleaned(self) -> bool:
        return self.applied_horizon > self.safe_horizon


@dataclass
class Report:
    cleanups: List[CleanupRecord]
    accepted_feedback: List[FeedbackEvent]
    stale_feedback: List[FeedbackEvent]   # 因 epoch 过旧被丢弃的迟到反馈
    late_feedback: List[FeedbackEvent]    # 到达时主库已清理越过其 xmin 的反馈
    final_views: Dict[str, Tuple[int, int]]  # replica -> (epoch, xmin)

    @property
    def blocked_replicas(self) -> List[str]:
        seen = set()
        for record in self.cleanups:
            seen.update(record.blocked_replicas)
        return sorted(seen)

    @property
    def erroneous_ranges(self) -> List[Tuple[int, int]]:
        ranges: List[Tuple[int, int]] = []
        for record in self.cleanups:
            ranges.extend(record.erroneous_ranges)
        return _merge_ranges(ranges)


def _merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    merged: List[Tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


# ---------------------------------------------------------------- 仿真引擎

# 同一时刻的事件处理优先级：快照/恢复/发送 -> 送达 -> 清理
_EV_SNAPSHOT_OPEN = 0
_EV_SNAPSHOT_CLOSE = 1
_EV_RECOVERY = 2
_EV_FEEDBACK_SEND = 3
_EV_FEEDBACK_DELIVER = 4
_EV_CLEANUP = 5


class _Engine:
    def __init__(self, scenario: Scenario):
        self.scenario = scenario
        self.active_snapshots: Dict[str, List[int]] = {}
        self.views: Dict[str, List[int]] = {}       # replica -> [epoch, xmin]
        self.last_epoch: Dict[str, int] = {}
        self.cleaned_horizon = 0
        self.accepted: List[FeedbackEvent] = []
        self.stale: List[FeedbackEvent] = []
        self.late: List[FeedbackEvent] = []
        self.records: List[CleanupRecord] = []
        self._queue: List[Tuple[float, int, int, object]] = []
        self._seq = 0

    def _push(self, time: float, kind: int, payload: object) -> None:
        heapq.heappush(self._queue, (time, kind, self._seq, payload))
        self._seq += 1

    def run(self) -> Report:
        sc = self.scenario
        for snap in sc.snapshots:
            self._push(snap.time, _EV_SNAPSHOT_OPEN, snap)
            if snap.duration is not None:
                self._push(snap.time + snap.duration, _EV_SNAPSHOT_CLOSE, snap)
        for fb in sc.feedbacks:
            self._push(fb.time, _EV_FEEDBACK_SEND, fb)
        for rec in sc.recoveries:
            self._push(rec.time, _EV_RECOVERY, rec)
        for cl in sc.cleanups:
            self._push(cl.time, _EV_CLEANUP, cl)

        while self._queue:
            time, kind, _, payload = heapq.heappop(self._queue)
            if kind == _EV_SNAPSHOT_OPEN:
                self.active_snapshots.setdefault(payload.replica, []).append(payload.xmin)
            elif kind == _EV_SNAPSHOT_CLOSE:
                snaps = self.active_snapshots.get(payload.replica, [])
                if payload.xmin in snaps:
                    snaps.remove(payload.xmin)
            elif kind == _EV_RECOVERY:
                self._recover(payload)
            elif kind == _EV_FEEDBACK_SEND:
                delay = sc.delay_for(payload.replica)
                self._push(time + delay, _EV_FEEDBACK_DELIVER, payload)
            elif kind == _EV_FEEDBACK_DELIVER:
                self._deliver(payload)
            elif kind == _EV_CLEANUP:
                self._cleanup(payload)

        return Report(
            cleanups=self.records,
            accepted_feedback=self.accepted,
            stale_feedback=self.stale,
            late_feedback=self.late,
            final_views={r: (v[0], v[1]) for r, v in self.views.items()},
        )

    def _recover(self, ev: RecoveryEvent) -> None:
        # 旧 incarnation 的快照全部失效；主库连接断开，约束清除
        self.active_snapshots[ev.replica] = []
        self.views.pop(ev.replica, None)
        self.last_epoch[ev.replica] = max(
            self.last_epoch.get(ev.replica, -1), ev.new_epoch
        )

    def _deliver(self, fb: FeedbackEvent) -> None:
        seen = self.last_epoch.get(fb.replica, -1)
        if fb.epoch < seen:
            self.stale.append(fb)  # 旧反馈晚到：丢弃
            return
        if fb.epoch > seen:
            self.last_epoch[fb.replica] = fb.epoch
            self.views[fb.replica] = [fb.epoch, fb.xmin]
        else:
            view = self.views.setdefault(fb.replica, [fb.epoch, fb.xmin])
            view[1] = max(view[1], fb.xmin)  # 同 epoch 乱序：取最大 xmin
        self.accepted.append(fb)
        if fb.xmin < self.cleaned_horizon:
            self.late.append(fb)  # 反馈迟到，主库已清理越过其 xmin

    def _cleanup(self, ev: CleanupEvent) -> None:
        now = ev.time
        long_tx = [t.xmin for t in self.scenario.long_transactions if t.active_at(now)]

        applied = min([ev.requested_horizon]
                      + [v[1] for v in self.views.values()]
                      + long_tx)

        truth = [xmin for xmins in self.active_snapshots.values() for xmin in xmins]
        safe = min([ev.requested_horizon] + truth + long_tx)

        erroneous: List[Tuple[int, int]] = []
        blocked: List[str] = []
        for replica, xmins in self.active_snapshots.items():
            if any(xmin < applied for xmin in xmins):
                blocked.append(replica)
                for xmin in xmins:
                    if xmin < applied:
                        erroneous.append((xmin, applied))

        self.cleaned_horizon = max(self.cleaned_horizon, applied)
        self.records.append(CleanupRecord(
            time=now,
            requested_horizon=ev.requested_horizon,
            applied_horizon=applied,
            safe_horizon=safe,
            erroneous_ranges=_merge_ranges(erroneous),
            blocked_replicas=sorted(blocked),
        ))


def run_scenario(scenario: Scenario) -> Report:
    """运行仿真，返回每次清理的水位、误清理范围和阻塞副本。"""
    return _Engine(scenario).run()


# ---------------------------------------------------------------- 演示

def _demo() -> None:
    scenario = Scenario(
        snapshots=[SnapshotEvent(time=0, replica="standby-1", xmin=420)],
        feedbacks=[FeedbackEvent(time=1, replica="standby-1", epoch=1, xmin=420)],
        cleanups=[
            CleanupEvent(time=10, requested_horizon=600),  # feedback 仍在途中
            CleanupEvent(time=40, requested_horizon=600),  # feedback 已送达
        ],
        long_transactions=[LongTransaction(xmin=500)],
        replication_delay=30,
    )
    report = run_scenario(scenario)
    for rec in report.cleanups:
        print("t=%-5s requested=%s applied=%s safe=%s blocked=%s erroneous=%s"
              % (rec.time, rec.requested_horizon, rec.applied_horizon,
                 rec.safe_horizon, rec.blocked_replicas, rec.erroneous_ranges))
    print("late feedback :", report.late_feedback)
    print("stale feedback:", report.stale_feedback)
    print("final views   :", report.final_views)


if __name__ == "__main__":
    _demo()
