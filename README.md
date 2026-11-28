# database-replica-feedback-horizon-validator
验证副本反馈地平线、快照和清理边界

纯内存仿真 hot standby feedback 协议，计算主库的安全清理边界。
**不连接任何真实数据库**：所有状态（副本快照、feedback 消息、清理水位、
长事务）都以事件形式注入仿真引擎，适合验证反馈延迟、副本重启、
清理竞争等时序问题，无需 PostgreSQL 实例。

## 环境

- Python 3.8.2+（仅标准库，无第三方依赖）

## 模型

- 版本用单调递增的 xid 表示；清理水位 `horizon` 表示所有
  `xid < horizon` 的旧版本允许被 vacuum 清理。
- 副本通过 feedback 消息通告 `(epoch, xmin)`：`epoch` 在副本重启
  （recovery）时递增；同一 epoch 内 `xmin` 单调不减。
- 主库合并 feedback 的规则：
  - `epoch` 小于已见 epoch：旧反馈晚到，丢弃（stale）；
  - `epoch` 大于已见 epoch：副本已重启，整体替换视图；
  - `epoch` 相同：取 `xmin` 最大值，容忍消息乱序。
- 副本重启时主库连接断开，该副本的约束立即失效，直到新 epoch 的
  feedback 送达（危险窗口）。
- 清理水位 = `min(vacuum 目标水位, 各副本视图 xmin, 活跃长事务 xmin)`，
  即多副本取最小水位。

## 输入与输出

`feedback_horizon.Scenario` 接收：副本快照（`SnapshotEvent`）、
feedback 及 epoch（`FeedbackEvent`）、主库清理水位（`CleanupEvent`）、
长事务（`LongTransaction`）、复制延迟（`replication_delay` /
`per_replica_delay`）和恢复事件（`RecoveryEvent`）。

`run_scenario()` 返回 `Report`，每次清理包含：

- `applied_horizon`：主库实际采用的清理水位（基于已送达反馈）；
- `safe_horizon`：基于副本真实需求的安全清理边界；
- `protected_floor`：`xid >= protected_floor` 的版本受保护；
- `erroneous_ranges`：被误清理但仍被副本需要的版本区间；
- `blocked_replicas`：因误清理而阻塞（快照过旧）的副本。

报告另含 `stale_feedback`（旧 epoch 晚到被丢弃）、`late_feedback`
（送达时主库已清理越过其 xmin）与 `final_views`（各副本最终视图）。

## 运行

```bash
python -m unittest -v     # 运行全部测试
python feedback_horizon.py  # 运行演示场景（反馈延迟导致过度清理）
```

测试覆盖：反馈延迟、副本重启（stale 丢弃 / 危险窗口 / epoch 替换）、
同 epoch 乱序、多副本最小水位、清理与在途反馈竞争、长事务约束。
