# Wallet Manager

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前已有部分 PostgreSQL Projection。

`SigbotWalletManager` 是会计与 Portfolio Source of Truth，拥有资金/保证金
Reservation、不可变 Ledger、Balance、Position、Realized/Unrealized P&L、Equity
Snapshot，以及 Strategy 消费的版本化 Portfolio Snapshot。它永远不能从 Strategy
Signal 或 Order Acknowledgement 推断资金变化。

## 资金输入契约

Wallet 消费由 Order Manager 拥有的权威、不可变 Execution Fact，包括 Fill、Fee、
Funding、Settlement、Transfer 与 Liquidation。每个 Fact 携带稳定 Event ID、Account、
Asset/Instrument、Quantity、Decimal Amount/Price、Venue Timestamp 与 Causation ID。

下单前，Order Manager 可以调用幂等 Reservation API。Reservation 减少可用资金或
保证金，但不伪装成 Fill。Fill、Reject、Cancel 与 Expiry Fact 按显式状态机消费或
释放 Reservation。

## 原子会计事务

对于每个资金事件，一个 Wallet Database Transaction 必须：

1. 插入 Inbox/Idempotency Record；
2. 追加平衡的不可变 Ledger Entry；
3. 消费或释放 Reservation；
4. 使用乐观并发或 Row Lock 更新 Balance/Position Projection；
5. 记录 Realized P&L、Fee、Funding 与 Settlement Effect；
6. 写入新 Portfolio Snapshot/Outbox Record；
7. Commit 后才 ACK 消息。

Inbox Insert 报告 Duplicate 时，禁止再次应用 Ledger 或 Projection Delta。Ledger
Entry 永不修改；修正必须使用 Compensating Entry。

## 所有模型

| Model | 用途 |
|---|---|
| Reservation | 外部下单前对资金或保证金的幂等 Hold |
| Ledger | 不可变 Double-entry Accounting Fact |
| Balance | 每 Account/Asset 的 Available、Reserved 与 Total Materialized View |
| Position | 每 Instrument 的 Quantity、Cost Basis、Margin 与 Realized/Unrealized P&L View |
| Equity Snapshot | 用于分析和风险的时间序列 View |
| `SigbotPortfolioSnapshot` | 带 Account、Balance、Position、Exposure、Event Watermark 与 Effective Time 的 Strategy-facing 版本化视图 |

Wallet Database/Schema、Credential、Migration 与 Write API 由本服务私有拥有。其他
服务只能查询 Wallet API 或消费 Snapshot，禁止读取 Wallet Table。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotWalletManager` | `SigbotTransactionalWalletManager` |
| Wallet Behavior | `ISigbotWalletManager` | Reservation 与 Event Application |
| Accounting | `ISigbotPortfolioLedger` | Double-entry Ledger 实现 |
| Persistence | `ISigbotPortfolioStore` | `SigbotPostgresPortfolioStore` |
| Reconciliation Source | Exchange Account Capability | Venue Balance/Position/Fill Query |

## 对账与一致性

周期性 Reconciliation 比较本地 Order/Fill Watermark、Balance、Position 与权威 Venue
API。差异必须生成显式 Reconciliation Case 与 Corrective Fact，不能静默覆盖。
Distributed Ownership 或稳定 Account Sharding 防止多个 Reconciler 重复应用同一范围。

Order 与 Wallet 分别维持本地 ACID，通过 Outbox/Inbox、Replay 与 Reconciliation
收敛。运维上可以使用同一个 PostgreSQL Cluster，但不能共享可写表或 Service
Credential。

## 安全不变量

- Money、Price、Quantity、Fee 与 Rate 使用显式 Decimal Scale 及 Currency/Instrument
  Metadata；Binary Float 不能作为权威值。
- 每个 Ledger Transaction 必须按 Currency 与 Accounting Policy 平衡。
- 除非 Product 与 Risk Policy 明确允许，否则拒绝负 Available Balance 或 Margin。
- Portfolio Snapshot 不可变，按 Account Version 单调递增，并包含已应用 Execution
  Watermark。
- 未经显式脱敏与 Access Policy，禁止记录 Balance、Credential 或私有 Account Detail。
- Database Transaction 禁止跨越 Exchange Network Call。

## 实现对齐

当前源码暴露 `SigbotWalletServer`、`ISigbotWalletManager`、`IWalletUpdater` 与
`PostgresWalletUpdater`，使用全局应用 PostgreSQL 配置。Ledger Insert 先单独 Commit，
Balance/Position 再异步分事务更新；Duplicate Ledger Insert 后仍可能应用 Projection
Delta，因此尚未达到目标原子/幂等边界。

必须对齐到 `SigbotWalletManager` Root、专用 Wallet Store、Inbox/Reservation Model、
单一原子 Accounting Transaction、不可变 Portfolio Snapshot 与 Exchange
Reconciliation。

## 源码地图

- Wallet Service 与 Persistence Adapter：`src/wallet/`
- Wallet Domain 类型：`src/types/src/modules/wallet/`
- Wallet Migration：`tooling/deploy/migrations/`
