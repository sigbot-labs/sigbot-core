# Wallet Manager

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 已实现权威预占、不可变会计与组合投影组件；广泛外部账户对账仍待完成。

`SigbotWalletServer` 是会计与 Portfolio Source of Truth，拥有资金/保证金
Reservation、不可变 Ledger、Balance、Position、Realized/Unrealized P&L、Equity
Snapshot，以及 Strategy 消费的版本化 Portfolio Snapshot。它永远不能从 Strategy
Signal 或 Order Acknowledgement 推断资金变化。

## 资金输入契约

Wallet 消费由 Order Manager 拥有的权威、不可变 Execution Fact，包括 Fill、Fee、
Funding、Settlement、Transfer 与 Liquidation。每个 Fact 携带稳定 Event ID、Account、
Asset/Instrument、Quantity、Decimal Amount/Price、Venue Timestamp 与 Causation ID。

下单前，Order 发布幂等 Reservation Request 并等待作用域一致的 Result。Reservation
减少可用资金或保证金，但不伪装成 Fill。Fill、Reject、Cancel 与 Expiry Fact 按显式
状态机消费或释放 Reservation。

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
| 版本化组合载荷 | 带 Account、Balance、Position、Exposure、Event Watermark 与 Effective Time 的 Strategy-facing JSON 视图 |

`wlt_*` 表领域只有 Wallet Repository 一个写入方。其他组件只能使用类型化消息/
Snapshot，禁止直接查询 Wallet Table。物理 Schema 与迁移文件可和其他领域共用，但
不能共享写所有权。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotWalletServer` | 组件启动与 Provider 注册 |
| Wallet Behavior | `IWallet` | `WalletAccountProcessor` |
| Reservation | `WalletReservationProcessor` | 幂等 Hold/Release 处理 |
| Persistence | Core Handler/Repository | `WalletPostgresRepository`、`WalletSQLiteRepository` |
| Reconciliation Source | Exchange Account Capability | Venue Balance/Position/Fill Query |

## 对账与一致性

周期性 Reconciliation 比较本地 Order/Fill Watermark、Balance、Position 与权威 Venue
API。差异必须生成显式 Reconciliation Case 与 Corrective Fact，不能静默覆盖。
Distributed Ownership 或稳定 Account Sharding 防止多个 Reconciler 重复应用同一范围。

Order 与 Wallet 分别维持事务写所有权，通过 Outbox/Inbox、Replay 与 Reconciliation
收敛。当前两域表位于同一物理 PostgreSQL Schema，但组件不得跨领域写入。

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

`SigbotWalletServer` 是 Wallet Reservation、不可变 Ledger/Journal/Posting、Balance、
Position、Equity Snapshot 与版本化 Portfolio 输出的唯一写入方。Funding/Execution Fact
经 Inbox 进入；平衡 Posting、预占释放、Projection 和 Outbox 在一个 PostgreSQL 事务内
提交。重复及乱序终态保持事实一致，Projection 可从不可变记录重建。

真实账户 Transfer、Corporate Action、完整衍生品 Margin/Liquidation、持续外部对账和
完整多资产 NAV 尚未验收。

## 源码地图

- Wallet 数据面组件：`src/component/wallet/`
- Wallet Repository：`src/core/src/modules/wallet/storage/`
- Wallet Domain 类型：`src/types/src/wallet/`
- Wallet Migration：`migration/`
