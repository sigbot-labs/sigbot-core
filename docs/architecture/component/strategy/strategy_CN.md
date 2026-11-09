# Strategy Runner

[English](strategy.md) · [架构总览](../../overview_CN.md)

**状态：** 已实现确定性 Python Effects、事务 State 与沙箱；分布式增量/模型分片仍待完成。

`SigbotStrategyRunner` 是单个 Strategy Pipeline 的确定性决策边界。它 Join 规范化
行情、最新有效 AI Evaluation 与当前 Portfolio Snapshot，运行带版本 Python 策略，
返回类型化 `Effects { intents, requests, timers }`；通过校验的 `TradeIntent` 发布到
StrategySignal topic。它不能调用 Exchange。

## 确定性执行上下文

每个 Decision Event 都组装一个不可变 SDK `StrategyContext`：

```text
env / tenant_id / workflow_id / node_id / run_id
decision_time
market_window + terminal_feed_event_id
eval_snapshot + evaluation_id
portfolio_snapshot + portfolio_version
strategy_code_version
validated_hyperparameters
risk_limits
```

只有 Evaluation 在 Decision Time 前已发布并生效、其 Input Watermark 不超过终点行情
事件且尚未过期时才可使用。超参数在执行前按 Strategy 的版本化 Schema 校验。
Portfolio Snapshot 必须满足 Strategy 的 Freshness 与 Account 要求。

相同 Context 与 Runtime Version 必须产生相同输出。Random、当前 Wall Clock、Network
Access 与隐藏进程可变状态必须不存在，或显式 Seed 并记录。

## 决策结果

`Effects` 包含有界的 `TradeIntent`、服务请求和 Timer 列表。`TradeIntent` 携带
Instrument、Side、Quantity/Exposure、Order Constraint、Execution Mode，以及导致本次
决策的不可变输入与版本 Reference。

以下情况不得产生开仓 Intent：

- Evaluation Regime 为 `UNKNOWN`，或 Confidence 低于 Policy；
- Evaluation/Portfolio Snapshot 缺失、过期、来自未来或不兼容；
- 推荐参数未通过 Schema/Range Validation；
- Strategy 执行 Timeout、失败，或返回非法 Signal。

只有通过校验的 Intent 才发布给 Order。无 Intent 决策可保留为 State/Audit Metric，
但不进入执行 Queue。

## Runtime 与状态

- Controller 为每个 Strategy Pipeline 管理一个 Worker Set。
- Runner 使用与 Evaluator 不同的 Strategy-specific Feed Consumer Group，并订阅
  Evaluation 与 Portfolio Snapshot。
- 输入按 Tenant、Pipeline、Strategy、Account、Instrument、Event Time 与 Version
  Join；Late Update 不能重写已发布决策。
- Stateful Rolling Window 每个 Partition 只有一个 Active Owner，并 Checkpoint 足够
  状态以确定性恢复。
- 配置变更产生新 Generation；一次执行从开始到结束只使用一个 Generation。

## 扩展点

| 角色              | 契约                                | 实现                             |
| ----------------- | ----------------------------------- | -------------------------------- |
| Service Root      | `SigbotStrategyRunner`              | 组件启动                         |
| Execution Engine  | `IStrategy`                         | `PythonStrategy`                 |
| Node Runtime      | `StrategyNode` / `StrategyService`  | 串行路由与类型化输入处理         |
| State Transaction | `CheckpointStrategyTransaction`     | Draft、Checkpoint 与 Outbox 提交 |
| Effect Validation | `Effects::validate` 与 Order 硬风控 | 发布前有界输出校验               |

Python Strategy SDK 定义稳定的用户代码 Surface。AI 生成代码属于外部 Research
Process；Runtime 只执行已批准、不可变的 Strategy Artifact。

## 隔离与安全不变量

- Strategy Code 具有显式 CPU、Memory、Time、Filesystem 与 Network Limit。
- Exchange Credential 和私有 Trading Client 永远不能进入 Strategy Sandbox。
- Money、Quantity 与 Price 需要精确 Decimal 语义时禁止使用 Float。
- Strategy 不能削弱 Order Manager Policy 提供的硬 Risk Limit。
- 每个决策都能追溯到不可变输入和 Content-addressed Code Artifact。
- 单个 Strategy 故障与其他 Pipeline 隔离，不能阻塞共享 Datafeed 或 Message Event
  Loop。

## 实现对齐

`SigbotStrategyRunner` 已串行化单个决策实例，并调用
`on_hyperparameters(ctx, update)` 或 `on_data(ctx, event)`。Python SDK 返回
`Effects {intents, requests, timers}`；可变 State 是事务草稿，与 Checkpoint 和出站请求
原子提交。行情、评估、组合、执行、定时、Snapshot、Compute、Notification 与 Export
Result 都是类型化输入。Strategy 只发布 Intent，不能调用 Exchange。

Rust Host 已限制进程、文件、网络、时间和资源。当前有界 Compute 可把支持的聚合下推
PostgreSQL/TimescaleDB，但 Redis 热缓存、通用分布式增量 Worker、Python 自动并行和任意
模型分片等价尚未实现。

## 恢复与 Worker 边界

Worker 清理继承环境和文件描述符，在宿主支持范围内应用 Landlock/seccomp、文件、网络、CPU/内存/PID 和超时约束。回调异常丢弃事务草稿；Checkpoint 版本 Fencing 防止第二 Owner 提交同一决策。历史批量仅用于预热/计算；虚拟时钟仍逐事件调用 `on_data`，回放 Snapshot 请求将 LATEST 冻结为虚拟时钟 AS_OF。

Worker 源码为 `src/component/strategy/runner/src/bin/worker.rs`，Cargo 二进制名为 `sigbot-strategy-worker`。SDK 与 Runner 的 Python 子树属于稳定用户契约，修改必须在明确范围内；冻结调用方所需的旧 Facade 只能委托共享 Context，不能另建 Registry。仓控/Rebalance 通过策略 Effects 表达，不表示所有内置策略已交付。宿主日志耐久性遵循 [Logger 采集契约](../logger/logger_CN.md#rust-log-capture)。

## 源码地图

- Runner：`src/component/strategy/runner/`
- Python Executor 与 SDK：`src/component/strategy/sdk/`
- 共享 Strategy 类型：`src/types/src/strategy/`
