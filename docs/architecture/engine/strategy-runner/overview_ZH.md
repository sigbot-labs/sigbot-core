# Strategy Runner

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前已有部分 Python 执行能力。

`SigbotStrategyRunner` 是单个 Strategy Pipeline 的确定性决策边界。它 Join 规范化
行情、最新有效 AI Evaluation 与当前 Portfolio Snapshot，运行带版本 Python 策略，
发布最终 `SigbotTradeSignal`。它不能调用 Exchange。

## 确定性执行上下文

每个 Decision Event 都组装一个不可变 `SigbotStrategyExecutionContext`：

```text
tenant_id / pipeline_id / strategy_id
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

`SigbotTradeSignal` 包含 Signal ID、Action（`BUY`、`SELL`、`CLOSE` 或 `NONE`）、
Instrument、目标 Quantity/Exposure、可选 Order Constraint，以及导致本次决策的
Feed Event、Evaluation、Portfolio、Strategy 与 Risk Version Reference。

以下情况必须产生类型化 `NONE`，不开新仓：

- Evaluation Regime 为 `UNKNOWN`，或 Confidence 低于 Policy；
- Evaluation/Portfolio Snapshot 缺失、过期、来自未来或不兼容；
- 推荐参数未通过 Schema/Range Validation；
- Strategy 执行 Timeout、失败，或返回非法 Signal。

只有可执行 Signal 才发布给 Order Manager。`NONE` 作为 Audit/Metric 记录保留，但不
进入执行 Queue。

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

| 角色 | 契约 | 实现 |
|---|---|---|
| Service Root | `SigbotStrategyRunner` | Strategy-scoped Runtime |
| Execution Engine | `IStrategy` | `PythonStrategy`；未来 Sandbox Runtime 需显式契约 |
| Input Join | `ISigbotStrategyContextAssembler` | 感知 Event Time 与 Version 的 Assembler |
| Signal Validation | `ISigbotTradeSignalValidator` | 发布前 Schema 与硬边界校验 |

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

当前源码暴露 `SigbotStrategyRunner`、`IStrategy` 与
`PythonStrategy`。当前 Execution Input 缺少完整 Evaluation/Portfolio
契约，尚未订阅 Evaluation Stream，并存在 Python 执行后直接调用 Binance Exchange
Client 的路径；Batch Execution 也未完成。

必须对齐到三输入确定性 Join、`NONE` Fail-safe、Version/Provenance 字段、Sandbox
保证，以及只向 Order Manager 发布。Rust 等其他策略语言只有在存在真实 Executor
与兼容性测试后才算支持。

## 源码地图

- Runner：`src/component/strategy/runner/`
- Python Executor 与 SDK：`src/component/strategy/sdk/`
- 共享 Strategy 类型：`src/types/src/strategy/`
