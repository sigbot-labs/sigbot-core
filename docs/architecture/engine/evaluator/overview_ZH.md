# Evaluator

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前已有部分 Multi-agent Executor。

`SigbotEvaluator` 是单个 Strategy Pipeline 的非确定性旁路。它观察规范化市场/新闻
事件，通过受限 Multi-agent 过程评估市场结构，并向确定性 Strategy Runner 发布
不可变建议。它永远不产生 Order 或 Trading Signal。

## 输入与输出

Evaluator 通过 Strategy-specific Consumer Group 消费 `SigbotFeedEvent`，维护按
Event Time 排序的有界上下文窗口。Trigger 可以基于时间、事件数、波动率或显式请求，
但每次运行都必须记录精确 Input Watermark。

它在交易链路中的唯一输出是 `SigbotEvaluationSnapshot`：

```text
evaluation_id
tenant_id / pipeline_id / strategy_id
regime: RANGING | TRENDING_BULLISH | TRENDING_BEARISH | UNKNOWN
confidence
hyperparameter_overrides_or_weights
evaluated_through_event_id / evaluated_through_at
valid_from / expires_at
model_ref / prompt_version / tool_versions
evidence_refs / created_at
```

超参数必须符合 Strategy 声明的 Schema、允许范围和版本。Evaluator 不能修改策略
代码、硬风险上限、Account、Venue 或 Order Instruction。置信度低、证据冲突或无法
判断市场时产生 `UNKNOWN`；Strategy Runner 随后默认输出 `NONE`。

## Multi-agent 实现

`ISigbotMarketEvaluator` 是可替换能力。主要目标实现
`SigbotMultiAgentMarketEvaluator` 使用固定内部评审顺序，而不是用户定义 DAG：

1. `BootAgent` 初始化 Run、Policy、Model 与可复现 Metadata。
2. `LoaderAgent` 组装获准的 Feed Window 与可选外部证据。
3. `AlphaAgent` 提议 Regime、Confidence 与受限 Parameter Value。
4. `AuditorAgent` 校验证据、Schema、时间边界、Risk Constraint 与矛盾结论。
5. Alpha 与 Auditor 最多迭代配置次数；耗尽后产生 `UNKNOWN`，不能发布未经审核的建议。

Boot 与 Loader 的输入独立时可以并行准备；Alpha/Auditor Loop 必须顺序且有界。MCP
Tool 可以补充上下文证据，但不能替代规范 Datafeed Event；Replay 时也不能访问未来或
实时数据。

## Runtime 与扩缩容

- Controller 为每个 Strategy Pipeline 管理一组 Evaluator Worker。
- 该 Worker Set 的 Replica 共用 Evaluator-specific Consumer Group。
- Evaluator 与 Strategy Runner 禁止共用 Group；两者都需要完整 Feed Stream。
- Stateful Window 按稳定 Feed/Instrument Key 分区；扩容必须保持顺序，并避免一个
  Window 同时有两个 Active Owner。
- Evaluation 异步运行；Strategy Runner 使用最新有效 Snapshot，不让每个行情 Tick
  阻塞等待 LLM。

## 扩展点

| 角色 | 契约 | 实现 |
|---|---|---|
| Service Root | `SigbotEvaluator` | Strategy-scoped Evaluation Runtime |
| 市场评估 | `ISigbotMarketEvaluator` | `SigbotMultiAgentMarketEvaluator` |
| Agent | `ISigbotAgent` | Boot、Loader、Alpha 与 Auditor Agent |
| Model/Tool Access | Provider-specific Port | 受 Run Policy 约束的 LLM 与 MCP Adapter |

## 安全与可观测性不变量

- 每个 Snapshot 不可变，并关联 Input Watermark、Model、Prompt、Tool 与 Evaluator
  Version。
- 只有通过 Schema 与 Policy Validation 后才能发布输出。
- Timeout、Tool Error、非法 Model Output 或 Review 不完整必须产生类型化失败或
  `UNKNOWN`；禁止静默延长过期结果。
- Prompt 与 Evidence 必须防止不可信指令提权；Tool 使用显式 Allowlist 与最小权限
  Credential。
- Metric 分离 Queue Delay、Context Loading、Model Latency、Review Round、Validation
  Failure、Regime Distribution 与 Snapshot Staleness。
- Log 禁止包含 Secret 或未脱敏私有 Feed 内容。

## 实现对齐

当前源码暴露 `SigbotEvaluatorRunner`、`ISigbotEvaluationExecutor`、
`SigbotMasEvaluationExecutor` 及 Boot、Loader、Alpha、Auditor Agent 结构。Event-driven
Datafeed Subscription 尚未完成，部分数据准备仍是 Placeholder；当前 Hyperparameter
Event 缺少完整 Regime、Confidence、Watermark、Expiry 与 Provenance 契约，部分标识
仍硬编码，Runtime 也存在直连核心数据库路径。

必须对齐到 `SigbotEvaluator`、`ISigbotMarketEvaluator`、APIServer 提供的不可变配置、
真实 Feed Subscription、有界 Agent Review 与类型化 `SigbotEvaluationSnapshot` 发布。

## 源码地图

- Evaluator Runtime 与 Executor：`src/evaluator/src/`
- Multi-agent 实现：`src/evaluator/src/executor/adk/`
- 共享 Evaluation/Message 类型：`src/types/src/modules/evaluator/`
