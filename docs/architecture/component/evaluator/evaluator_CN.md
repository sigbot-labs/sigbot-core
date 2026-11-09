# Evaluator

[English](evaluator.md) · [架构总览](../../overview_CN.md)

**状态：** 已实现异步评估旁路和类型化 Snapshot；真实模型质量与生产记忆检索仍缺证据。

`SigbotEvaluatorStarter` 是单个 Strategy Pipeline 的非确定性旁路。它观察规范化市场/新闻
事件，通过受限 LLM 过程评估市场结构，并向确定性 Strategy Runner 发布
不可变建议。它永远不产生 Order 或 Trading Signal。

## 输入与输出

Evaluator 通过 Strategy-specific Consumer Group 消费 `DatafeedEvent` Envelope，维护按
Event Time 排序的有界上下文窗口。Trigger 可以基于时间、事件数、波动率或显式请求，
但每次运行都必须记录精确 Input Watermark。

它在交易链路中的唯一输出是 `HyperparametersUpdate`：

```text
DurableMessage: env / tenant_id / workflow_id / node_id / run_id / seq / traceparent / produced_at
HyperparametersUpdate: version / as_of / valid_until
probabilities: trend / range / stabilizing / suppression / unknown
parameters
model_version / classification_version
case_refs
```

超参数必须符合 Strategy 声明的 Schema、允许范围和版本。Evaluator 不能修改策略
代码、硬风险上限、Account、Venue 或 Order Instruction。置信度低、证据冲突或无法
判断市场时产生 `UNKNOWN`；Strategy Runner 随后不输出开仓 Intent。

## Evaluation 实现

`IEvaluator` 是可替换能力，当前实现是 `LlmEvaluator`。它领取一个已准入的源事件，
只加载该事件水位前已持久化的事实和历史案例，调用一次有界 OpenAI-compatible
Endpoint，校验 `EvaluationAnswer`，再原子提交类型化更新与 Outbox。模型引用的事实必须
与边界实际提供的事实一致。未来如引入 Multi-agent Review，应隐藏在 `IEvaluator`
之后；当前代码并未实现 Agent Loop。

## Runtime 与扩缩容

- Controller 为每个 Strategy Pipeline 管理一组 Evaluator Worker。
- 该 Worker Set 的 Replica 共用 Evaluator-specific Consumer Group。
- Evaluator 与 Strategy Runner 禁止共用 Group；两者都需要完整 Feed Stream。
- Stateful Window 按稳定 Feed/Instrument Key 分区；扩容必须保持顺序，并避免一个
  Window 同时有两个 Active Owner。
- Evaluation 异步运行；Strategy Runner 使用最新有效 Snapshot，不让每个行情 Tick
  阻塞等待 LLM。

## 扩展点

| 角色                | 契约                    | 实现                                                       |
| ------------------- | ----------------------- | ---------------------------------------------------------- |
| Service Root        | `SigbotEvaluatorRunner` | 组件启动与 Provider 注册                                   |
| Evaluation Contract | `IEvaluator`            | `LlmEvaluator`                                             |
| Model Result        | `EvaluationAnswer`      | 已校验概率、Evidence 与参数                                |
| Persistence         | Core Handler/Repository | `EvaluatorPostgresRepository`、`EvaluatorSQLiteRepository` |

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

`SigbotEvaluatorRunner` 已消费作用域 Feed Event，只加载决策 Watermark 前可得的事实/案例，
调用有界 LLM，校验五态概率，并提交带版本 `HyperparametersUpdate` 与 Outbox。
事实与推断分表保存，引用案例必须来自本次允许输入。

E2E 使用明确的 OpenAI-compatible Mock，只证明契约，不证明模型收益、概率校准或生产
pgvector 案例库质量。

<a id="review-design"></a>

## 保留的评审与工具设计

早期 MAS 方案由 Boot/Loader 并行收集基础统计和预热行情/新闻，再由 Alpha 计算参数、Auditor 顺序校验；审计失败把有界反馈送回 Alpha（草案为最多 3 次重试、间隔 500 ms）。只读 Binance/Twitter 工具、数学辅助、缓存特征、逐工具 QPS/超时/熔断、输入输出哈希均应隐藏在 `IEvaluator` 之后并显式 Allowlist；工具和评审不能修改硬风控或直接交易。这里只保留设计选择，旧 Manager/Job Registry、Cron 扫描器、兼容别名、Mock Tool 和 ADK 兼容补丁均已删除，不是当前运行能力。

动态建议异步预计算，只能在记录的版本和有效期内复用，一次决策只用一个快照。早期内嵌参数服务或静态回退提案不能覆盖当前 UNKNOWN/过期输入不开仓的规则。未来工具或 Agent Loop 仍需保持数据有界、来源可查、可审计和可重放，不能另建交易路径。模型完成的实际时间才是输出可得时间，不能回填成输入水位。真实案例反馈、pgvector 检索质量和概率校准仍待验收。

## 源码地图

- Evaluator Runtime 与 LLM Adapter：`src/component/evaluator/src/`
- Evaluator Repository：`src/core/src/modules/evaluator/storage/`
- 共享 Evaluation/Message 类型：`src/types/src/evaluator/`
