# Messager

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性内嵌 SDK 架构。Local 与 MQTT Client 已存在，但可观测 Subscription
行为尚不等价。

Messager 是 SigBot 集群内部异步通信 SDK，不是业务 Service，也不拥有 Trading
Payload。All-in-one 模式使用 Local In-memory 实现，分布式模式通过 EMQX 使用 MQTT；
Service 只依赖 `ISigbotMessagerClient`。

## 异步 Port

```text
ISigbotMessagerClient
  async publish(topic, envelope, delivery_options)
  async subscribe(SubscriptionSpec, handler) -> SubscriptionHandle
  async close()

SubscriptionSpec
  topic_filter
  consumer_group?
  qos
  ordering_key?
  capacity / backpressure_policy
```

`SubscriptionHandle` 拥有 Cancellation 与 Graceful Drain。Callback 只有在处理结果已
知时才完成；Durable Transport 按选定 Delivery Contract ACK。

实现为 `SigbotLocalMessagerClient` 与 `SigbotMqttMessagerClient`。Local 模式必须模拟
与 MQTT 相同的 Fan-out 和 Consumer Group 语义，不能用改变应用行为的单 Handler
简化 Queue。

## Topic 与 Consumer-group 语义

规范内部 Topic：

```text
/internal/v1/{tenant_id}/{pipeline_id}/{domain}/{event}
```

Publisher 始终使用普通 Topic。对于 MQTT，Consumer Group 只编码在 Subscription
Filter 中：

```text
$share/evaluator-{pipeline_id}/internal/v1/.../feed/event
$share/strategy-{pipeline_id}/internal/v1/.../feed/event
```

Evaluator 与 Strategy Runner 必须使用不同 Group，确保双方都收到每个 Feed Event；
同一组件的 Replica 共用本组件 Group 以分担工作。Stateful Consumer 还必须使用稳定
Partition/Ownership；仅依赖随机 Shared-subscription Delivery 无法维护 Rolling Window。

没有 Group 时，每个 Active Subscription 各收一份；有 Group 时，每条消息由一个
Active Member 接收。Local 与 MQTT 模式必须遵守相同规则。

## Envelope

`SigbotEventEnvelope<T>` 包含 Event ID、Tenant ID、Pipeline ID、Producer、Payload
Schema/Version、Event Time、Publication Time、Correlation/Causation ID、Partition
Key 与 Trace Context，不包含 Broker Credential 或 Provider Secret。

Producer 拥有 `T` 及其兼容性 Policy：

- Datafeed 拥有 `SigbotFeedEvent`。
- Evaluator 拥有 `SigbotEvaluationSnapshot`。
- Strategy Runner 拥有 `SigbotTradeSignal`。
- Order Manager 拥有 `SigbotExecutionEvent`。
- Wallet Manager 拥有 `SigbotPortfolioSnapshot`。

Messager 只拥有 Serialization Framing、Size Limit、Subscription Lifecycle、Delivery
Metadata 与 Transport Error Mapping，不拥有业务 Schema。

## 投递与 Backpressure

- 默认处理为 At-least-once；Consumer 按 Event ID 与领域 Key 实现幂等。
- 只在显式声明 Partition 和 Transport Capability 内保证顺序，不承诺全局顺序。
- Queue 必须有界；`block`、`reject`、`coalesce` 或 `drop` Policy 按 Event Class 显式
  定义；Money/Order Fact 禁止使用 Lossy Policy。
- Retry 区分 Transport Failure 与 Handler Failure；Poison Message 进入有界 Retry/
  Dead-letter 或 Operator-visible Failure Contract。
- Reconnect 必须恢复全部 Subscription 后才 Ready；Broker 无法 Replay 时暴露 Gap。
- 慢 Handler 在 Network Event Loop 外执行，不能阻塞 Keepalive 或无关 Subscription。

## 安全不变量

- Topic Authorization 在 Publisher 与 Subscriber 边界都校验 Tenant/Pipeline Scope。
- 分布式环境的 MQTT 使用 TLS 与 Scoped Credential。
- 调用 Handler 前校验 Payload Size、Decompression、Schema 与 Deserialization。
- Trace/Log Metadata 必须脱敏，不包含 Payload Secret。
- Wildcard Subscription 需要显式 Service Policy；用户提供的 Topic Filter 不能越出
  所属 Scope。

## 实现对齐

当前源码暴露 `ISigbotMessagerClient`、Local/MQTT Client 与异步 Publish/Subscribe，
但没有显式 Consumer-group Model。Local 模式实际上每个 Exact Topic 只有一个 Receiver，
缺少等价 Fan-out/Wildcard；MQTT Event Loop 可能把后续 Subscription 也交给第一个
Handler。各 Producer/Consumer 对 Topic Placeholder 的展开不一致，仓库也尚未使用
`$share`。

必须对齐到 `SubscriptionSpec`、多个独立 Handler、Consumer Group、Local/MQTT 行为
等价、规范 Topic 展开、有界 Backpressure、Cancellation 与两种实现共用的 Contract
Test。

## 源码地图

- Messager SDK 与 Client：`src/messager/`
- Topic Constant 与 Message 类型：`src/types/src/modules/messager/`
