# Messager

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** v2 持久运行时使用 MQTT/EMQX 与 PostgreSQL Inbox/Outbox。Local 保留兼容入口，
尚不具备等价的持久会话、广播和恢复保证，不能作为金融链路的可靠传输替代。

Messager 是 SigBot 集群内部异步通信 SDK，不是业务 Service，也不拥有 Trading
Payload。Service 依赖 `IMessager`；即使多个业务组件在同一宿主中运行，v2 金融
链路仍使用受 scope 隔离的 MQTT 会话和各领域自己的持久事务。

## 异步 Port

```text
IMessager
  async publish(topic, message)
  async publish_confirmed(topic, message)
  async subscribe(topic, handler)
  async close()
```

`publish_confirmed` 等待 broker PUBACK；`subscribe` 等待 SUBACK。Callback 只有在其承诺
的持久处理完成后返回，MQTT 才 ACK。关闭客户端终止连接和内部任务。

实现为 `LocalMessager` 与 `MqttMessager`。Local 当前为单 topic
内存队列；其限制必须在配置和验收中明确，不能宣称与 MQTT 重启恢复等价。

## Topic 与单 owner 语义

规范内部 Topic：

```text
internal/v2/{env}/{tenant_id}/workflow/{workflow_id}/{suffix}
internal/v2/{env}/{tenant_id}/account/{account_id}/{suffix}
```

Evaluator 与 Strategy 使用不同会话订阅相同 feed topic，各自收到完整输入。
策略实例按 scope/workflow/node/run 单 owner 串行执行，数据库 checkpoint 版本防止并发
提交。当前实现没有 `$share` 消费组契约，不能用随机共享订阅替代状态所有权。

## Envelope

`MessageEnvelope<T>` 包含版本、scope、Workflow/Account、源/目标节点、run_id、seq、
attempt_no、事件/可得/生产时间和 traceparent。`DurableMessage<T>` 保存相同业务事实，
排除仅用于临时配对的 requestSpanId。禁止新建关联 ID，也不包含 Broker Secret。

Producer 拥有 `T` 及其兼容性 Policy：

- Datafeed 发布 `DatafeedEvent`。
- Evaluator 发布 `HyperparametersUpdate`。
- Strategy 发布 `TradeIntent` 和查询请求。
- Order 发布独立的接受回执、`ExecutionFact` 与 `OrderStatusFact`。
- Wallet 发布版本化组合快照与预占结果。

Messager 只拥有 Serialization Framing、Size Limit、Subscription Lifecycle、Delivery
Metadata 与 Transport Error Mapping，不拥有业务 Schema。

## 投递与 Backpressure

- 默认处理为 At-least-once；Consumer 按 scope/node/run/seq 与领域自然键实现幂等。
- 只在显式声明 Partition 和 Transport Capability 内保证顺序，不承诺全局顺序。
- Queue 必须有界；`block`、`reject`、`coalesce` 或 `drop` Policy 按 Event Class 显式
  定义；Money/Order Fact 禁止使用 Lossy Policy。
- Retry 区分 Transport Failure 与 Handler Failure；Poison Message 进入有界 Retry/
  Dead-letter 或 Operator-visible Failure Contract。
- Reconnect 必须恢复全部 Subscription 后才 Ready；Broker 无法 Replay 时暴露 Gap。
- 慢 Handler 在 Network Event Loop 外执行，不能阻塞 Keepalive 或无关 Subscription。

## 安全不变量

- Topic Authorization 在 Publisher 与 Subscriber 边界都校验 env/tenant Scope。
- 分布式环境的 MQTT 使用 TLS 与 Scoped Credential。
- 调用 Handler 前校验 Payload Size、Decompression、Schema 与 Deserialization。
- Trace/Log Metadata 必须脱敏，不包含 Payload Secret。
- Wildcard Subscription 需要显式 Service Policy；用户提供的 Topic Filter 不能越出
  所属 Scope。

## 实现对齐

MQTT 按实际 topic 路由 handler，持久队列在订阅注册前等待。消费回调失败进入有界退避，
不会把未提交金融事实标为已处理。领域层可显式持久隔离不可接受的输入；隔离后重复投递
仍验证事实一致性。Local 广播、共享消费组与完整优雅排空契约尚未纳入本次等价验收。

## 源码地图

- Messager SDK 与 Client：`src/messager/`
- Topic Constant 与 Message 类型：`src/types/src/messager/`

## v2 可靠消息契约与迁移

唯一 topic 生成器位于 `sigbot_types::messager::topic`，完整展开 17 个 Workflow 后缀和 5 个 Account 后缀。路径无前导 `/`，env/tenant 和资源 ID 禁止通配符或占位符。Workflow 消费者还必须校验 source/target 节点对应的边。

`MessageEnvelope<T>` 是 wire DTO；`DurableMessage<T>` 是唯一可持久化表示。它们包含版本、隔离范围、节点、run_id、seq、attempt_no、事件/可得/生产时间和 traceparent。请求的 `requestSpanId` 等于 producer span_id，响应通过有上限、有 TTL 的 `ReplyPairingCache` 临时回显；该字段不会写入数据库，重启后的补放不依赖它。应用重试保持 run_id/seq，递增 attempt_no 并创建新的 producer span。

MQTT 使用 QoS 1、非 clean start、七天 session expiry 和手动 ACK。客户端从初始化开始持续轮询，发送方不需要先订阅。订阅完成必须收到成功的 QoS 1 SUBACK；Outbox 发送完成必须收到 broker PUBACK。业务 handler 返回成功表示已经提交 inbox/业务事务，之后才 ACK 消费消息。输入队列和并发有界、消费者串行处理；连接失败指数退避，持久会话消息等待 handler 注册完成。MQTT broker 的租户/角色 ACL 仍必须由部署配置启用，客户端校验不能替代 broker 授权。

`MessagerPostgresRepository::{accept,complete,enqueue,next_sequence}` 参与调用方事务。Inbox/Outbox 键包含 env/tenant、consumer/producer、源节点、run_id、seq。原子入库后由宿主中的出箱转发任务限批发送；失败保留 pending 和退避时间。收到 PUBACK 后再提交 sent 标记；这两个动作之间的崩溃会重投，因此下游必须使用 Inbox。共享库按 broker/client 会话隔离 Provider 实例，不按实现类型复用所有租户连接。
## 显式补放与隔离

`MessagerPostgresRepository::replay(producer, scope, node_id, run_id, first..=last)` 只重新排队指定域、生产者、节点、run 和有界序号区间中的 `sent` Outbox。调用方必须先验证运维权限；不会跨租户扫描或创造关联 ID。下一次真实发布保留业务键，递增 `attempt_no` 并创建新的 producer span。已完成或持久隔离的 Inbox 重投会校验事实一致性后直接 ACK；同身份而事实变化仍拒绝。

`src/messager/tests/postgres_replay.rs` 提供真实 PostgreSQL 与认证 EMQX 的集成验证，通过 `SIGBOT_TEST_DATABASE_URL`、`SIGBOT_TEST_MESSAGE_SCOPE` 和 `SIGBOT_TEST_MQTT_CONFIG` 注入测试配置。MQTT JSON 只保存密码环境变量名；测试从实际接收消息验证补放次数和 span，不修改捕获消息模拟重试。

MQTT client_id 必须包含现有 run 身份的稳定编码：同 run 重启继续原会话，不同 run 独立。
Workflow topic 本身仍可承载多个节点和 run。Strategy 先检查 scope/Workflow，再按目标、
输入边和 run 路由。与本实例不匹配的消息存入 `strategy.routing:{workflow}:{node}:{当前run}`
的 quarantined Inbox，原因分别为 `other_target`、`outside_input_edge`、`inactive_run`
（迁移 15 增加原因列），提交后 ACK。它不会占用真正目标的 `strategy.{node}` Inbox，
也不会读取当前虚拟时钟或调用 Python；跨 scope/Workflow 的消息仍拒绝。
