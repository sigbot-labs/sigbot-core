# Exporter

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前 Google Sheets 与 Kafka Writer 仍是 Placeholder。

`SigbotExporter` 是共享导出 daemon。它消费经过批准、带 Schema Version 的业务事件，
投递到用户配置的下游系统，用于分析、数仓和自动化。初始规范输入是 Order Manager
Execution Stream。

## 边界与术语

`ExportTarget` 是用户配置；`ISigbotExportSink` 是执行投递的 Adapter。一个 daemon
承载多个相互隔离的 Target Instance：

```text
SigbotExporter
  -> ISigbotExporter
  -> ISigbotExportSink
       -> SigbotKafkaExportSink
       -> SigbotMqttExportSink
       -> SigbotHttpExportSink
       -> SigbotGoogleSheetsExportSink
```

产品名称必须是 **Google Sheets**，不是 “Google Street”。外部 MQTT Sink 与内部
Messager Transport 即使都使用 MQTT，也属于不同边界。

## 输入与投递

Export Envelope 包含 Event ID、Tenant/Pipeline Scope、Business Schema/Version、
Event/Publication Time、Causation 与获准 Payload，不暴露 Exchange Credential、私有
Strategy Code 或 Target Data Policy 排除的字段。

对于每个配置 Target，Exporter：

1. 校验 Tenant Ownership、Event Category、Schema Compatibility 与 Field Projection；
2. 派生稳定 `{target_id, event_id}` Delivery Key；
3. 按 Sink 语义与用户 Policy Buffer/Batch；
4. 通过选定 Sink 写入，不能阻塞 Message Callback；
5. ACK Durable Progress 前保存 Checkpoint/Receipt State；
6. Retry Transient Failure，并 Dead-letter Permanent/Exhausted Failure。

Kafka 与 MQTT 是 Streaming Sink；HTTP 支持 Idempotency Key 与签名请求；Google Sheets
是受 Rate Limit 的 Batch Sink，使用稳定 Row Identity，不能作为事务型 System of
Record。

## 扩展点

| 角色 | 契约 | 实现 |
|---|---|---|
| Service Root | `SigbotExporter` | 共享 Subscription 与 Target Supervisor |
| Routing/Orchestration | `ISigbotExporter` | `SigbotPolicyExporter` |
| Destination | `ISigbotExportSink` | Kafka、MQTT、HTTP、Google Sheets Sink |
| Projection | `ISigbotExportTransformer` | 版本化 Event-to-target Schema Mapping |

新增 Sink 不创建新微服务，也不能独立订阅任意内部 Topic。Exporter 只维护一份
Subscription、Routing、Policy、Checkpoint 与 Lifecycle。

## 可靠性与安全不变量

- At-least-once Input 在 Target 支持幂等时不能产生重复 Effect；无法保证时必须明确
  并可审计 Duplicate Behavior。
- Backpressure 与有界 Queue 隔离慢 Target，不能影响其他 Target 或交易主链。
- Schema 不兼容时停止该 Target 并暴露 Condition，禁止静默丢弃或改形数据。
- Target Credential 使用 Tenant-scoped Secret Reference，并从 Event、Log、Status
  中脱敏。
- HTTP Destination 强制 HTTPS、Allowlist/Private-network Policy、Timeout、Redirect
  与 Response-size Limit，防止 SSRF 和资源耗尽。
- Range Replay 使用与 Live Delivery 相同的 Delivery Key 与 Projection Version。

## 实现对齐

当前源码暴露 `SigbotExporterServer`、`ISigbotExporterManager`、Google Sheets 与
Kafka Manager，并订阅 Placed-trade Topic。Writer 当前只记录预期写入，没有真实投递；
MQTT/HTTP Sink、Typed Target Routing、Idempotency、Checkpoint、Retry/Dead-letter
State 与隔离均未实现。部分源码文件名与 Alias 仍使用 “googlestreet”。

必须对齐到 `SigbotExporter` Root、单一 Subscription Supervisor、
`ISigbotExportSink` Adapter、正确 Google Sheets 命名与 Durable Per-target Delivery
语义。

## 源码地图

- Exporter Service 与 Manager：`src/exporter/`
- Export 配置与 Event 类型：`src/types/src/modules/`
