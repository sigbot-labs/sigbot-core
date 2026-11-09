# Exporter

[English](exporter.md) · [架构总览](../../overview_CN.md)

**状态：** 已实现租户/Workflow 级 Kafka 与 Google Sheets 导出组件；MQTT/HTTP 目标仍为保留设计。

`SigbotExporterServer` 是 Controller 管理的租户/Workflow 组件。它消费经过批准、带 Schema Version 的业务事件，
投递到用户配置的下游系统，用于分析、数仓和自动化。初始规范输入是 Order Manager
Execution Stream。

## 边界与术语

`ExportNodeConfig` 是用户配置，`ExportSinkConfig` 选择投递 Adapter。一个 daemon
承载相互隔离的 Target Instance：

```text
SigbotExporterServer
  -> IExporter
  -> ExportSinkConfig
       -> KafkaExporter
       -> GoogleSheetsExporter
```

产品名称必须是 **Google Sheets**，不是 “Google Street”。外部 MQTT Sink 与内部
Messager Transport 即使都使用 MQTT，也属于不同边界。

## 输入与投递

Export Envelope 包含 env/tenant/Workflow/node/run Scope、Business Schema/Version、
Event/Available/Publication Time、W3C Trace Context 与获准 Payload，不暴露 Exchange Credential、私有
Strategy Code 或 Target Data Policy 排除的字段。

对于每个配置 Target，Exporter：

1. 校验 Tenant Ownership、Event Category、Schema Compatibility 与 Field Projection；
2. 从 Envelope Scope、Run、源节点和序列派生稳定 Delivery Key；
3. 校验 Producer 提供且已有界的 `ExportBatch`；
4. 通过选定 Sink 写入；
5. ACK Durable Progress 前保存 Checkpoint/Receipt State；
6. Retry Transient Failure，并 Dead-letter Permanent/Exhausted Failure。

Kafka 是 Streaming Sink；Google Sheets 是受 Rate Limit 的 Batch Sink，使用稳定 Row
Identity，不能作为事务型 System of Record。MQTT/HTTP Sink 是保留扩展，不是当前实现。

## 扩展点

| 角色                  | 契约                    | 实现                                                     |
| --------------------- | ----------------------- | -------------------------------------------------------- |
| Service Root          | `SigbotExporterServer`  | 组件启动与 Provider 注册                                 |
| Routing/Orchestration | `IExporter`             | `ExportService`                                          |
| Destination           | `ExportSinkConfig`      | `KafkaExporter`、`GoogleSheetsExporter`                  |
| Persistence           | Core Handler/Repository | `ExporterPostgresRepository`、`ExporterSQLiteRepository` |

新增 Sink 不创建新微服务，也不能独立订阅任意内部 Topic。Exporter 只维护一份
Subscription、Routing、Policy、Checkpoint 与 Lifecycle。

## 可靠性与安全不变量

- At-least-once Input 在 Target 支持幂等时不能产生重复 Effect；无法保证时必须明确
  并可审计 Duplicate Behavior。
- Kafka 的有界 Producer Queue 提供 Backpressure。当前组件通过已配置 Sink 顺序处理领取
  的请求；尚未实现 Per-target Worker 隔离。
- Batch Shape/Size 不兼容时拒绝请求，禁止静默丢弃或改形数据。
- Target Credential 使用 Tenant-scoped Secret Reference，并从 Event、Log、Status
  中脱敏。
- 未来 HTTP Destination 必须强制 HTTPS、Allowlist/Private-network Policy、Timeout、
  Redirect 与 Response-size Limit。
- Range Replay 使用与 Live Delivery 相同的 Delivery Key 与 Projection Version。

## 实现对齐

`SigbotExporterServer` 已消费类型化 Export Request，并路由至 Kafka 或 Google Sheets。
Kafka 使用幂等 Producer、`acks=all`、稳定业务 Key、有界 Producer Queue 和持久
Checkpoint；Sheets 使用固定 Range 的 ValueRange PUT 和 Secret 引用。Scope、Checkpoint、
身份冲突及重启行为持久化。Retry 来自携带同一业务 Key 的 At-least-once 消息重投；当前
没有独立 Durable Exporter Retry Scheduler。

Kafka Broker ACK 与 PostgreSQL Commit 无法形成同一事务，崩溃窗口仍可能重复业务 Key。
真实 Google 凭据、Kafka 多 Broker 故障切换，以及保留的 MQTT/HTTP Sink 尚未验收。

## Google Sheets 协议边界

Google Sheets 通过 `reqwest::Client` 与官方 ValueRange JSON 形状执行固定 Range 的 values.update PUT，支持显式可选 `proxy_url`，不需要第二套 SDK/Client/Factory 或 append 操作。凭据只使用中央 Secret-map 引用，业务模块不直接读取环境。固定范围和稳定身份在重试/重放中保持不变。当前没有独立耐久重试调度器；永久/耗尽失败必须显式暴露，不能报告成功。

## 源码地图

- Exporter 组件与 Sink：`src/component/exporter/`
- Exporter Repository：`src/core/src/modules/exporter/storage/`
- Export 配置与 Event 类型：`src/types/src/exporter/`
