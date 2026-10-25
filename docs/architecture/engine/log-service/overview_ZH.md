# Log Service

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 辅助可观测性 Service；当前已有部分 Real-time 与 Archive 路径。

`SigbotLogService` 收集结构化运维日志，向已授权实时 Client Fan-out，并归档到专用
Observability Store。它不在 Trading Decision Path 上，Order Placement 或 Wallet
Accounting Commit 不能依赖它。

## Log 契约

`SigbotLogEvent` 包含 Event ID、Timestamp、Severity、Tenant/Pipeline Scope、Component、
Instance、Trace/Correlation ID、Message 与结构化脱敏字段。Producer 每个逻辑 Record
发布一个 Event；Multi-line Text 必须显式表示，不能由下游重新解析成结构。

该 Event Schema 与 Audit Event、Business Fact 分离。Security Audit 使用其所属 Audit
Contract；Trade、Fill 与 Ledger 状态从 Source of Truth 查询，不能从 Log 重建。

## 处理路径

```text
component logs -> Messager
  -> real-time consumer -> bounded per-scope stream -> WebSocket/API clients
  -> archive consumer group -> batch writer -> observability store
```

慢 Client 场景下，Real-time Fan-out 可以按 Policy 丢弃/合并低优先级 Record，但必须
暴露 Gap Marker。Archive Path 使用独立 Consumer Group 与 Durable Checkpoint；两个
路径必须各收一份 Event，不能共用一个 Group。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotLogService` | Real-time 与 Archive Supervisor |
| Processing | `ISigbotLogManager` | `SigbotDefaultLogManager` |
| Archive | `ISigbotLogArchive` | 显式选择 PostgreSQL/TimescaleDB、Object Storage 或 Log Backend |
| Live Stream | `ISigbotLogStream` | Bounded WebSocket Fan-out |

Observability Store 由 Log Service 拥有，不属于核心控制面数据库。APIServer 可以认证/
Proxy Log Query，但其他组件不能共享 Archive Table。

## 不变量

- Producer 在发布前脱敏 Credential、Key、Token、Prompt、私有 Account Data 与 Strategy
  Source；Service 再应用第二层 Policy Filter。
- Log Ingest、Query 与 Stream 都校验 Tenant/Pipeline Authorization。
- Archive Idempotency 按 Event ID 定义，Retry 不能产生重复 Record。
- 慢 Archive 或 WebSocket Client 不能在没有有界、可观测 Policy 的情况下反压交易
  Message Loop。
- Retention、Legal Hold、Deletion 与 Export Rule 按 Data Class 显式定义。
- Health/Metric 暴露 Lag、Drop、Gap Marker、Archive Failure 与 Connected Client，
  但不记录 Payload Content。

## 实现对齐

当前源码暴露 `SigbotLogServer`、`ISigbotLogManager`、
`SigbotDefaultLogManager`、Per-workflow Broadcast State，并通过核心 `LogHandler`
转换归档。它会初始化 `SigbotState` 访问数据库，Payload 仍面向 Workflow/Node，当前
Messager 语义也尚不具备文档要求的独立 Shared Consumer 行为。

必须对齐到 `SigbotLogService` Root、Pipeline/Component-scoped 结构化 Event、独立拥有
Observability Store、两条真实 Consumer Path、Authorization、Redaction、Idempotent
Archive 与 Bounded Fan-out。

## 源码地图

- Log Service：`src/logservice/`
- Log API 与当前 Persistence Handler：`src/core/src/sys/`
- 共享 Log 类型：`src/types/`
