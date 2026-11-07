# Logger

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 已实现租户/Workflow 日志归档组件，具备本地持久 Spool、不可变对象分块和
Cursor 读取；尚未实现实时 WebSocket Fan-out API。

`SigbotLogServer` 收集结构化运维日志并归档到专用 Observability Store。它不在 Trading
Decision Path 上，Order Placement 或 Wallet Accounting Commit 不能依赖它。

## Log 契约

`LogLine` 包含 Timestamp、Severity、env/tenant/Workflow/node/run Scope、Component、
Instance、W3C Trace/Span ID、Message 与结构化脱敏字段。Producer 每个逻辑 Record
发布一个 Event；Multi-line Text 必须显式表示，不能由下游重新解析成结构。

该 Event Schema 与 Audit Event、Business Fact 分离。Security Audit 使用其所属 Audit
Contract；Trade、Fill 与 Ledger 状态从 Source of Truth 查询，不能从 Log 重建。

## 处理路径

```text
component logs -> Messager
  -> archive consumer -> durable SQLite WAL
  -> immutable object chunk -> PostgreSQL cursor index
```

当前组件只实现 Archive Path。未来 Real-time Fan-out 必须使用独立命名订阅和有界
Per-client Policy，确保每个合格事件都能送达且不会抢走归档消费。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotLogServer` | 组件启动与 Capture 生命周期 |
| Processing | `ILogger` | `S3Logger` |
| Capture | `ArchiveCapture`、`ArchiveTracingLayer` | Python Stream 与 Rust tracing |
| Persistence | `LoggerSQLiteRepository`、`LoggerPostgresRepository` | 耐久 Spool 与 Chunk Index |

Logger 拥有 `log_chunk` 表领域与对象 key。当前最终态 Schema 可在物理上与控制面表
共置，但其他组件不能写该表。

## 不变量

- Producer 在发布前脱敏 Credential、Key、Token、Prompt、私有 Account Data 与 Strategy
  Source；Service 再应用第二层 Policy Filter。
- Log Ingest、Query 与 Stream 都校验 env/tenant/Workflow Authorization。
- Archive Idempotency 按 Event ID 定义，Retry 不能产生重复 Record。
- 慢 Archive 不能在没有有界、可观测 Policy 的情况下反压交易 Message Loop；Producer
  Capture Queue 必须有界。
- Retention、Legal Hold、Deletion 与 Export Rule 按 Data Class 显式定义。
- Health/Capture Statistic 暴露 Durable、Rejected 与 Write Failure；更完整 Archive Lag
  和 Live-client Metric 仍待增强。

## 实现对齐

当前源码由 `SigbotLogServer` 注册 `ILogger` Provider，组件负责采集、WAL 缓冲、
不可变 S3 分块和游标读取；核心 `LogHandler` 负责业务边界，数据库操作只存在于
Logger Repository。`append` 是逻辑追加：ACK 在 SQLite WAL `FULL` 持久提交后返回，
随后每个分块以条件创建写入新的 S3 Key，绝不修改已经存在的 S3 对象。

Strategy Host 的 Rust tracing 与 Python stdout/stderr/exception 已走这条路径。其他 Rust
Host 的全量采集、宿主卷损毁恢复和生产桶复制/保留策略仍是运维缺口。

## 源码地图

- Logger：`src/component/logger/`
- Log Handler 与 Repository：`src/core/src/modules/logger/`
- 共享 Log 类型：`src/types/src/logger/`
