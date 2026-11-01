# Strategy 宿主 Rust 日志归档

配置现有 `StrategyNodeConfig.log_archive` 即启用宿主 Rust tracing 采集，与 Python stdout/stderr/exception 共用 `LogArchive`、SQLite WAL、S3 不可变对象和 PG 游标索引。Logservice 是九个租户级数据面组件之一，由 Controller 按 tenant/workflow 隔离部署。

这里的 `append` 是对有序日志流的逻辑追加，不是修改 S3 现有对象。接收端先把完整记录提交到本地 SQLite WAL 后 ACK；归档器随后以 `PutMode::Create` 为每个有界序号创建新的不可变对象，遇到同名对象时校验 SHA-256，再提交 PostgreSQL 游标索引并释放 WAL payload。S3 从未承担 append-write。

Core 的现有 logging subscriber 保留空 route layer 的 reload handle，只提供泛型 layer 安装/移除接口。安装与移除原位修改 layer 和 filter，保留 subscriber 初始化时注册的 FilterId。具体 `ArchiveTracingLayer` 由 Logservice 提供，Strategy Runtime 负责装配；Core 与 common-telemetry 不引用 Logservice。现有 stderr、日志格式和 OTel layer 保留。

## 身份与采集范围

每个安装的 capture 持有固定 `env/tenant_id/workflow_id/node_id/run_id`。这些值来自宿主运行配置，事件字段不能覆盖；不使用全局用户上下文。进程已安装一个归档 route 时，第二次安装会失败，避免把多个身份的宿主日志混入同一流。

首个接入宿主是配置了 `log_archive` 的 Strategy 进程，采集该进程内 `sigbot_` 和 `common_telemetry` target 的 INFO/WARN/ERROR tracing 事件。第三方库、其他服务进程、DEBUG/TRACE 以及 `sigbot_archive_internal` 诊断不进入该归档。配置安装前的进程启动日志也不在此范围。

MQTT handler 装饰器将输入已有 `traceparent` 作为标准 OTel consumer span 的父上下文，并用 `Instrument` 跨异步 poll 传播 span；日志追加沿用该 trace。没有 ambient span 的宿主事件明确标记 `background=true`，使用标准 OTel 根上下文，不新增日志关联 ID。

## 持久性和背压

- tracing 的同步 `on_event` 只做有界字段采集、脱敏和内存入队；宏返回不代表持久化 ACK。
- 默认队列容量 256，单字段最多 1024 字节、最多 24 个字段，最终内容最多 16 KiB。超界信息明确标记截断。
- `flush_durable()` 的 FIFO barrier 等待此前已接受记录完成 SQLite WAL `FULL` 提交。Strategy 启动报告 ready 前、每个 MQTT handler 成功返回前均执行 barrier；默认超时 5 秒。
- 满队列不能静默报告成功：拒绝计数增加，capture readiness 置失败，后续 barrier 返回错误。拒绝事件的完整性故障保持到该 capture 重建；写盘暂时失败则保留队列头记录重试，恢复后可继续落盘。
- 队列拒绝或归档采集任务异常退出属于不可恢复状态，也使 liveness 失败；临时 WAL 错误只影响 readiness。任务 guard 在 spawn 前创建，正常关闭必须完成 WAL barrier 后才能标记成功结束。
- `accepted/durable/rejected/write_failures` 统计可从 capture 获取；内部故障诊断使用固定独立 target，避免归档自身日志形成循环。
- 关闭先停止 MQTT intake，再在极短同步锁中关闭事件 admission，随后等待最终 WAL barrier。该锁只保护关闭检查与入队，不覆盖格式化或异步 I/O，避免关闭成功后仍接受未落盘事件。

内存入队到 WAL 提交之间存在明确的未确认窗口；进程在 barrier 成功前被强制终止时，不能宣称这些内存事件已持久。WAL 提交成功以后才适用原归档的重启、条件上传、checksum 去重和 PG 游标读取保证。S3 上传和 PG 索引成功是归档完成边界，不是本地追加 ACK 的前提。

## 序号与恢复

自动 Rust 日志与 Python 日志统一使用 `LogAppend` / `append_generated`。每个 log stream 的序号在 SQLite `log_sequence` 中分配，并与完整 `log_buffer` 行同事务提交，因此日志接收不依赖 PG 或 S3 在线。首次升级扫描该流现有 WAL 记录和幂等 tombstone 恢复高水位，不重编号旧记录。

原 `append(&DurableMessage<LogLine>)` API 保持固定身份去重语义；写入显式序号也会推进本地高水位。未确认 COMMIT 的 generated draft 在重试时先用 `BEGIN IMMEDIATE` 核验 tentative key 和不可变 fact hash：相同记录已存在则去重；无记录或该序号属于其他 fact 时，证明本 draft 没有提交，重新分配序号并保留对方原记录。已经确认的记录不会改号。

同一日志流的宿主必须继续使用同一个持久 `spool_path` 和单 owner 部署。独立 WAL 文件不能充当同一 `node/run` 的并行序号分配器。

## 字段处理与验收入口

`requestSpanId`、`request_span_id` 等临时配对字段直接丢弃，包括 JSON message 的嵌套字段。无法安全解析、但包含配对键或敏感键赋值的 Debug 文本整段省略。敏感字段名、Bearer 凭据、URI 用户信息和常见 credential 参数会脱敏；任意不带标识的自由文本不能被宣称拥有通用秘密识别能力，调用方仍不得把凭据写入 message。

独立测试 target `sigbot-logservice --test tracing_archive` 包含字段处理、真实 SQLite tentative 竞争/重启、满队列、可控格式化暂停下的 shutdown 竞态。ignored 集成测试 `actual_tracing_offline_wal_restart_minio_index_and_cursor` 使用实际 Core subscriber 初始化、安装、移除及重装路径；先让 PG/S3 endpoint 不可用，发出真实 tracing 事件并确认 WAL，再重开同一 WAL，使用真实 MinIO/PG 验证对象、脱敏、trace、游标、重启序号和租户读取隔离。实际服务链的 `logs` 场景同时要求 Python stdout 和 Rust tracing 到达不可变对象及 PG 索引。测试执行结果以实际 runner 报告为准。
