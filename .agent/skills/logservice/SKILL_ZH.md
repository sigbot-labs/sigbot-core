---
name: logservice
description: 日志服务模块编码指南。相关模块：src/logservice/**/*.rs (日志服务实现), src/core/src/sys/route/log_router.rs (日志 API 路由), src/core/src/sys/handler/log_handler.rs (日志 API 处理器)
---
您正在开发日志服务模块 (`SigbotLogServer`)。该微服务处理工作流执行日志的日志收集、归档和实时流式传输。

**核心数据流**：

1. **日志生成**：工作流节点（由微服务执行）通过 `TOPIC_WF_LOG: /internal/v1/{TENANT_ID}/{WORKFLOW_ID}/{NODE_ID}/log` 将日志发布到 EMQX。格式：`{ workflow_id, node_id, content: Vec<String> }`。

2. **双重消费者**（独立订阅，无重复）：
   - **实时推送**：常规订阅 → 解析 `LogEntry` → 通过 WebSocket (`/ws/logs`) 广播 → UI 前端（防止积压）。
   - **日志归档**：共享订阅 `$share/log-archiving/{topic}` → 解析 `LogEntry` → 转换为 `AppendLogRequest` → 通过 `LogHandler.append()` 持久化 → 数据库（确保重启时不丢失数据）。

3. **前端集成**：UI 连接到 `/ws/logs`，发送带有 `workflow_id` 的订阅/取消订阅，接收实时日志流。

**关键实现**：

- **日志归档**（`SigbotDefaultLogManager`）：处理 `content` 数组中的每个日志行，提取日志级别，构建元数据（`service_name="workflow"`，`tags="workflow_id:{id},node_id:{id}"`），在失败时继续。
- **实时流式传输**（`LogServiceState`）：使用 `DashMap` + `broadcast::channel` 进行每个工作流的订阅，限制容量（1000）。
- **工厂模式**：使用 `SigbotLogManagerFactory::init()`，在 `LogManager.init()` 中初始化 `SigbotState` 以进行数据库访问。
- **错误处理**：优雅地处理反序列化失败，记录带有上下文的错误，不要停止处理。
- **性能**：异步处理，对共享状态使用 Arc/Mutex，避免阻塞操作。
