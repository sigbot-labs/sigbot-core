---
name: logservice
description: Log service module coding guidelines. Related modules: src/logservice/**/*.rs (log service implementation), src/core/src/sys/route/log_router.rs (log API routes), src/core/src/sys/handler/log_handler.rs (log API handlers)
---
You are working on the Log Service module (`SigbotLogServer`). This microservice handles log collection, archiving, and real-time streaming for workflow execution logs.

**Core Data Flow**:

1. **Log Generation**: Workflow nodes (executed by microservices) publish logs to EMQX via `TOPIC_WF_LOG: /internal/v1/{TENANT_ID}/{WORKFLOW_ID}/{NODE_ID}/log`. Format: `{ workflow_id, node_id, content: Vec<String> }`.

2. **Dual Consumers** (separate subscriptions, no duplication):
   - **Real-time Push**: Regular subscription → Parse `LogEntry` → Broadcast via WebSocket (`/ws/logs`) → UI frontend (prevents backlog).
   - **Log Archiving**: Shared subscription `$share/log-archiving/{topic}` → Parse `LogEntry` → Convert to `AppendLogRequest` → Persist via `LogHandler.append()` → Database (ensures no data loss on restart).

3. **Frontend Integration**: UI connects to `/ws/logs`, sends subscribe/unsubscribe with `workflow_id`, receives real-time log streams.

**Key Implementation**:

- **Log Archiving** (`SigbotDefaultLogManager`): Process each log line in `content` array, extract log level, build metadata (`service_name="workflow"`, `tags="workflow_id:{id},node_id:{id}"`), continue on failures.
- **Real-time Streaming** (`LogServiceState`): Use `DashMap` + `broadcast::channel` for per-workflow subscriptions, limit capacity (1000).
- **Factory Pattern**: Use `SigbotLogManagerFactory::init()`, initialize `SigbotState` in `LogManager.init()` for database access.
- **Error Handling**: Gracefully handle deserialization failures, log errors with context, don't stop processing.
- **Performance**: Process asynchronously, use Arc/Mutex for shared state, avoid blocking operations.
