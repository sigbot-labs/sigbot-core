# MCP Adapter

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 可选协议 Adapter；当前已有部分 Server/Client 骨架。

MCP 在 SigBot 中只有两个显式角色：

- `SigbotMcpServer` 通过与 APIServer 相同的 Authentication、Authorization 与 Domain
  Service，暴露少量 Allowlist Application Capability。
- `SigbotMcpClientManager` 为 Evaluator Run 提供获准外部 Tool，作为辅助证据。

MCP 不是 Trading Pipeline Stage、控制面数据库 Client 或规范 Live Market Data Source。

## Inbound Server 边界

每个 `ISigbotMcpTool` 声明 Input/Output Schema、Required Permission、Tenant/Namespace
Scope、Timeout、Side-effect Class 与 Audit Policy。Transport Code 校验协议 Request 后
委托 Application Service，不能复制 Business Logic 或暴露内部 Repository。

默认只提供 Read-only Discovery/Analysis Tool。Tool 不能调用 Exchange 或绕过
`Strategy Runner -> Order Manager`；未来 Mutating Tool 必须具有显式 Idempotency，并
遵循与公共 API 相同的硬风险契约。

## Evaluator Client 边界

Evaluator 从 Run-specific Allowlist 选择 Tool。每次调用受 Timeout、Response Size、
Concurrency、Data Classification 与 Budget 限制。Tool Result 是不可信证据：Loader
负责规范化，Auditor 负责校验，最终 Evaluation 记录 Server/Tool Identity 与 Version。

Backtest `RECOMPUTE` 中，Tool 接收 Virtual-time Policy，禁止获取模拟 Decision 之后
发布的信息。规范 `SigbotFeedEvent` 始终来自 Datafeed 或 Historical Replay。

## 扩展点

| 角色 | 契约 | 用途 |
|---|---|---|
| Inbound Root | `SigbotMcpServer` | Protocol Transport 与 Tool Registry |
| Tool | `ISigbotMcpTool` | 一个获准 Application Capability |
| Outbound Client | `ISigbotMcpClient` | 一个已配置 External MCP Server |
| Client Manager | `SigbotMcpClientManager` | 多 Client 的 Lifecycle、Policy、Routing 与 Health |

## 安全与可靠性

- 认证每个 Remote Connection，并绑定 Request-local Tenant 与 Principal Scope。
- 使用 Endpoint Allowlist、Network Policy、TLS Verification、Redirect Limit 与
  Response-size Limit 防止 SSRF。
- Secret 只通过 ID Reference 使用，禁止进入 Prompt、Event、Status 或 Log。
- Tool Error 必须隔离且类型化；一个 Server 不可用不能让整个 Evaluation 超过声明
  Deadline。
- Audit Record 包含 Caller、Tool、Scope、Input Hash、Outcome、Latency 与 Correlation
  ID，默认不保存 Sensitive Raw Content。

## 实现对齐

当前 `src/agents/mcp/` 已有 Server、Client、Manager、Configuration 与 Transport
Scaffolding。多个 Server Tool 与 Connection-management Feature 仍是 Placeholder；
Streamable HTTP、Authentication、Retry、Health 与完整 Evaluator Integration 必须有
实现和测试证据后才能宣称支持。

## 源码地图

- MCP Server/Client Adapter：`src/agents/mcp/`
- Evaluator Integration：`src/evaluator/`
- APIServer Composition：`src/core/`、`src/cmd/`
