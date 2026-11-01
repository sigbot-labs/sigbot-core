# MCP 协议出口

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 可选、已实现的 Streamable HTTP Workflow 工具出口。

API 进程使用官方 `rust-mcp-sdk` Server、Streamable HTTP 与 Auth SPI。MCP 不是行情源、
Workflow 节点或第二套业务实现；当前没有保留旧 MCP Client/Manager 占位层。

## 工具边界

Allowlist 仅包含 `workflow_overview`、`workflow_events`、`workflow_start`、`workflow_stop`。
每个 Tool 声明 JSON Schema、只读/幂等/开放世界提示，并调用 Core Workflow Repository。
未知 Tool 返回协议错误；业务失败返回脱敏的 `is_error` 结果。

MCP Tool 禁止直接调用 Exchange、读取组件私有数据库或绕过 `Strategy -> Order -> Wallet`。
Tool 参数只包含 env 和必要的 workflow_id；tenant/principal 来自验证后的认证上下文，
客户端不能在参数中伪造。

## Authguard 与生命周期

- MCP SDK 从 `Authorization: Bearer` 取得凭据；`AuthguardMcpAuth` 同时适配 Authguard
  signed-header 和 gRPC resolver，再把验证后的 tenant/principal 写入 SDK `AuthInfo`。
- `/mcp` 由 Helm Authguard Gateway 保护；Health endpoint 不执行写操作。
- MCP Provider 注册在 API 进程的共享 `ApplicationContext`，关闭时执行限时优雅停止。
- 配置位于 `services.api.mcp`，只支持 `streamable_http`，不保留旧 SSE/STDIO 兼容分支。

## 可靠性与可观测性

- 每个 HTTP 请求重新验证凭据，认证信息只保存在当前 SDK 请求/会话上下文。
- Workflow start/stop 的幂等性和 `run_id` 由 Core Repository 负责；MCP 不生成关联 ID。
- 请求大小、协议版本、Session、SSE 背压与错误格式由官方 SDK 处理。
- 日志不记录 Bearer、Authguard 签名或业务原始敏感内容。

## 源码地图

- 协议契约：`src/api/mcp/src/mcp.rs`
- Authguard Adapter：`src/api/mcp/src/mcp/authguard.rs`
- 官方 SDK Server：`src/api/mcp/src/mcp/http.rs`
- Workflow Tool Registry：`src/api/mcp/src/mcp/workflow.rs`
- API 进程装配：`src/cmd/src/startup/api.rs`
