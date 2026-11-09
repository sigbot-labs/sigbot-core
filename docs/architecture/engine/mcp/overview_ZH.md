# 通过 mcpfather 暴露 MCP

[English](overview.md) · [mcpfather](https://github.com/flowgent-labs/mcpfather)

Sigbot 维护 HTTP/A2A API，由独立部署的 mcpfather 生成服务把 HTTP OpenAPI 转换为 MCP。这是 Sigbot 对 mcpfather 的真实接入案例；活动 Rust workspace 不再包含 MCP server，也不再注册 sigbot mcp 子命令。

旧实现、当前未提交的 MCP Starter 修改、认证适配器、测试、文档和集成快照保存在 .archived/20261109382032_builtin_mcp_server_replaced_by_mcpfather/。目录内 SHA256SUMS 可核验搬移前后的文件一致性。沿用现有 .gitignore，归档当前仅保存在本地。

生成时，先通过获授权的 HTTP 入口取得 Sigbot 实际的 OpenAPI JSON。示例配置路径为 /swagger-ui/openapi.json，须结合 server.context_path 和 Gateway 的路由/权限；不要另写一份 API schema。然后调用相邻项目的 bin/mcpfather，以 --input 指定 schema、--output 指定独立的 sigbot-mcp 项目，使用生成器默认模式。当前本地生成器的可选 --validation 严格校验不接受 OpenAPI 3.1 中合法的 propertyNames；这是校验器兼容性限制，不改写 schema 掩盖该字段，也不宣称严格校验通过。完整命令见英文说明。

Sigbot schema 的十领域 50 个 CRUD 操作各自有 operationId；生成器负责 MCP tools、传输、OAuth 发现、认证和部署。生成代码放独立项目，不再塞回 Sigbot workspace。

分别配置 mcpfather 的前端 OIDC 与后端上游凭据，通过 Authguard/Gateway 以对应租户和服务主体调用 Sigbot。保留 env/domain grants 和 Trace Context，不透传 AI 客户端令牌，也不在 schema 中放机密值或绕过租户隔离。

HTTP crate 的可选集成测试会导出真实 SwaggerHttpApi 文档并调用本地 mcpfather 生成器；运行方式见英文说明。该验证不等同于真实 OIDC 登录或交易 E2E。Datafeed 的外部 MCP 客户端和外部源 fixture 继续保留。
