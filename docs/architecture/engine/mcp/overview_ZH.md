# MCP API

[English](overview.md) · [领域契约](../../domain-model_ZH.md)

官方 rust-mcp-sdk 提供认证的 streamable HTTP。ResourceMcpRegistry 汇聚十领域共 50 个 CRUD 工具。

IMcpApi、HttpMcpApi 生命周期与协议端点统一注册在 src/api/mcp/src/mcp.rs，直接复用 SDK 的 RemoteAuthProvider、AuthorizationServerMetadata 和 OauthProtectedResourceMetadata。发现 JSON、CORS、方法校验及 401 WWW-Authenticate challenge 都由 SDK 处理。

当 public_url 为 https://app.example/mcp 时，匿名发现入口为：

- GET/HEAD/OPTIONS /.well-known/oauth-protected-resource/mcp：RFC 9728 资源元数据。
- GET/HEAD/OPTIONS /.well-known/oauth-authorization-server：SDK 提供的真实授权服务器元数据副本。

资源文档包含规范化外部 public_url、真实 issuer、支持的 scopes 和 Bearer header 用法。401 的 resource_metadata 指向外部规范地址，不使用监听地址或请求方 Host。带前缀的资源 URL 使用 SDK 生成的对应路径级 well-known 地址。

必须配置 services.platform.api.mcp.public_url 和 authguard.oauth 的 issuer、discovery_url（真实 OIDC 发现文档）、可选 scopes。启动通过 SDK 拉取元数据，带超时并校验 issuer、HTTPS 和 scope；缺失或非法配置直接启动失败。仅本机 loopback 开发地址允许 HTTP。这里不保存机密值。

authorize/token/registration/revocation 与 OIDC 端点由授权服务器拥有，发现文档保留它们的真实地址；Sigbot 不伪造或自行实现发令牌接口。外部用户 OAuth/OIDC 验证仍归 Authguard/Gateway；后端 SDK 的 src/api/mcp/src/auth/authguard.rs 校验网关交付的可信凭据，保留 tenant 和 allow/deny grants。Core bridge 已补齐 signed_header 和 gRPC opaque scope-token 的官方 header 选择。Gateway 必须以 Bearer 向 MCP SDK 凭据回调交付已验证凭据，不能把原始用户 JWT 当成 Authguard 签名上下文。

Helm 使用独立公共 HTTPRoute 暴露发现端点，/mcp 保持受保护；外部域名或 protocols.mcp.publicUrl 决定发布地址。真实 TCP 测试覆盖匿名发现、405、401 challenge、认证后 SDK 初始化、issuer/URL 拒绝。
