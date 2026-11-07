# A2A API

[English](overview.md) · [领域契约](../../domain-model_ZH.md)

src/api/a2a/src/a2a.rs 统一负责 IA2aApi、ResourceA2aApi、监听生命周期、Authguard middleware 和协议注册。src/domain/resource.rs 只汇聚领域 skills，并将 ResourceCommand 分发到共用 Core ResourceHandler。

以下能力直接复用官方 a2a/a2a_server：

- 公开 GET/HEAD /.well-known/agent-card.json：agent_card_router/StaticAgentCard，保留官方 CORS、Cache-Control、ETag。
- 认证 JSON-RPC POST /a2a（同时支持 /a2a/）：jsonrpc_router/DefaultRequestHandler。
- 认证 GetExtendedAgentCard：with_extended_agent_card，公开 Card 声明 extendedAgentCard=true。

客户端发送与 Card 的 supportedInterfaces.protocolVersion 一致的 A2A-Version；版本协商和协议错误归 SDK。发布的 public_url 会被校验，其路径即真实注册的 RPC 路径。认证读取 OriginalUri，避免嵌套路由截断路径；尾斜杠别名对应同一规范 Authguard 资源。

配置 authguard.oauth 时，Card 使用标准 OpenID Connect security scheme 公布 discovery_url；否则使用标准 HTTP Bearer 声明网关可信凭据。原先虚构的 HTTP scheme authguard 已移除。认证保留完整资源身份及 allow/deny grants；公开发现不要求登录，伪造内部身份 header 不能调用 RPC。

OAuth/OIDC 发现和发令牌由外部授权服务器拥有，A2A 在 Agent Card 中公布其地址，不在 Agent 内重建 issuer。配置位于 services.platform.api.a2a 和可选 authguard.oauth。真实 TCP 测试覆盖公开缓存/CORS 元数据、发布的接口与版本、未认证拒绝、Extended Agent Card 和路径隔离。
