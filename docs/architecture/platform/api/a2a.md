# A2A API

[中文](a2a_CN.md) · [Domain contracts](api_CN.md#domain-contracts)

src/api/a2a/src/a2a.rs owns IA2aApi, ResourceA2aApi, listener lifecycle, Authguard middleware and all protocol registration. src/domain/resource.rs only aggregates domain skills and dispatches ResourceCommand through the shared Core ResourceHandler.

The official a2a/a2a_server library provides:

- Public GET/HEAD /.well-known/agent-card.json through agent_card_router/StaticAgentCard, including its CORS, Cache-Control and ETag behavior.
- Authenticated JSON-RPC POST /a2a (also /a2a/) through jsonrpc_router/DefaultRequestHandler.
- Authenticated GetExtendedAgentCard through with_extended_agent_card; the public card declares extendedAgentCard=true.

Clients send A2A-Version matching supportedInterfaces.protocolVersion from the card. The SDK handles version negotiation and protocol errors. Published public_url is validated and its path is the actual registered RPC path, including prefixed deployments. Authentication verifies OriginalUri rather than the stripped nested path; both trailing-slash variants use the same canonical Authguard resource.

The card advertises the standard OpenID Connect security scheme and authguard.oauth.discovery_url when configured. Otherwise it advertises standard HTTP Bearer for trusted Authguard credentials; the invented HTTP scheme authguard is removed. Authenticated requests preserve the complete verified resource identity and grants. Discovery stays public; forged internal identity headers do not authorize RPC calls.

The external authority owns OAuth/OIDC discovery and token issuance; an A2A agent publishes their location in its Agent Card instead of hosting a second issuer. Configuration is services.platform.api.a2a plus optional authguard.oauth. Real TCP tests verify public caching/CORS metadata, advertised interface/version, unauthorized requests, authenticated extended cards and resource-path isolation.
