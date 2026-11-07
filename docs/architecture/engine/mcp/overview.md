# MCP API

[中文](overview_ZH.md) · [Domain contracts](../../domain-model_ZH.md)

The official rust-mcp-sdk serves authenticated streamable HTTP. ResourceMcpRegistry advertises 50 tools: five CRUD operations for each of the ten domains.

The IMcpApi contract, HttpMcpApi lifecycle and protocol endpoint composition live in src/api/mcp/src/mcp.rs. It uses the SDK RemoteAuthProvider, AuthorizationServerMetadata and OauthProtectedResourceMetadata directly. The SDK owns discovery serialization, CORS, method validation and the 401 WWW-Authenticate challenge; Sigbot does not implement a second authorization server.

For public_url https://app.example/mcp, the anonymous discovery endpoints are:

- GET/HEAD/OPTIONS /.well-known/oauth-protected-resource/mcp (RFC 9728).
- GET/HEAD/OPTIONS /.well-known/oauth-authorization-server (the SDK's copy of the configured authority's real discovery metadata).

The resource document advertises the canonical public_url, authority issuer, supported scopes and Bearer header usage. The challenge points to that canonical metadata URL, never to the bind address or an untrusted Host header. A prefixed resource path produces the corresponding SDK path-scoped well-known URL.

Configure services.platform.api.mcp.public_url and authguard.oauth with issuer, discovery_url and optional scopes. discovery_url must be the authority's real OIDC discovery document. Startup fetches it through the SDK with a bounded timeout and validates issuer, HTTPS and supported scopes before binding. Missing/invalid configuration fails startup. HTTP is allowed only for loopback development. Values are non-secret; credential material remains in Secrets.

The authority owns authorize/token/registration/revocation and OIDC endpoints; their real URLs are preserved in discovery. Sigbot neither invents these addresses nor issues credentials. End-user OAuth/OIDC verification remains at the Authguard/Gateway boundary. The backend SDK token verifier in src/api/mcp/src/auth/authguard.rs accepts the trusted Authguard credential handed off by that boundary, preserving tenant and allow/deny grants. The shared Core bridge now maps signed-header and gRPC opaque scope-token modes to the appropriate official SDK headers. A gateway must hand the MCP credential callback the verified credential as Bearer; raw end-user JWTs are not treated as signed Authguard contexts.

Helm exposes the discovery paths through a separate public HTTPRoute while /mcp remains protected. The configured external hostname or explicit protocols.mcp.publicUrl supplies the advertised URL. Real TCP tests cover anonymous discovery, 405, 401 discovery challenge, authenticated SDK initialization and issuer/URL rejection.
