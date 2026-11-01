# MCP Protocol Outlet

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Optional, implemented Streamable HTTP Workflow tool outlet.

The API process uses the official `rust-mcp-sdk` server, Streamable HTTP, and
authentication SPI. MCP is not a market-data source, Workflow node, or second
business implementation. The former placeholder MCP client/manager layer is
not retained.

## Tool Boundary

The allowlist contains only `workflow_overview`, `workflow_events`,
`workflow_start`, and `workflow_stop`. Every tool declares JSON Schema and
read-only, idempotent, and open-world annotations, then delegates to Core
Workflow Repositories. Unknown tools produce protocol errors; business failures
produce sanitized `is_error` results.

Tools cannot call an exchange, access component-private storage, or bypass
`Strategy -> Order -> Wallet`. Parameters contain environment, tenant, and the
Workflow ID when needed. Tenant and principal identities come only from the
verified authentication context and cannot be supplied as tool arguments.

## Authguard and Lifecycle

- The MCP SDK extracts `Authorization: Bearer` credentials.
  `AuthguardMcpAuth` supports both Authguard signed-header and gRPC resolvers and
  maps verified tenant/principal data into SDK `AuthInfo`.
- Helm routes `/mcp` through the Authguard Gateway; the health endpoint performs
  no writes.
- The MCP Provider shares the API process `ApplicationContext` and performs a
  bounded graceful shutdown.
- Configuration lives under `services.api.mcp`. Only `streamable_http` is
  supported; obsolete SSE/STDIO compatibility paths are not retained.

## Reliability and Observability

- Credentials are verified on every HTTP request and authentication state stays
  within the current SDK request/session context.
- Core Repositories own start/stop idempotency and `run_id`; MCP creates no
  correlation identifier.
- The official SDK handles request bounds, protocol version, sessions, SSE
  backpressure, and protocol error shapes.
- Logs never contain Bearer credentials, Authguard signatures, or sensitive raw
  business content.

## Source Map

- Contract: `src/api/mcp/src/mcp.rs`
- Authguard adapter: `src/api/mcp/src/mcp/authguard.rs`
- Official SDK server: `src/api/mcp/src/mcp/http.rs`
- Workflow tool registry: `src/api/mcp/src/mcp/workflow.rs`
- API composition: `src/cmd/src/startup/api.rs`
