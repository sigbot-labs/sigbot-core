# MCP Adapter

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Optional protocol adapter with partial server/client scaffolding.

MCP has two explicit roles in SigBot:

- `SigbotMcpServer` exposes a small allowlisted set of application capabilities
  through the same authentication, authorization, and domain services as
  APIServer.
- `SigbotMcpClientManager` gives Evaluator runs access to approved external
  tools for supporting evidence.

MCP is not a trading-pipeline stage, a control-plane database client, or the
canonical source of live market data.

## Inbound Server Boundary

Every `ISigbotMcpTool` declares its input/output schema, required permission,
tenant/namespace scope, timeout, side-effect class, and audit policy. Transport
code validates protocol requests and delegates to application services. It
does not duplicate business logic or expose internal repositories.

Read-only discovery and analysis tools are the default. A tool cannot call an
exchange or bypass `Strategy Runner -> Order Manager`; any future mutating tool
requires explicit idempotency and the same hard-risk contract as the public API.

## Evaluator Client Boundary

Evaluator selects tools from a run-specific allowlist. Each call is bounded by
timeout, response size, concurrency, data classification, and budget. Tool
results are untrusted evidence: Loader normalizes them, Auditor checks them, and
the final evaluation records server/tool identity and version.

During Backtest `RECOMPUTE`, tools receive virtual-time policy and cannot fetch
information published after the simulated decision. Canonical `SigbotFeedEvent`
data always comes from Datafeed or historical replay.

## Extension Points

| Role | Contract | Purpose |
|---|---|---|
| Inbound root | `SigbotMcpServer` | Protocol transport and tool registry |
| Tool | `ISigbotMcpTool` | One approved application capability |
| Outbound client | `ISigbotMcpClient` | One configured external MCP server |
| Client manager | `SigbotMcpClientManager` | Lifecycle, policy, routing, and health across clients |

## Security and Reliability

- Authenticate every remote connection and bind it to request-local tenant and
  principal scope.
- Prevent SSRF with endpoint allowlists, network policy, TLS verification,
  redirect limits, and response-size limits.
- Secrets are referenced by ID and never included in prompts, events, status,
  or logs.
- Tool errors are isolated and typed; one unavailable server cannot block an
  entire evaluation beyond its declared deadline.
- Audit records include caller, tool, scope, input hash, outcome, latency, and
  correlation ID without storing sensitive raw content by default.

## Implementation Alignment

Current `src/agents/mcp/` contains server, client, manager, configuration, and
transport scaffolding. Several server tools and connection-management features
are placeholders; streamable HTTP, authentication, retry, health, and complete
Evaluator integration require implementation evidence and tests before being
claimed as supported.

## Source Map

- MCP server/client adapters: `src/agents/mcp/`
- Evaluator integration: `src/evaluator/`
- APIServer composition: `src/core/`, `src/cmd/`
