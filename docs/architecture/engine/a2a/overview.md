# A2A Protocol Outlet

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Optional, implemented Workflow control-plane protocol outlet.

The API process uses the Linux Foundation `a2a-lf` and `a2a-server-lf` crates
for Agent Card discovery, JSON-RPC, streaming responses, and protocol errors.
A2A is not a Workflow compute node and owns no business state.

## Capability Boundary

`WorkflowA2aApi` advertises and executes only implemented operations:

- read a tenant-scoped Workflow overview;
- read ordered events for one Workflow;
- idempotently request a Workflow Run start or stop.

The executor delegates to Repositories under
`src/core/src/modules/workflow/storage/`. It contains no SQL, does not access
component-private storage, inject arbitrary nodes, call an exchange, or turn a
model response into an order. Trading remains behind Strategy, Order, and
Wallet boundaries.

## Identity and Lifecycle

- Authguard validates every `/a2a` request. Verified tenant and principal data
  is passed to the official handler only as request-local headers.
- The Agent Card declares an Authguard security scheme. The Helm Gateway
  protects `/a2a` and exposes `/.well-known/agent-card.json` for discovery.
- A2A, HTTP, and MCP share one `ApplicationContext`; shutdown stops the listener
  and awaits its task.
- Configuration lives under `services.api.a2a`; `public_url` is the JSON-RPC
  address advertised by the Agent Card.

## Reliability Rules

- Commands are typed JSON; unknown fields and operations are rejected.
- Repositories enforce tenant, environment, Workflow ownership, and
  idempotency.
- Client errors are sanitized while complete causes remain in structured logs
  and traces.
- A2A task IDs remain protocol-local. Business retries and recovery use
  `run_id`; no additional business correlation ID is introduced.

## Source Map

- Contract: `src/api/a2a/src/a2a.rs`
- Agent Card, Authguard, Workflow executor: `src/api/a2a/src/a2a/workflow.rs`
- API composition: `src/cmd/src/startup/api.rs`
- Helm ports and routes: `deploy/helm/sigbot/templates/`
