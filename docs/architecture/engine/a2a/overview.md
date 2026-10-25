# A2A Adapter

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Optional protocol adapter; the current request handler is partial.

`SigbotA2aServer` exposes explicitly approved SigBot agent capabilities through
the Agent-to-Agent protocol. It is an external interoperability adapter, not a
trading-pipeline stage and not an alternate authorization boundary.

## Boundary

- Publish an Agent Card that describes only implemented, policy-approved
  capabilities and protocol versions.
- Authenticate the caller, resolve tenant/namespace scope, and invoke the same
  application services and policy decisions as APIServer.
- Translate A2A JSON-RPC messages, task lifecycle, errors, and streaming updates
  into internal typed requests without exposing internal message topics.
- Carry correlation, trace, idempotency, and cancellation identifiers across
  the protocol boundary.

A2A cannot access private component databases, inject arbitrary pipeline nodes,
call an exchange directly, or turn an AI response into an order. Trading
actions, if ever exposed, still require a validated strategy/order contract and
the normal hard-risk boundary.

## Extension Points

| Role | Contract | Purpose |
|---|---|---|
| Protocol root | `SigbotA2aServer` | Discovery and JSON-RPC transport |
| Task handler | `ISigbotA2aTaskHandler` | Maps an approved A2A skill to an application service |
| Agent implementation | `ISigbotAgent` | Reuses approved evaluator/research agent behavior |

Protocol types and authentication remain outside domain agents. A handler must
declare input/output schema, timeout, cancellation, idempotency, side-effect
class, and required permission.

## Security and Reliability

- Deny unregistered skills and undeclared side effects.
- Validate JSON-RPC size, schema, content type, task transition, and replay
  token before dispatch.
- Stream backpressure and cancellation propagate to the owned task; abandoned
  client connections do not leak unbounded work.
- Agent cards never advertise placeholder capabilities.
- Errors are protocol-safe and redact prompts, tools, credentials, private
  strategy code, and internal topology.

## Implementation Alignment

Current `src/agents/a2a/` contains server-builder and ADK type integration.
The primary handler remains placeholder behavior and does not yet invoke a
complete agent capability; advertised capabilities are limited. End-to-end
authentication, scoped authorization, durable task state, streaming, and
contract tests remain required before enabling external use.

## Source Map

- A2A adapter: `src/agents/a2a/`
- Shared agent abstractions: `src/agents/`
- Evaluator agents: `src/evaluator/src/executor/adk/`
