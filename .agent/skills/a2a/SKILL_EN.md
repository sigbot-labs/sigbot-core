---
name: a2a
description: A2A (Agent-to-Agent) module coding guidelines for exposing Sigbot Agents to external systems via minimal A2A protocol.
---

# A2A Module Guide

## 1. Module Design Overview

- **Module Positioning**: The A2A module provides **minimal A2A protocol implementation** based on `adk-core` and `axum`
- **Core Responsibilities**:
  - **A2A Server**: Wrap Sigbot Agents as HTTP endpoints for external callers
  - **JSON-RPC Handler**: Standard A2A JSON-RPC 2.0 protocol support

- **Architecture Principles**:
  - **High Cohesion Low Coupling**: All A2A logic encapsulated, only depends on `adk-core` and `axum`
  - **Configuration-Driven**: All A2A behaviors are uniformly managed through configuration in `sigbot-core`
  - **Reuse ADK**: Reuse `adk-core` `Agent` trait, avoid redefining agent types

- **Design Boundaries** (Important):
  - ✅ **A2A Server**: Expose Sigbot capabilities to external callers (core scenario)
  - ❌ **A2A Client**: **Not needed** currently, use `sigbot-mcp` MCP Client for calling external services

## 2. Module Directory Structure

```
src/agents/a2a/
├── Cargo.toml
└── src/
    ├── lib.rs           # Module entry, exports A2A types
    └── server.rs        # A2A Server implementation (JSON-RPC + Agent integration)
```

## 3. A2A Protocol Endpoints

A2A Server automatically exposes the following endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/.well-known/agent.json` | GET | Agent Card discovery (standard A2A protocol) |
| `/a2a` | POST | JSON-RPC A2A protocol endpoint |
| `/healthz` | GET | Health check |

## 4. Configuration Structure

**Config Path**: `services.a2a`

```yaml
services:
  a2a:
    enabled: true
    bind-address: "0.0.0.0:8081"
    # Note: A2A protocol is standard, no need for custom capabilities
    # Agent capabilities are automatically exposed via adk_core::Agent trait
```

## 5. Core Components

### 5.1 A2AServerBuilder

**Responsibility**: Build A2A Server, integrates with `adk_core::Agent`

```rust
use sigbot_a2a::A2AServerBuilder;
use adk_core::Agent;
use std::sync::Arc;

// Create Agent (implement adk_core::Agent trait)
let agent: Arc<dyn Agent> = Arc::new(create_sigbot_agent());

// Build and start A2A Server
let server = A2AServerBuilder::new()
    .with_agent_loader(agent)
    .with_bind_address("0.0.0.0:8081")
    .build();

server.start().await?;
```

### 5.2 Reusing adk-core Types

```rust
// Export for external use
pub use adk_core::Agent;  // Agent trait

// Types provided by A2A module:
// - A2AServer, A2AServerBuilder
// - A2AConfig, A2ARequest, A2AResponse
// - AgentCard, AgentCapability
```

## 6. Layered Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Sigbot A2A Module                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                   A2A Server                                │ │
│  │                                                             │ │
│  │  ┌─────────────┐    ┌─────────────┐    ┌────────────────┐ │ │
│  │  │  A2A        │───▶│  Axum       │───▶│ Sigbot Agents  │ │ │
│  │  │  Protocol   │    │  HTTP       │    │ (adk_core::    │ │ │
│  │  │  (JSON-RPC) │    │  Router     │    │  Agent)        │ │ │
│  │  └─────────────┘    └─────────────┘    └────────────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Standard Endpoints:                                             │
│  - GET  /.well-known/agent.json  (Agent Card)                   │
│  - POST /a2a                     (JSON-RPC)                     │
│  - GET  /healthz                   (Health Check)               │
└─────────────────────────────────────────────────────────────────┘
```

## 7. Usage Scenarios

### 7.1 Expose Evaluator Agent to sigbot-researcher

```rust
use sigbot_a2a::A2AServerBuilder;
use adk_core::Agent;
use std::sync::Arc;

async fn start_evaluator_a2a_server() -> Result<()> {
    // Create Evaluator Agent (implement adk_core::Agent trait)
    let evaluator_agent: Arc<dyn Agent> = Arc::new(create_evaluator_agent());

    // Build and start server
    let server = A2AServerBuilder::new()
        .with_agent_loader(evaluator_agent)
        .with_bind_address("0.0.0.0:8081")
        .with_agent_name("evaluator-agent")
        .with_agent_description("Strategy evaluation agent")
        .build();

    server.start().await?;
    Ok(())
}
```

### 7.2 External System Invocation (curl example)

```bash
# 1. Get Agent Card (discover agent capabilities)
curl http://localhost:8081/.well-known/agent.json | jq

# Response example:
# {
#   "name": "evaluator-agent",
#   "description": "Strategy evaluation agent",
#   "capabilities": [...]
# }

# 2. Send A2A message (JSON-RPC format)
curl -X POST http://localhost:8081/a2a \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "message/send",
    "params": {
      "message": {
        "role": "user",
        "messageId": "msg-1",
        "parts": [{"text": "Evaluate strategy-123 with 24h data"}]
      }
    },
    "id": 1
  }'
```

### 7.3 Using RemoteA2aAgent to Call Other A2A Services (Optional)

```rust
use adk_core::Agent;

// Create remote agent reference (for calling other A2A servers)
// Note: In current implementation, RemoteA2aAgent needs to implement adk_core::Agent trait
// by calling remote A2A services via HTTP client
```

## 8. Coding Guidelines

### 8.1 Reusing adk-core Types

**Do not** define custom A2A protocol structures, use `adk-core`:

```rust
// ❌ Wrong: Do not define custom
pub struct MyA2ARequest { ... }
pub struct MyA2AResponse { ... }

// ✅ Correct: Use standard types from A2A module
// - A2ARequest, A2AResponse provided by sigbot-a2a
// - Agent trait provided by adk-core
```

### 8.2 Implementing ADK Agent

```rust
use adk_core::{Agent, EventStream, InvocationContext, Result as AdkResult};
use std::sync::Arc;

pub struct SigbotEvaluatorAgent;

#[async_trait]
impl Agent for SigbotEvaluatorAgent {
    async fn run(&self, ctx: Arc<dyn InvocationContext>) -> AdkResult<EventStream> {
        // Implement Agent logic
        // A2A requests are routed here via axum
        Ok(EventStream::default())
    }
}
```

### 8.3 Dependency Management

```toml
[dependencies]
# Core dependencies
adk-core = "0.2.0"

# Web framework
axum = "0.8"
tokio = "1.43"

# Serialization
serde = "1.0"
serde_json = "1.0"
```

## 9. Testing Guide

### 9.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use adk_core::{Agent, EventStream, InvocationContext, Result as AdkResult};

    struct TestAgent;

    #[async_trait]
    impl Agent for TestAgent {
        async fn run(&self, _ctx: Arc<dyn InvocationContext>) -> AdkResult<EventStream> {
            Ok(EventStream::default())
        }
    }

    #[tokio::test]
    async fn test_a2a_server_builder() {
        let agent: Arc<dyn Agent> = Arc::new(TestAgent);

        let server = A2AServerBuilder::new()
            .with_agent_loader(agent)
            .with_bind_address("0.0.0.0:0")  // Use port 0 for testing
            .build();

        assert_eq!(server.config.bind_address, "0.0.0.0:0");
    }
}
```

### 9.2 Integration Tests

```rust
use axum::body::Body;
use http_body_util::BodyExt;
use serde_json::json;

#[tokio::test]
async fn test_agent_card() {
    let server = create_test_server();
    let router = server.build_router();

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/.well-known/agent.json")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let agent_card: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(agent_card.get("name").is_some());
}
```

## 10. Related File Paths

- `src/agents/a2a/**/*.rs` - A2A module main code
- `src/core/src/config/config.rs` - Config definition (`A2AProperties`)
- `src/evaluator/src/executor/adk/agents/` - ADK Agent implementation examples

## 11. Interactions with Other Modules

| Module | Interaction Type | Description |
|--------|------------------|-------------|
| `sigbot-core` | Config dependency | Reads `services.a2a` configuration |
| `sigbot-evaluator` | Agent exposure | Evaluator Agents exposed via A2A to external |
| `sigbot-mcp` | Complementary | MCP for Sigbot **calling external** services, A2A for **exposing to external** calls |
| `adk-core` | Core dependency | Agent trait definition |

## 12. Best Practices

1. **Thin Wrapper Principle**: Do not define custom A2A protocol types, reuse `adk-core` and `axum`
2. **Standard Endpoints**: Keep `/.well-known/agent.json`, `/a2a`, `/healthz` endpoints unchanged
3. **JSON-RPC Format**: A2A protocol uses standard JSON-RPC 2.0 format
4. **Agent Card Discovery**: Ensure Agent Card correctly describes Agent capabilities
5. **Agent Integration**: Integrate adk-core agents via `Arc<dyn Agent>`

## 13. Comparison with MCP Module

| Feature | MCP | A2A |
|---------|-----|-----|
| **Direction** | Sigbot **calls external** services | **Exposes to external** calls to Sigbot |
| **Protocol** | Model Context Protocol | A2A Protocol (JSON-RPC) |
| **Dependencies** | `rust-mcp-sdk` | `adk-core`, `axum` |
| **Configuration** | `services.api.mcp`, `services.evaluator.mcp_servers` | `services.a2a` |
| **Typical Scenario** | Evaluator calls Binance MCP for data | sigbot-researcher calls Evaluator Agent |
| **Endpoints** | None (MCP is client protocol) | `/.well-known/agent.json`, `/a2a`, `/healthz` |

## 14. A2A Protocol Examples

### JSON-RPC Request Format

```json
{
  "jsonrpc": "2.0",
  "method": "message/send",
  "params": {
    "message": {
      "role": "user",
      "messageId": "msg-123",
      "parts": [
        {"type": "text", "text": "Evaluate strategy-123"}
      ]
    }
  },
  "id": 1
}
```

### JSON-RPC Response Format

```json
{
  "jsonrpc": "2.0",
  "result": {
    "message": {
      "role": "agent",
      "messageId": "msg-456",
      "parts": [
        {"type": "text", "text": "Evaluation complete..."}
      ]
    }
  },
  "id": 1
}
```
