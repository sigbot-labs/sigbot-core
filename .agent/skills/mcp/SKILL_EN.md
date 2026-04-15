---
name: mcp
description: MCP (Model Context Protocol) module coding guidelines for Sigbot integration with external MCP services.
---

# MCP Module Guide

## 1. Module Design Overview

- **Module Positioning**: The MCP module is the protocol layer for Sigbot communication with external MCP services, providing a **highly cohesive, loosely coupled** architecture.
- **Core Responsibilities**:
  - **MCP Server**: Wraps Sigbot RESTful API as MCP protocol for external clients
  - **MCP Client**: Enables modules like Evaluator to call external MCP services (e.g., Binance MCP, Bitget MCP) for market data

- **Architecture Principles**:
  - **Layered Design**: MCP Protocol Layer → API Adapter Layer → Sigbot Core RESTful API
  - **Configuration-Driven**: All MCP behaviors are uniformly managed through configuration in `sigbot-core`
  - **Protocol-Agnostic**: Supports SSE, STDIO, Streamable HTTP and other transport protocols

## 2. Module Directory Structure

```
src/agents/mcp/
├── Cargo.toml
└── src/
    ├── lib.rs                 # Module entry with architecture docs
    ├── config.rs              # Config types (re-export from sigbot-core)
    ├── error.rs               # Error type definitions
    ├── server/
    │   ├── mod.rs
    │   └── mcp_server.rs      # MCP Server (wraps RESTful API)
    └── client/
        ├── mod.rs
        └── mcp_client.rs      # MCP Client (calls external services)
```

## 3. Configuration Structure

### 3.1 API MCP Server Configuration

**Config Path**: `services.api.mcp`

```yaml
services:
  api:
    mcp:
      enabled: true                    # Enable MCP Server
      name: "sigbot-api-mcp-server"    # MCP Server name
      transport: "sse"                 # Transport: sse, stdio, streamable-http
      bind-address: "0.0.0.0:8080"     # Bind address
      capabilities:                    # Exposed capabilities
        - "trading"
        - "market-data"
        - "backtest"
        - "strategy"
      auth-enabled: true               # Enable authentication
```

### 3.2 Evaluator MCP Clients Configuration

**Config Path**: `services.evaluator.mcp_servers`

```yaml
services:
  evaluator:
    mcp-servers:
      - name: "binance-mcp"
        url: "https://binance-mcp.example.com/sse"
        transport: "sse"
        api-key: "${BINANCE_API_KEY}"
        api-secret: "${BINANCE_API_SECRET}"
        enabled-tools:
          - "get_kline"
          - "get_ticker"
          - "get_orderbook"
        timeout-secs: 30
      - name: "bitget-mcp"
        url: "https://bitget-mcp.example.com/sse"
        transport: "sse"
        api-key: "${BITGET_API_KEY}"
        api-secret: "${BITGET_API_SECRET}"
        enabled-tools:
          - "get_market_data"
        timeout-secs: 30
```

## 4. Core Components

### 4.1 MCP Server (`SigbotMcpServer`)

**Responsibility**: Wrap Sigbot RESTful API as MCP protocol

```rust
use sigbot_mcp::server::SigbotMcpServer;
use sigbot_core::config::config::get_config;

let config = get_config();
let mcp_config = &config.services.api.mcp;

let mut server = SigbotMcpServer::new(mcp_config.clone());
server.init().await?;
server.start().await?;
```

**Layered Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│                      MCP Server                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ MCP Protocol│───▶│ API Adapter │───▶│ Sigbot RESTful  │  │
│  │ (SSE/STDIO) │    │ (Translation)│    │ API (Axum)      │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 MCP Client (`SigbotMcpClient`)

**Responsibility**: Connect to external MCP services and call tools

```rust
use sigbot_mcp::client::{SigbotMcpClient, McpClientManager};
use sigbot_core::config::config::get_config;

let config = get_config();
let manager = McpClientManager::new();

// Register all configured MCP servers
for mcp_server_config in &config.services.evaluator.mcp_servers {
    manager.register(mcp_server_config.clone()).await?;
}

// Call external MCP service
if let Some(client) = manager.get("binance-mcp").await {
    let kline_data = client.call_tool("get_klines", json!({
        "symbol": "BTCUSDT",
        "interval": "1h"
    })).await?;
}
```

### 4.3 MCP Client Manager (`McpClientManager`)

**Responsibility**: Manage multiple MCP Client connection pools

- Supports dynamic registration/deregistration of MCP clients
- Supports retrieving specific clients by name
- Supports unified shutdown of all connections

## 5. Usage Scenarios

### 5.1 Evaluator Calls External MCP for Market Data

When Evaluator needs to adjust strategy hyperparameters based on market data:

```rust
// In Evaluator Agent
use sigbot_mcp::client::McpClientManager;

async fn analyze_market_conditions(
    manager: &McpClientManager,
    symbol: &str,
) -> Result<MarketAnalysis> {
    // Get K-line data from Binance MCP
    let binance = manager.get("binance-mcp").await
        .ok_or("Binance MCP client not found")?;

    let klines = binance.call_tool("get_klines", json!({
        "symbol": symbol,
        "interval": "1h",
        "limit": 24
    })).await?;

    // Calculate support/resistance levels
    let support = calculate_support(&klines);
    let resistance = calculate_resistance(&klines);

    // Identify market regime (ranging/trending)
    let market_regime = identify_market_regime(&klines);

    Ok(MarketAnalysis {
        symbol: symbol.to_string(),
        support,
        resistance,
        market_regime,
    })
}
```

### 5.2 Expose Sigbot Capabilities to External Clients

Expose Sigbot capabilities to Claude, AI Agents, etc. via MCP Server:

```rust
// When API Server starts
if config.services.api.mcp.enabled {
    let mut mcp_server = SigbotMcpServer::new(config.services.api.mcp.clone());
    mcp_server.init().await?;

    // Register Sigbot Tools
    mcp_server.register_tool("get_strategy_performance", ...);
    mcp_server.register_tool("get_market_data", ...);
    mcp_server.register_tool("create_backtest", ...);

    // Start MCP Server
    tokio::spawn(async move {
        mcp_server.start().await?;
    });
}
```

## 6. Coding Guidelines

### 6.1 Dependency Management

- **No Circular Dependencies**: `sigbot-mcp` depends on `sigbot-core`, but `sigbot-core` should not depend on `sigbot-mcp`
- **Unified Configuration**: All config types are defined in `sigbot-core/src/config/config.rs`
- **Type Exports**: `sigbot-mcp` exports config types via `pub use` for external use

### 6.2 Error Handling

```rust
use sigbot_mcp::error::{McpError, Result};

async fn call_external_mcp(client: &SigbotMcpClient) -> Result<Value> {
    client.call_tool("get_price", json!({ "symbol": "BTCUSDT" }))
        .await
        .map_err(|e| {
            error!("MCP tool call failed: {}", e);
            e
        })
}
```

### 6.3 Connection Management

- **On-Demand Connection**: MCP Client establishes connection on first call
- **Timeout Control**: All MCP calls should have configured timeouts (default 30 seconds)
- **Reconnection**: Support automatic reconnection on disconnect (configurable)

### 6.4 Authentication & Security

- **Auth Config**: Enable/disable MCP auth via `auth-enabled`
- **Key Management**: API keys/secrets injected via environment variables, never hardcoded
- **Log Sanitization**: Never log sensitive information (API Secrets, etc.)

## 7. Testing Guidelines

### 7.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = EvaluatorMcpServerConfig {
            name: "test-mcp".to_string(),
            url: "http://localhost:8080/sse".to_string(),
            transport: "sse".to_string(),
            auth_token: None,
            api_key: None,
            api_secret: None,
            enabled_tools: vec![],
            timeout_secs: 30,
        };
        let client = SigbotMcpClient::new(config);
        assert_eq!(client.name(), "test-mcp");
    }
}
```

### 7.2 Integration Tests

- Test Client behavior with Mock MCP Server
- Test disconnect/reconnection scenarios
- Test timeout handling

## 8. Related File Paths

- `src/agents/mcp/**/*.rs` - MCP module main code
- `src/core/src/config/config.rs` - Config definitions (`ApiMcpProperties`, `EvaluatorMcpServerConfig`)
- `src/evaluator/src/executor/adk/tools/mcp_binance_tool.rs` - Evaluator integration example

## 9. Module Interactions

| Module | Interaction | Description |
|--------|-------------|-------------|
| `sigbot-core` | Config dependency | Reads `services.api.mcp` and `services.evaluator.mcp_servers` |
| `sigbot-evaluator` | Client calls | Evaluator calls external MCP services via `McpClientManager` |
| `sigbot-api` | Server wrapping | MCP Server wraps RESTful API for external exposure |
| `sigbot-a2a` | Protocol complement | A2A for Agent-to-Agent communication, MCP for external services |

## 10. Best Practices

1. **Configuration First**: All MCP behaviors controlled by configuration, no hardcoding
2. **Clear Layering**: Separate MCP Protocol Layer from Business Logic Layer
3. **Error Isolation**: MCP call failures should not affect core business logic
4. **Performance Considerations**: Set reasonable timeouts for MCP calls to avoid blocking main flow
5. **Observability**: Log MCP call metrics and logs for troubleshooting
