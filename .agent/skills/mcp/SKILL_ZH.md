---
name: mcp
description: MCP (Model Context Protocol) 模块编码指南，负责 Sigbot 与外部 MCP 服务的协议适配与集成。
---

# MCP Module Guide

## 1. 模块设计核心

- **模块定位**：MCP 模块是 Sigbot 与外部 MCP 服务通信的协议层，提供**高内聚低耦合**的架构设计
- **核心职责**：
  - **MCP Server**：将 Sigbot RESTful API 包装为 MCP 协议暴露给外部客户端
  - **MCP Client**：使 Evaluator 等模块能调用外部 MCP 服务（如 Binance MCP、Bitget MCP）获取市场数据

- **架构原则**：
  - **分层设计**：MCP Protocol Layer → API Adapter Layer → Sigbot Core RESTful API
  - **配置驱动**：所有 MCP 行为通过 `sigbot-core` 中的配置统一管理
  - **协议无关**：支持 SSE、STDIO、Streamable HTTP 等多种传输协议

## 2. 模块目录结构

```
src/agents/mcp/
├── Cargo.toml
└── src/
    ├── lib.rs                 # 模块入口，含架构文档
    ├── config.rs              # 配置类型 (re-export from sigbot-core)
    ├── error.rs               # 错误类型定义
    ├── server/
    │   ├── mod.rs
    │   └── mcp_server.rs      # MCP Server 实现 (包装 RESTful API)
    └── client/
        ├── mod.rs
        └── mcp_client.rs      # MCP Client 实现 (调用外部服务)
```

## 3. 配置结构

### 3.1 API MCP Server 配置

**配置路径**：`services.api.mcp`

```yaml
services:
  api:
    mcp:
      enabled: true                    # 是否启用 MCP Server
      name: "sigbot-api-mcp-server"    # MCP Server 名称
      transport: "sse"                 # 传输协议：sse, stdio, streamable-http
      bind-address: "0.0.0.0:8080"     # 监听地址
      capabilities:                    # 暴露的能力列表
        - "trading"
        - "market-data"
        - "backtest"
        - "strategy"
      auth-enabled: true               # 是否启用认证
```

### 3.2 Evaluator MCP Clients 配置

**配置路径**：`services.evaluator.mcp_servers`

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

## 4. 核心组件

### 4.1 MCP Server (`SigbotMcpServer`)

**职责**：将 Sigbot RESTful API 包装为 MCP 协议

```rust
use sigbot_mcp::server::SigbotMcpServer;
use sigbot_core::config::config::get_config;

let config = get_config();
let mcp_config = &config.services.api.mcp;

let mut server = SigbotMcpServer::new(mcp_config.clone());
server.init().await?;
server.start().await?;
```

**分层架构**：
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

**职责**：连接外部 MCP 服务并调用工具

```rust
use sigbot_mcp::client::{SigbotMcpClient, McpClientManager};
use sigbot_core::config::config::get_config;

let config = get_config();
let manager = McpClientManager::new();

// 注册所有配置的 MCP 服务器
for mcp_server_config in &config.services.evaluator.mcp_servers {
    manager.register(mcp_server_config.clone()).await?;
}

// 调用外部 MCP 服务
if let Some(client) = manager.get("binance-mcp").await {
    let kline_data = client.call_tool("get_kline", json!({
        "symbol": "BTCUSDT",
        "interval": "1h"
    })).await?;
}
```

### 4.3 MCP Client Manager (`McpClientManager`)

**职责**：管理多个 MCP Client 连接池

- 支持动态注册/注销 MCP 客户端
- 支持按名称获取特定客户端
- 支持统一关闭所有连接

## 5. 使用场景

### 5.1 Evaluator 调用外部 MCP 获取市场数据

当 Evaluator 需要根据市场数据动态调整策略超参时：

```rust
// 在 Evaluator Agent 中
use sigbot_mcp::client::McpClientManager;

async fn analyze_market_conditions(
    manager: &McpClientManager,
    symbol: &str,
) -> Result<MarketAnalysis> {
    // 从 Binance MCP 获取 K 线数据
    let binance = manager.get("binance-mcp").await
        .ok_or("Binance MCP client not found")?;

    let klines = binance.call_tool("get_klines", json!({
        "symbol": symbol,
        "interval": "1h",
        "limit": 24
    })).await?;

    // 计算支撑位/阻力位
    let support = calculate_support(&klines);
    let resistance = calculate_resistance(&klines);

    // 判断市场状态（震荡/趋势）
    let market_regime = identify_market_regime(&klines);

    Ok(MarketAnalysis {
        symbol: symbol.to_string(),
        support,
        resistance,
        market_regime,
    })
}
```

### 5.2 暴露 Sigbot 能力给外部客户端

通过 MCP Server 将 Sigbot 能力暴露给 Claude、AI Agent 等外部客户端：

```rust
// 在 API Server 启动时
if config.services.api.mcp.enabled {
    let mut mcp_server = SigbotMcpServer::new(config.services.api.mcp.clone());
    mcp_server.init().await?;

    // 注册 Sigbot Tools
    mcp_server.register_tool("get_strategy_performance", ...);
    mcp_server.register_tool("get_market_data", ...);
    mcp_server.register_tool("create_backtest", ...);

    // 启动 MCP Server
    tokio::spawn(async move {
        mcp_server.start().await?;
    });
}
```

## 6. 编码指南

### 6.1 依赖管理

- **禁止循环依赖**：`sigbot-mcp` 依赖 `sigbot-core`，但 `sigbot-core` 不应依赖 `sigbot-mcp`
- **配置统一**：所有配置类型定义在 `sigbot-core/src/config/config.rs` 中
- **类型导出**：`sigbot-mcp` 通过 `pub use` 导出配置类型供外部使用

### 6.2 错误处理

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

### 6.3 连接管理

- **按需连接**：MCP Client 在首次调用时建立连接
- **超时控制**：所有 MCP 调用应配置超时时间（默认 30 秒）
- **重连机制**：连接断开时应支持自动重连（可配置）

### 6.4 认证与安全

- **认证配置**：通过 `auth-enabled` 配置是否启用 MCP 认证
- **密钥管理**：API 密钥/密钥通过环境变量注入，禁止硬编码
- **日志脱敏**：日志中禁止输出敏感信息（API Secret 等）

## 7. 测试指南

### 7.1 单元测试

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

### 7.2 集成测试

- 使用 Mock MCP Server 测试 Client 行为
- 测试连接断开/重连场景
- 测试超时处理

## 8. 相关文件路径

- `src/agents/mcp/**/*.rs` - MCP 模块主代码
- `src/core/src/config/config.rs` - 配置定义（`ApiMcpProperties`, `EvaluatorMcpServerConfig`）
- `src/evaluator/src/executor/adk/tools/mcp_binance_tool.rs` - Evaluator 集成示例

## 9. 与其他模块的交互

| 模块 | 交互方式 | 说明 |
|------|----------|------|
| `sigbot-core` | 配置依赖 | 读取 `services.api.mcp` 和 `services.evaluator.mcp_servers` |
| `sigbot-evaluator` | Client 调用 | Evaluator 通过 `McpClientManager` 调用外部 MCP 服务 |
| `sigbot-api` | Server 包装 | MCP Server 包装 RESTful API 暴露给外部 |
| `sigbot-a2a` | 协议互补 | A2A 用于 Agent 间通信，MCP 用于与外部服务通信 |

## 10. 最佳实践

1. **配置优先**：所有 MCP 行为通过配置控制，避免硬编码
2. **分层清晰**：MCP Protocol Layer 与 Business Logic Layer 分离
3. **错误隔离**：MCP 调用失败不应影响核心业务流程
4. **性能考虑**：MCP 调用应设置合理超时，避免阻塞主流程
5. **可观测性**：记录 MCP 调用的指标和日志，便于问题排查
