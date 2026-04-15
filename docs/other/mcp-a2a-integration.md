# Sigbot MCP & A2A 集成指南

## 概述

本指南介绍如何为 Sigbot 交易平台集成 MCP (Model Context Protocol) 和 A2A (Agent-to-Agent) 能力。

## 架构说明

### MCP 与 ADK 的关系

```
┌─────────────────────────────────────────────────────────────────┐
│                     协议层次图                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Sigbot 应用层                                │    │
│  ├─────────────────────────────────────────────────────────┤    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │    │
│  │  │   Evaluator │  │  Strategy   │  │   Backtest  │     │    │
│  │  │   (Agents)  │  │   Runner    │  │   Runner    │     │    │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │    │
│  │         │                 │                 │            │    │
│  │         └─────────────────┼─────────────────┘            │    │
│  │                           │                              │    │
│  │         ┌─────────────────▼─────────────────┐            │    │
│  │         │        ADK-Rust (wl4g)            │            │    │
│  │         │  - Agent 编排                      │            │    │
│  │         │  - Tool trait                     │            │    │
│  │         │  - Session 管理                    │            │    │
│  │         └─────────────────┬─────────────────┘            │    │
│  │                           │                              │    │
│  │         ┌─────────────────▼─────────────────┐            │    │
│  │         │        Tool Adapter               │            │    │
│  │         │  (MCP Tool ↔ ADK Tool)            │            │    │
│  │         └─────────────────┬─────────────────┘            │    │
│  │                           │                              │    │
│  │         ┌─────────────────▼─────────────────┐            │    │
│  │         │        MCP Module (独立)           │            │    │
│  │         │  - MCP Protocol (Anthropic)       │            │    │
│  │         │  - Resources/Tools/Prompts        │            │    │
│  │         │  - SSE/STDIO Transport            │            │    │
│  │         └─────────────────┬─────────────────┘            │    │
│  │                           │                              │    │
│  │         ┌─────────────────┼─────────────────┐            │    │
│  │         │                 │                 │            │    │
│  │  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐     │    │
│  │  │  MCP Server │  │  MCP Client │  │  A2A Gateway│     │    │
│  │  │  (对外暴露)  │  │  (调用外部)  │  │  (Agent 通信) │     │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  外部系统：                                                        │
│  - 研究员内部系统 ←→ MCP Server                                  │
│  - Binance/Bitget MCP ←→ MCP Client                             │
│  - 外部 Agent ←→ A2A Gateway                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 关键设计决策

**为什么 MCP 模块不直接依赖 ADK？**

1. **协议独立性**: MCP 是 Anthropic 定义的独立协议，不应依赖 Google ADK
2. **可测试性**: MCP 模块可以独立测试，无需 ADK 运行时
3. **灵活性**: 可以适配任何 Tool 系统（ADK 只是其中之一）
4. **标准兼容**: 确保与官方 MCP SDK 的兼容性

**正确的依赖关系**:

```
sigbot-mcp (独立)
├── 核心 MCP 协议实现
├── SSE/STDIO 传输
└── 可选：adk-adapter feature (可选启用)

sigbot-evaluator
├── adk-core (Agent 框架)
├── adk-tool (Tool trait)
└── sigbot-mcp (MCP 集成)
```

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Sigbot MCP & A2A Architecture                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐         ┌──────────────────┐                      │
│  │  External MCP    │         │  Researcher's    │                      │
│  │  Services        │         │  Internal System │                      │
│  │  (Binance/Bitget)│         │                  │                      │
│  └────────┬─────────┘         └────────┬─────────┘                      │
│           │ MCP Client                 │ MCP Server                      │
│           ▼                            ▼                                 │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │                   Sigbot MCP Module                      │            │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │            │
│  │  │ MCP Client  │  │ MCP Server  │  │   A2A       │     │            │
│  │  │ (external)  │  │ (sigbot)    │  │   Gateway   │     │            │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │            │
│  └─────────────────────────────────────────────────────────┘            │
│           │                            │                                 │
│           ▼                            ▼                                 │
│  ┌──────────────────┐         ┌──────────────────┐                      │
│  │  Evaluator       │         │  Sigbot Core     │                      │
│  │  (ADK Agents)    │         │  (API/Strategy)  │                      │
│  └──────────────────┘         └──────────────────┘                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## 模块结构

```
src/
├── mcp/                          # MCP 模块
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── config.rs             # MCP 配置
│       ├── error.rs              # MCP 错误类型
│       ├── server/               # MCP Server
│       │   ├── mod.rs
│       │   ├── mcp_server.rs     # 服务器核心
│       │   ├── sigbot_resource.rs # 资源定义
│       │   ├── sigbot_tool.rs    # 工具定义
│       │   ├── sigbot_tool_adapter.rs # ADK Tool 适配
│       │   └── transport/        # 传输层
│       │       ├── mod.rs
│       │       ├── sse_server.rs # SSE 传输
│       │       └── stdio_transport.rs # STDIO 传输
│       └── client/               # MCP Client
│           ├── mod.rs
│           ├── mcp_client.rs     # 客户端核心
│           ├── external_mcp_tool.rs # 外部工具封装
│           └── mcp_toolset.rs    # 工具集聚合
│
└── a2a/                          # A2A 模块
    ├── Cargo.toml
    └── src/
        ├── lib.rs
        ├── a2a_protocol.rs       # A2A 协议定义
        ├── remote_agent_client.rs # 远程 Agent 客户端
        └── agent_gateway.rs      # Agent 网关
```

## 使用场景

### 场景 1: 研究员内部系统通过 MCP 调用 Sigbot

**配置 MCP Server:**

```bash
# 启动 MCP Server
cargo run --bin sigbot -- mcp-server \
  --bind-address "0.0.0.0:8080" \
  --transport sse \
  --auth-enabled true
```

**研究员系统调用示例:**

```javascript
// 1. 连接到 MCP Server (SSE endpoint)
const eventSource = new EventSource('http://sigbot:8080/sse');

// 2. 发送工具调用请求
fetch('http://sigbot:8080/message', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    jsonrpc: '2.0',
    id: 1,
    method: 'tools/call',
    params: {
      name: 'run_backtest',
      arguments: {
        strategy_id: 'strategy_123',
        start_time: '2024-01-01T00:00:00Z',
        end_time: '2024-03-01T00:00:00Z',
        initial_capital: 100000
      }
    }
  })
});

// 3. 通过 SSE 接收结果
eventSource.onmessage = (event) => {
  const response = JSON.parse(event.data);
  console.log('Backtest result:', response);
};
```

**可用 MCP 工具:**

| 工具名称 | 描述 |
|---------|------|
| `get_strategies` | 获取所有策略列表 |
| `run_backtest` | 执行回测 |
| `get_market_data` | 获取市场数据 |
| `get_position` | 获取持仓信息 |

**可用 MCP Resources:**

| Resource URI | 描述 |
|-------------|------|
| `sigbot://strategies` | 所有策略 |
| `sigbot://backtests` | 所有回测结果 |
| `sigbot://evaluators` | 所有评估器状态 |
| `sigbot://market-data` | 市场数据 |
| `sigbot://positions` | 持仓信息 |

---

### 场景 2: Sigbot 调用外部 MCP 服务 (如 Binance MCP)

**配置外部 MCP 服务:**

```yaml
# external_mcp_services.yaml
services:
  - name: "binance-mcp"
    url: "https://mcp.binance.com/sse"
    transport: "sse"
    api_key: "${BINANCE_API_KEY}"
    api_secret: "${BINANCE_API_SECRET}"
    enabled_tools:
      - "get_price"
      - "get_klines"
      - "get_orderbook"

  - name: "bitget-mcp"
    url: "https://mcp.bitget.com/sse"
    transport: "sse"
    api_key: "${BITGET_API_KEY}"
    api_secret: "${BITGET_API_SECRET}"
    enabled_tools:
      - "get_ticker"
      - "get_candles"
```

**在 Evaluator 中使用 MCP 工具:**

```rust
use sigbot_mcp::client::{McpClient, McpClientManager};
use sigbot_mcp::config::ExternalMcpService;

// 1. 创建 MCP Client Manager
let manager = McpClientManager::new();

// 2. 注册 Binance MCP 服务
let binance_config = ExternalMcpService {
    name: "binance-mcp".to_string(),
    url: "https://mcp.binance.com/sse".to_string(),
    transport: "sse".to_string(),
    auth_token: None,
    api_key: Some("your-api-key".to_string()),
    api_secret: Some("your-api-secret".to_string()),
    enabled_tools: vec!["get_price".to_string(), "get_klines".to_string()],
};

manager.register(binance_config).await.unwrap();

// 3. 获取 MCP Client
let binance_client = manager.get("binance-mcp").await.unwrap();

// 4. 调用外部 MCP 工具
let price_result = binance_client
    .call_tool("get_price", serde_json::json!({"symbol": "BTCUSDT"}))
    .await
    .unwrap();

println!("BTC Price: {:?}", price_result);
```

**在策略评估中使用 MCP 工具:**

```rust
// evaluator 模块自动集成 MCP 工具
// src/evaluator/src/executor/adk/tools/mod.rs

// 使用默认工具 (包含 MCP 工具)
let toolset = register_default_tools();

// 或使用带 MCP Client 的工具集
let mcp_clients = vec![binance_client]; // 从 manager 获取
let toolset_with_mcp = register_tools_with_mcp(mcp_clients);

// AlphaAgent 可以使用这些工具获取实时市场数据
```

---

### 场景 3: A2A - Sigbot Agent 与外部 Agent 通信

**配置 Agent Gateway:**

```rust
use sigbot_a2a::{AgentGateway, AgentRegistry, AgentGatewayConfig};

// 1. 创建 Agent Registry
let registry = Arc::new(AgentRegistry::new());

// 2. 注册本地 Agent (Sigbot 内部)
registry.register_local(AgentMetadata {
    agent_id: "sigbot-alpha-agent".to_string(),
    name: "Alpha Agent".to_string(),
    description: "Strategy analysis agent".to_string(),
    version: "1.0.0".to_string(),
    capabilities: vec![/* ... */],
    endpoint: None,
    tags: HashMap::new(),
}).await;

// 3. 注册远程 Agent
let remote_client = RemoteAgentClient::new(
    "https://research-agent.example.com",
    agent_metadata
);
registry.register_remote(remote_client).await;

// 4. 创建并启动 Gateway
let config = AgentGatewayConfig {
    name: "sigbot-a2a-gateway".to_string(),
    bind_address: "0.0.0.0:8081".to_string(),
    auth_enabled: true,
    jwt_secret: Some("your-jwt-secret".to_string()),
    rate_limit: Some(100),
};

let gateway = AgentGateway::new(config, registry);
gateway.start().await.unwrap();
```

**Sigbot Agent 调用外部 Agent:**

```rust
use sigbot_a2a::{RemoteAgentClient, A2ARequest, a2a_protocol::request_types};

// 1. 创建远程 Agent Client
let client = RemoteAgentClient::new(
    "https://research-agent.example.com",
    agent_metadata
).with_auth_token("bearer-token");

// 2. 发送请求
let request = A2ARequest::new(
    "research-agent",
    request_types::QUERY,
    serde_json::json!({
        "query": "Analyze market sentiment for BTC",
        "time_range": "24h"
    })
);

let response = client.execute(request).await.unwrap();
println!("Research result: {:?}", response.payload);
```

**外部系统调用 Sigbot Agent:**

```bash
# POST http://sigbot:8081/execute
curl -X POST http://sigbot:8081/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "request_id": "req-123",
    "target_agent": "sigbot-evaluator-agent",
    "request_type": "execute",
    "payload": {
      "workflow_id": "wf-456",
      "action": "evaluate_strategy",
      "parameters": {...}
    },
    "timestamp": "2024-03-28T10:00:00Z"
  }'
```

---

## 配置选项

### MCP Server 配置

| 参数 | 默认值 | 描述 |
|-----|-------|------|
| `--bind-address` | `0.0.0.0:8080` | 绑定地址 |
| `--transport` | `sse` | 传输类型：sse, stdio, both |
| `--auth-enabled` | `true` | 启用认证 |
| `--external-mcp-services` | - | 外部 MCP 服务配置文件路径 |

### 环境变量

```bash
# MCP Server
export SIGBOT_MCP_JWT_SECRET="your-jwt-secret"

# Binance MCP
export BINANCE_API_KEY="your-api-key"
export BINANCE_API_SECRET="your-api-secret"

# Bitget MCP
export BITGET_API_KEY="your-api-key"
export BITGET_API_SECRET="your-api-secret"
```

---

## 开发指南

### 添加新的 MCP Resource

```rust
// 在 server/sigbot_resource.rs 中
registry.register_template(ResourceTemplate {
    uri_template: "sigbot://new-resource/{id}".to_string(),
    name: "New Resource".to_string(),
    description: "Description of the new resource".to_string(),
    mime_type: "application/json".to_string(),
});
```

### 添加新的 MCP Tool

```rust
// 在 cmd/mcp_server_starter.rs 中
registry.register(
    SigbotTool::new(
        "new_tool",
        "Tool description",
        serde_json::json!({
            "type": "object",
            "properties": {
                "param1": {"type": "string"}
            },
            "required": ["param1"]
        }),
    ),
    NewToolHandler,
);

// 实现 Handler
struct NewToolHandler;

#[async_trait::async_trait]
impl ToolHandler for NewToolHandler {
    async fn execute(&self, args: Value) -> sigbot_mcp::error::Result<Value> {
        // 实现工具逻辑
        Ok(serde_json::json!({"result": "success"}))
    }
}
```

### 集成新的外部 MCP 服务

```rust
// 在 evaluator/tools/mod.rs 中
use sigbot_mcp::client::ExternalMcpToolBuilder;

// 创建新的外部服务 Client
let client = McpClient::new(ExternalMcpService {
    name: "new-exchange-mcp".to_string(),
    url: "https://mcp.new-exchange.com/sse".to_string(),
    // ... 配置
});

// 构建工具
let builder = ExternalMcpToolBuilder::new(client.clone());
let tools = builder.build_all().await;

// 添加到工具集
```

---

## 测试

### MCP Server 测试

```bash
# 使用 MCP Inspector 测试
npx @modelcontextprotocol/inspector \
  http://localhost:8080/sse \
  http://localhost:8080/message
```

### 单元测试

```bash
# 运行 MCP 模块测试
cargo test -p sigbot-mcp

# 运行 A2A 模块测试
cargo test -p sigbot-a2a

# 运行 Evaluator MCP 工具测试
cargo test -p sigbot-evaluator -- mcp
```

---

## 故障排除

### 常见问题

1. **MCP Client 连接失败**
   - 检查外部 MCP 服务 URL 是否正确
   - 验证 API Key/Secret 配置
   - 确认网络连接和防火墙规则

2. **工具调用返回 Mock 数据**
   - 确认 MCP Client 已成功连接 (`is_connected()` 检查)
   - 检查外部服务是否返回正确的工具列表

3. **A2A 请求超时**
   - 检查 Agent Gateway 是否运行
   - 验证 JWT Token 有效性
   - 增加请求超时时间

---

## 安全考虑

1. **认证**: MCP Server 和 A2A Gateway 都应启用 JWT 认证
2. **速率限制**: 配置适当的 rate limit 防止滥用
3. **审计日志**: 记录所有 MCP 和 A2A 请求用于审计
4. **最小权限**: MCP Client 只配置必要的工具权限

---

## 参考

- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [ADK-Rust Documentation](https://docs.rs/adk-core/)
- [Sigbot Architecture](../docs/deploy/architecture.md)
