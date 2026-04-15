---
name: a2a
description: A2A (Agent-to-Agent) 模块编码指南，基于 adk-core 实现最小化 A2A 协议支持，用于暴露 Sigbot Agents 给外部系统调用。
---

# A2A Module Guide

## 1. 模块设计核心

- **模块定位**：A2A 模块提供 **最小化 A2A 协议实现**，直接基于 `adk-core` 和 `axum` 构建
- **核心职责**：
  - **A2A Server**：使用 `axum` HTTP 框架暴露 Agents
  - **薄包装原则**：不自定义复杂协议，直接复用 `adk-core` 的 `Agent` trait

- **架构原则**：
  - **高内聚**：所有 A2A 相关功能封装在此模块
  - **低耦合**：仅依赖 `adk-core`、`axum`，不依赖不稳定的 `adk-server`
  - **薄包装**：仅提供 Sigbot 特定的配置默认值和 Agent 集成

- **设计边界**（重要）：
  - ✅ **A2A Server**：暴露 Sigbot Agents 给外部调用（核心场景）
  - ⚠️ **RemoteA2aAgent**：可选，用于调用其他 A2A 服务（通过 `adk_core::Agent` 扩展）

## 2. 模块目录结构

```
src/agents/a2a/
├── Cargo.toml
└── src/
    ├── lib.rs           # 模块入口，导出 A2A 类型
    └── server.rs        # A2A Server 实现（JSON-RPC + Agent 集成）
```

## 3. A2A 协议端点

A2A Server 自动暴露以下端点：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/.well-known/agent.json` | GET | Agent Card 发现（标准 A2A 协议） |
| `/a2a` | POST | JSON-RPC A2A 协议端点 |
| `/healthz` | GET | 健康检查 |

## 4. 配置结构

**配置路径**：`services.a2a`

```yaml
services:
  a2a:
    enabled: true
    bind-address: "0.0.0.0:8081"
    # 注意：A2A 协议是标准的，不需要自定义 capabilities
    # Agent 的能力通过 adk_core::Agent trait 自动暴露
```

## 5. 核心组件

### 5.1 A2AServerBuilder

**职责**：构建 A2A Server，集成 `adk_core::Agent`

```rust
use sigbot_a2a::A2AServerBuilder;
use adk_core::Agent;
use std::sync::Arc;

// 创建 Agent（实现 adk_core::Agent trait）
let agent: Arc<dyn Agent> = Arc::new(create_sigbot_agent());

// 构建并启动 A2A Server
let server = A2AServerBuilder::new()
    .with_agent_loader(agent)
    .with_bind_address("0.0.0.0:8081")
    .build();

server.start().await?;
```

### 5.2 复用 adk-core 类型

```rust
// 直接导出供外部使用
pub use adk_core::Agent;  // Agent trait

// A2A 模块提供的类型：
// - A2AServer, A2AServerBuilder
// - A2AConfig, A2ARequest, A2AResponse
// - AgentCard, AgentCapability
```

## 6. 分层架构

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
│  标准端点：                                                        │
│  - GET  /.well-known/agent.json  (Agent Card)                   │
│  - POST /a2a                     (JSON-RPC)                     │
│  - GET  /healthz                   (Health Check)               │
└─────────────────────────────────────────────────────────────────┘
```

## 7. 使用场景

### 7.1 暴露 Evaluator Agent 给 sigbot-researcher

```rust
use sigbot_a2a::A2AServerBuilder;
use adk_core::Agent;
use std::sync::Arc;

async fn start_evaluator_a2a_server() -> Result<()> {
    // 创建 Evaluator Agent（实现 adk_core::Agent trait）
    let evaluator_agent: Arc<dyn Agent> = Arc::new(create_evaluator_agent());

    // 构建并启动服务器
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

### 7.2 外部系统调用（curl 示例）

```bash
# 1. 获取 Agent Card（发现 Agent 能力）
curl http://localhost:8081/.well-known/agent.json | jq

# 响应示例:
# {
#   "name": "evaluator-agent",
#   "description": "Strategy evaluation agent",
#   "capabilities": [...]
# }

# 2. 发送 A2A 消息（JSON-RPC 格式）
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

### 7.3 使用 RemoteA2aAgent 调用其他 A2A 服务（可选）

```rust
use adk_core::Agent;

// 创建远程 Agent 引用（用于调用其他 A2A 服务器）
// 注意：当前实现中，RemoteA2aAgent 需要自行实现 adk_core::Agent trait
// 通过 HTTP 客户端调用远程 A2A 服务
```

## 8. 编码指南

### 8.1 复用 adk-core 类型

**禁止**自定义 A2A 协议结构体，直接使用 `adk-core`：

```rust
// ❌ 错误：不要自定义
pub struct MyA2ARequest { ... }
pub struct MyA2AResponse { ... }

// ✅ 正确：使用 A2A 模块提供的标准类型
// - A2ARequest, A2AResponse 由 sigbot-a2a 提供
// - Agent trait 由 adk-core 提供
```

### 8.2 实现 ADK Agent

```rust
use adk_core::{Agent, EventStream, InvocationContext, Result as AdkResult};
use std::sync::Arc;

pub struct SigbotEvaluatorAgent;

#[async_trait]
impl Agent for SigbotEvaluatorAgent {
    async fn run(&self, ctx: Arc<dyn InvocationContext>) -> AdkResult<EventStream> {
        // 实现 Agent 逻辑
        // A2A 请求会通过 axum 路由到这里
        Ok(EventStream::default())
    }
}
```

### 8.3 依赖管理

```toml
[dependencies]
# 核心依赖
adk-core = "0.2.0"

# Web 框架
axum = "0.8"
tokio = "1.43"

# 序列化
serde = "1.0"
serde_json = "1.0"
```

## 9. 测试指南

### 9.1 单元测试

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
            .with_bind_address("0.0.0.0:0")  // 使用端口 0 进行测试
            .build();

        assert_eq!(server.config.bind_address, "0.0.0.0:0");
    }
}
```

### 9.2 集成测试

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

## 10. 相关文件路径

- `src/agents/a2a/**/*.rs` - A2A 模块主代码
- `src/core/src/config/config.rs` - 配置定义（`A2AProperties`）
- `src/evaluator/src/executor/adk/agents/` - ADK Agent 实现示例

## 11. 与其他模块的交互

| 模块 | 交互方式 | 说明 |
|------|----------|------|
| `sigbot-core` | 配置依赖 | 读取 `services.a2a` 配置 |
| `sigbot-evaluator` | Agent 暴露 | Evaluator Agents 通过 A2A 暴露给外部 |
| `sigbot-mcp` | 互补关系 | MCP 用于 Sigbot **调用外部**服务，A2A 用于**暴露给外部**调用 |
| `adk-core` | 核心依赖 | Agent trait 定义 |

## 12. 最佳实践

1. **薄包装原则**：不要自定义 A2A 协议类型，直接复用 `adk-core` 和 `axum`
2. **标准端点**：保持 `/.well-known/agent.json`、`/a2a`、`/healthz` 端点不变
3. **JSON-RPC 格式**：A2A 协议使用标准 JSON-RPC 2.0 格式
4. **Agent Card 发现**：确保 Agent Card 正确描述 Agent 能力
5. **Agent 集成**：通过 `Arc<dyn Agent>` 集成 adk-core agents

## 13. 与 MCP 模块对比

| 特性 | MCP | A2A |
|------|-----|-----|
| **方向** | Sigbot **调用外部**服务 | **暴露给外部**调用 Sigbot |
| **协议** | Model Context Protocol | A2A Protocol (JSON-RPC) |
| **依赖** | `rust-mcp-sdk` | `adk-core`, `axum` |
| **配置** | `services.api.mcp`, `services.evaluator.mcp_servers` | `services.a2a` |
| **典型场景** | Evaluator 调用 Binance MCP 获取数据 | sigbot-researcher 调用 Evaluator Agent |
| **端点** | 无（MCP 是客户端协议） | `/.well-known/agent.json`, `/a2a`, `/healthz` |

## 14. A2A 协议示例

### JSON-RPC 请求格式

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

### JSON-RPC 响应格式

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
