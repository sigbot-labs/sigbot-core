# Evaluator Multi-Agent Tool System

本文档说明如何使用基于 adk-rust 标准库的 evaluator 模块工具系统。

## 概述

evaluator 模块现在使用 [adk-rust](https://github.com/wl4g-ai/adk-rust) 标准库来实现 multi-agent 系统，提供了：

- **标准化的 Agent 接口**: 使用 `adk_core::Agent` trait
- **标准化的 Tool 接口**: 使用 `adk_core::Tool` trait  
- **工具集管理**: 使用 `adk_tool::Toolset` 进行工具注册和管理
- **向后兼容**: 通过 `ISigbotAgent` 和适配器模式保持与现有代码的兼容性

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    SigbotOrchestrator                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Toolset (Binance, Twitter, etc.)                    │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│         ┌────────────────┼────────────────┐                 │
│         ▼                ▼                ▼                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │  Boot    │───▶│  Loader  │───▶│  Alpha   │              │
│  │  Agent   │    │  Agent   │    │  Agent   │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                        │                │                    │
│                        ▼                ▼                    │
│                   Tool Calls      Signal Generation         │
└─────────────────────────────────────────────────────────────┘
```

## 可用工具

### Binance 工具

1. **BinanceMarketDataTool** - 获取当前市场价格
   ```rust
   // 参数: { "symbol": "BTCUSDT" }
   // 返回: { "symbol": "BTCUSDT", "price": 42000.0, "timestamp": 1234567890 }
   ```

2. **BinanceKlineTool** - 获取历史 K 线数据
   ```rust
   // 参数: { "symbol": "BTCUSDT", "interval": "1h", "limit": 24 }
   // 返回: { "symbol": "BTCUSDT", "klines": [...], "count": 24 }
   ```

3. **BinanceVolumeTool** - 获取交易量分析
   ```rust
   // 参数: { "symbol": "BTCUSDT", "interval": "1h", "limit": 24 }
   // 返回: { "total_volume": 12345.67, "volume_trend_percent": 5.2 }
   ```

### Twitter 工具

1. **TwitterSearchTool** - 搜索推文
2. **TwitterUserTool** - 获取用户信息
3. **TwitterTrendsTool** - 获取趋势话题

## 使用示例

### 1. 注册工具到 Orchestrator

```rust
use sigbot_evaluator::tools::register_default_tools;
use sigbot_evaluator::core::orchestrator::SigbotOrchestrator;
use std::sync::Arc;

// 注册所有默认工具
let toolset = Arc::new(register_default_tools());

// 创建 orchestrator 并添加工具集
let orchestrator = SigbotOrchestrator::new(agents)
    .with_toolset(toolset);
```

### 2. LoaderAgent 自动工具调用

LoaderAgent 会自动：
1. 从 context 中发现可用工具
2. 使用 LLM 分析 bootstrap 数据并选择合适的工具
3. 调用选中的工具并聚合结果
4. 将结果传递给下一个 agent

```rust
use sigbot_evaluator::agents::loader_agent::SigbotLoaderAgent;
use sigbot_evaluator::core::agent_base::SigbotAgentContext;

let loader = SigbotLoaderAgent::new();
let mut ctx = SigbotAgentContext::new("tenant1".to_string(), Some("workflow1".to_string()));

// Orchestrator 会自动将 toolset 添加到 context
let result = loader.execute(&ctx).await?;

// result.data 包含工具调用结果
println!("Tool results: {:?}", result.data.get("tool_results"));
```

### 3. 创建自定义工具

```rust
use adk_core::{Tool, ToolContext, Result as AdkResult};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

pub struct CustomTool;

#[async_trait]
impl Tool for CustomTool {
    fn name(&self) -> &str {
        "custom_tool"
    }

    fn description(&self) -> &str {
        "My custom tool description"
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "param1": {
                    "type": "string",
                    "description": "Parameter description"
                }
            },
            "required": ["param1"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let param1 = args["param1"].as_str().unwrap();
        
        // 实现工具逻辑
        Ok(json!({
            "result": format!("Processed: {}", param1)
        }))
    }
}

// 注册到 toolset
let mut toolset = BasicToolset::new("my_tools");
toolset.add_tool(Arc::new(CustomTool));
```

### 4. 定期执行 LoaderAgent

```rust
use tokio::time::{interval, Duration};

// 每 1 分钟执行一次 LoaderAgent
let mut ticker = interval(Duration::from_secs(60));

loop {
    ticker.tick().await;
    
    // 执行 loader agent
    let result = loader.execute(&ctx).await?;
    
    // 处理结果并传递给 AlphaAgent
    if result.success {
        // 分析支撑/阻力位
        // 生成交易信号
        // 推送给 strategy runner
    }
}
```

## 工作流程

### 完整的数据流

1. **BootAgent** 收集市场统计信息
   ```
   { "symbol": "BTCUSDT", "timeframe": "1h" }
   ```

2. **LoaderAgent** 使用工具获取数据
   - 调用 `binance_kline` 获取历史数据
   - 调用 `binance_volume` 获取交易量
   - 调用 `twitter_search` 获取社交媒体情绪
   
3. **AlphaAgent** 分析数据生成信号
   ```json
   {
     "signals": [
       {
         "type": "support",
         "level": 42000.0,
         "strength": 0.85
       },
       {
         "type": "resistance",
         "level": 45000.0,
         "strength": 0.78
       }
     ],
     "recommendation": "BUY",
     "confidence": 0.72
   }
   ```

4. **Strategy Runner** 执行 pycode 策略
   - 接收 AlphaAgent 的信号
   - 在 pycode 节点中执行策略逻辑
   - 生成交易订单

## 与 adk-rust 的集成

### 标准 Agent Trait

```rust
use adk_core::Agent;

#[async_trait]
pub trait Agent: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn sub_agents(&self) -> &[Arc<dyn Agent>];
    async fn run(&self, ctx: Arc<dyn InvocationContext>) -> Result<EventStream>;
}
```

### 标准 Tool Trait

```rust
use adk_core::Tool;

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters_schema(&self) -> Option<Value>;
    async fn execute(&self, ctx: Arc<dyn ToolContext>, args: Value) -> Result<Value>;
}
```

### 向后兼容

现有的 `ISigbotAgent` 实现可以通过 `SigbotAgentAdapter` 转换为标准 `Agent`:

```rust
let legacy_agent = Arc::new(SigbotLoaderAgent::new());
let ctx = SigbotAgentContext::new("tenant1".to_string(), None);
let adapter = SigbotAgentAdapter::new(legacy_agent, ctx);

// adapter 现在实现了 adk_core::Agent
let event_stream = adapter.run(invocation_ctx).await?;
```

## 下一步

1. **实现实际的工具调用**: 当前 LoaderAgent 返回占位符数据，需要实际调用工具
2. **集成 adk-runner**: 使用 adk-runner 来管理 agent 执行和事件流
3. **实现 Twitter API**: 完成 Twitter 工具的实际 API 集成
4. **添加更多工具**: 如 TruthSocial, Coinmarketcap 等
5. **实现信号传递**: 完成 AlphaAgent 到 Strategy Runner 的信号传递机制

## 参考资料

- [adk-rust GitHub](https://github.com/wl4g-ai/adk-rust)
- [adk-rust 文档](https://github.com/wl4g-ai/adk-rust/tree/main/docs)
- [Google ADK-Go](https://github.com/google/adk-go) (参考实现)
