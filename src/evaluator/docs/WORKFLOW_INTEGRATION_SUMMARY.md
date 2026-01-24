# Evaluator Manager - WorkflowManager Integration Summary

## 概述

成功将 Evaluator Manager 重构为与 WorkflowManager 集成的微服务，完全遵循 Strategy Runner 的架构模式。现在 Evaluator Manager 正确处理 `EVALUATION` stage 中 `LLM` 类型的节点，而 Strategy Runner 处理 `PYCODE` 类型的节点。

## 关键变更

### 1. **微服务职责划分**

**Evaluator Manager (本微服务)**:
- 负责 `EVALUATION` stage 中 `StrategyProvider::LLM` 类型的节点
- 执行 Multi-Agent 工作流（BootAgent → LoaderAgent → AlphaAgent ↔ AuditorAgent）
- 使用 LLM 动态计算策略超参

**Strategy Runner (另一个微服务)**:
- 负责 `EVALUATION` stage 中 `StrategyProvider::PYCODE` 类型的节点
- 执行静态 Python 策略代码
- 使用预定义的策略逻辑

### 2. **架构对比**

#### Strategy Runner Pattern (参考)
```rust
// strategy_runner.rs
SigbotStrategyRunner
├── WorkflowManager (Time-Driven)
│   └── Scan t_workflow table → Start PYCODE strategy jobs
├── Start Handler
│   └── Filter: EVALUATION + Strategy(PYCODE)
│   └── Create: SigbotStrategyExecutor
└── Stop Handler
    └── Shutdown: SigbotStrategyExecutor
```

#### Evaluator Manager Pattern (实现)
```rust
// evaluator_manager.rs
SigbotEvaluatorManager
├── WorkflowManager (Time-Driven)
│   └── Scan t_workflow table → Start LLM evaluation jobs
├── Start Handler
│   └── Filter: EVALUATION + Strategy(LLM)
│   └── Create: SigbotEvaluationExecutor
├── Stop Handler
│   └── Shutdown: SigbotEvaluationExecutor
└── Event-Driven (Messager)
    └── Subscribe to manual trigger events
```

### 3. **核心组件**

#### SigbotEvaluatorManager
- 主入口类，负责初始化和启动
- 集成 WorkflowManager 进行 workflow 扫描
- 订阅 messager 事件用于手动触发

#### SigbotEvaluationExecutorFactory
- 管理 evaluation executor 的生命周期
- 维护 executor registry: `executor_id → SigbotEvaluationExecutor`
- executor_id 格式: `"{workflow_id}:{node_id}"`

#### SigbotEvaluationExecutor
- 管理单个 LLM evaluation workflow 的执行
- 创建和配置所有 agents (datafeed + strategy)
- 执行 orchestrator 并发布结果

### 4. **Start Handler 逻辑**

```rust
// 1. 提取 StrategyProvider
match &node_info.stage {
    WorkflowStageType::EVALUATION(providers) => {
        match providers.as_ref().and_then(|v| v.first()) {
            Some(WorkflowStageWrapper::Strategy(provider)) => {
                // 2. 检查是否为 LLM provider (不是 PYCODE)
                if *provider != StrategyProvider::LLM {
                    // Skip: 这是 Strategy Runner 的责任
                    return Err(...);
                }
                provider.clone()
            }
            _ => return Err(...), // 不是 Strategy provider
        }
    }
    _ => return Err(...), // 不是 EVALUATION stage
}

// 3. 初始化并启动 evaluation executor
SigbotEvaluationExecutorFactory::init(workflow_id, node_id, provider, argument).await?
    .startup(messager).await;
```

### 5. **Stop Handler 逻辑**

```rust
// 1. 构建 executor_id
let executor_id = format!("{}:{}", workflow_id, node_id);

// 2. 注销并关闭 evaluation executor
SigbotEvaluationExecutorFactory::close(executor_id).await?;
```

### 6. **Executor 生命周期**

```rust
// 创建 executor
let executor = SigbotEvaluationExecutor::new(executor_id, workflow_id, node_id);

// 启动 executor (后台任务循环执行)
executor.startup(messager).await;
    ↓
    loop {
        // 检查是否停止
        if *stop_chan.read().await { break; }
        
        // 执行 orchestrator
        let result = orchestrator.execute(ctx).await?;
        
        // 发布 hyperparameter update
        messager.publish(&topic, &message).await?;
        
        // 等待下次执行 (1 hour)
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }

// 关闭 executor
executor.shutdown().await;
```

## 数据流

```
1. WorkflowManager 扫描 t_workflow 表
   │
   ▼
2. 发现 EVALUATION stage + LLM provider 节点
   │
   ▼
3. 调用 Start Handler
   │
   ├─ 验证 provider == LLM
   ├─ 创建 SigbotEvaluationExecutor
   └─ 启动后台任务
      │
      ▼
4. 后台任务循环执行
   │
   ├─ BootAgent (并行) → 收集基础统计
   ├─ LoaderAgent (并行) → 获取温数据
   │
   ├─ AlphaAgent (顺序) → 计算超参
   └─ AuditorAgent (顺序) → 审计验证
      │ (失败) → 重试 AlphaAgent
      │
      ▼
5. 发布 Hyperparameter Update 到 Messager
   │
   ├─ Strategy Runner 订阅并应用到 Python 代码
   └─ Notification Forwarder 转发到用户通知渠道
```

## 与 Strategy Runner 的对比

| 特性 | Strategy Runner | Evaluator Manager |
|------|----------------|-------------------|
| **Stage** | EVALUATION | EVALUATION |
| **Provider** | StrategyProvider::PYCODE | StrategyProvider::LLM |
| **执行内容** | 静态 Python 策略代码 | Multi-Agent LLM 工作流 |
| **触发方式** | WorkflowManager (time-driven) | WorkflowManager (time-driven) + Messager (event-driven) |
| **Executor** | SigbotStrategyExecutor | SigbotEvaluationExecutor |
| **Factory** | SigbotStrategyExecutorFactory | SigbotEvaluationExecutorFactory |
| **输出** | 交易信号 | 策略超参 |

## 配置示例

### Workflow Node (LLM Evaluation)
```json
{
  "id": 1,
  "name": "AI Evaluator",
  "stage": "LLM@EVALUATION",
  "config": {
    "model": "gemini-2.0-flash-exp",
    "temperature": 0.7,
    "max_tokens": 4096
  }
}
```

### Workflow Node (PYCODE Strategy)
```json
{
  "id": 2,
  "name": "Python Strategy",
  "stage": "PYCODE@EVALUATION",
  "config": {
    "script_path": "/path/to/strategy.py",
    "entry_point": "main"
  }
}
```

## 未来增强

1. **配置化**: 从配置文件加载 tenant_id, strategy_id 等参数
2. **手动触发**: 通过 messager 事件手动触发 evaluation
3. **状态同步**: 将 executor 状态同步到数据库
4. **指标监控**: 添加 Prometheus 指标
5. **分布式追踪**: 添加 OpenTelemetry 追踪

## 总结

成功将 Evaluator Manager 重构为与 WorkflowManager 集成的微服务，完全遵循 Strategy Runner 的架构模式。现在两个微服务各司其职：

- **Strategy Runner**: 处理 PYCODE 类型的静态策略
- **Evaluator Manager**: 处理 LLM 类型的动态 Multi-Agent 工作流

代码结构清晰，职责明确，易于维护和扩展！🎉
