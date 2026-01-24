---
name: evaluator
description: 评估器模块编码指南，负责编排 Multi-Agent 工作流，利用 LLM 动态计算策略超参。
---

# Evaluator Module Guide

## 1. 模块设计核心

- 设计概要：Evaluator 模块被设计为一个**旁路子服务**（异步旁路按需执行，而非同步减少 LLM tokens 消耗），它只是 Workflow(由用户在 UI 自由拖拽创建) 的 stage=WorkflowStageType.EVALUATION[LLM] 类型 nodes 的底层执行引擎；

- 触发执行
  - 事件驱动：从 messager(e.g emqx) 订阅 Workflow Input Stage 的如 Twitter/TruthSocial Datefeed nodes 发布的市场数据，累计到一定阈值时，将根据 用户自定义 assistant prompt 触发执行 multi agents orchestrator
  - 时间驱动：启动 cron job 定时(默认30s)动态扫描抽取 timescaledb 的 t_news_twitter/t_news_truthsocial/t_market_kline 等表最新（如4h）的数据，并根据 用户自定义 assistant prompt 触发执行 multi agents orchestrator

- 执行核心：multi agents orchestrator 将根据动态抽取的市场数据，经过多层 agents 分析执行，最终生成合格的策略超参（如价格的支撑位/阻力位、以及新闻的利空/利多的权重等），从而使静态策略代码能动态自适应市场行情。

## 2. 与 Strategy Runner 微服务职责对比

**关键设计逻辑对比**: 在 SigBot 架构中，`EVALUATION` stage 的节点由两个不同的微服务负责处理，根据 `StrategyProvider` 类型进行划分：

- **Strategy Runner 微服务** (`src/strategy/runner/`)
  - **职责**: 负责异步执行 workflow 的 `EVALUATION` stage 中 **PYCODE 类型**的节点
  - **执行内容**: 静态 Python 策略代码
  - **Provider**: `StrategyProvider::PYCODE`
  - **输出**: 交易信号 (Trading Signals)
  - **特点**: 执行预定义的策略逻辑，性能高效

- **Evaluator Manager 微服务** (`src/evaluator/`)
  - **职责**: 负责异步执行 workflow 的 `EVALUATION` stage 中 **LLM 类型**的节点
  - **执行内容**: Multi-Agent LLM 工作流
  - **Provider**: `StrategyProvider::LLM`
  - **输出**: 策略超参 (Strategy Hyperparameters)
  - **特点**: 利用 LLM 动态计算，适应性强

**架构集成**: 两个微服务都通过 `WorkflowManager` 扫描 `t_workflow` 表，根据节点的 `stage` 和 `provider` 类型自动路由到相应的微服务处理。

## 3. 核心架构 (Core Architecture)

### 实现结构 (Implementation Structure)
- **Evaluator Manager** (`SigbotEvaluatorManager`): 模块入口，负责启动和管理评估器生命周期，集成 WorkflowManager。
- **Evaluation Executor Factory** (`SigbotEvaluationExecutorFactory`): 负责根据 workflow 节点创建和管理 executor 实例。
- **Evaluation Executor** (`SigbotEvaluationExecutor`): 核心实现，采用类似 Google ADK-Go 的 **Loop + Workflow Orchestrator** 架构，负责编排 Multi-Agent 协作。
- **Generic Multi-Agent Orchestrator** (`GenericMultiAgentOrchestrator`): 通用的 Multi-Agent 编排器，支持并行和顺序执行。

### 触发机制 (Dual Trigger)
1.  **Time-Driven (WorkflowManager)**: WorkflowManager 定期扫描 `t_workflow` 表，自动启动 LLM 类型的 evaluation 节点。
2.  **Event-Driven (Messager)**: 订阅 messager 事件，支持手动触发 evaluation。

## 4. Multi-Agent 工作流 (Orchestration Logic)

工作流由 Orchestrator 驱动，分为并行和顺序两个阶段：

### 并行阶段 (Parallel Stage) - Datafeed Agents
1.  **BootAgent (引导者)**
    -   **职责**: 主动查询并收集最近的市场基础统计数据。
    -   **输出**: **引导信息 (Guidance Info)**。

2.  **LoaderAgent (加载者)**
    -   **输入**: 引导信息。
    -   **职责**: 根据 Skills Prompts 自主调用 **Fetch Tools** 加载近期多种类型的**温数据**（Warm Data，而非实时数据）。
    -   **示例数据**: 过去 1d/4h/1h/30m 的 Kline 数据、Twitter/TruthSocial 新闻等。

### 顺序阶段 (Sequential Stage) - Strategy Agents with Loop
3.  **AlphaAgent (分析者)**
    -   **输入**: 温数据。
    -   **职责**: 根据 Super Analysis Prompts 分析数据，并自主调用 **Math Tools** 精确计算**策略超参**。
    -   **示例计算**: 动态支撑位、阻力位、RSI 阈值等。

4.  **AuditorAgent (审计者)**
    -   **输入**: 拟定超参。
    -   **职责**: 根据 Risk Prompts 进行合规边界检查。
    -   **逻辑**:
        -   若检查通过：输出合规超参。
        -   若检查失败（如 ETH 支撑位异常低）：打回给 **AlphaAgent** 重算，并附带失败原因。
        -   **Timeout 机制**: 防止循环打回耗时过久导致数据失效。

## 5. 结果分发 (Publishing)
-   **Messager**: 将合规的 **超参** 发布到消息总线 (如 EMQX)。
-   **Notification**: Notification Forwarder 订阅更新，推送到用户通知渠道 (Email/Telegram)。
-   **Strategy Runner**: 订阅超参更新消息，动态应用到运行中的 Python 策略代码。

## 6. 相关文件路径 (Related File Paths)
-   `src/evaluator/**/*.rs`: 评估器模块主代码。
-   `src/evaluator/src/evaluator_manager.rs`: Evaluator Manager 主入口，集成 WorkflowManager。
-   `src/evaluator/src/core/orchestrator.rs`: 通用 Multi-Agent 编排器。
-   `src/evaluator/src/agents/**/*.rs`: 各个 Agent 实现（BootAgent, LoaderAgent, AlphaAgent, AuditorAgent）。
-   `src/core/src/modules/evaluator/**/*.rs`: 核心共享逻辑。
-   `src/types/src/modules/evaluator.rs`: 评估器相关类型定义。
-   `src/types/src/modules/strategy/strategy.rs`: StrategyProvider 枚举定义（PYCODE, LLM）。

## 7. 代码示例 (Code Examples)

### Start Handler (过滤 LLM 类型节点)
```rust
// Evaluator manager only handles EVALUATION stage with LLM provider
let strategy_provider = match &node_info.stage {
    WorkflowStageType::EVALUATION(providers) => {
        match providers.as_ref().and_then(|v| v.first()) {
            Some(WorkflowStageWrapper::Strategy(provider)) => {
                // Check if it's LLM provider (not PYCODE)
                if *provider != StrategyProvider::LLM {
                    // Skip: This is Strategy Runner's responsibility
                    return Err(...);
                }
                provider.clone()
            }
            _ => return Err(...),
        }
    }
    _ => return Err(...),
};
```

### Workflow Node 配置示例
```json
{
  "id": 1,
  "name": "AI Evaluator",
  "stage": "LLM@EVALUATION",
  "config": {
    "model": "gemini-2.0-flash-exp",
    "temperature": 0.7
  }
}
```
