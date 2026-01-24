---
name: evaluator
description: Evaluator module coding guide, responsible for orchestrating Multi-Agent workflows, using LLM to dynamically calculate strategy hyperparameters.
---

# Evaluator Module Guide

## 1. Module Design Core

- Design Overview: Evaluator module is designed as a **sidecar service** (asynchronous sidecar on-demand execution, rather than synchronous reduction of LLM token consumption), it is only the underlying execution engine of Workflow (created by users in UI freely dragged and dropped) stage=WorkflowStageType.EVALUATION[LLM] type nodes;

- Trigger Execution
  - Event-Driven: From messager(e.g emqx) subscribe to Workflow Input Stage's like Twitter/TruthSocial Datefeed nodes published market data, when accumulated to a certain threshold, it will trigger execution of multi agents orchestrator based on user-defined assistant prompt
  - Time-Driven: Start cron job定时(默认30s)dynamic scan timescaledb's t_news_twitter/t_news_truthsocial/t_market_kline etc table latest (e.g 4h) data, and trigger execution of multi agents orchestrator based on user-defined assistant prompt

- Execution Core: The multi-agent orchestrator will analyze and execute dynamically extracted market data through multiple agents to generate qualified strategy hyperparameters (such as price support/resistance levels and the weight of bullish/bearish news), thus enabling the static strategy code to dynamically adapt to market conditions.

## 2. Vs Strategy Runner Responsibilities

**Key Design Logic Contrast**: In SigBot architecture, `EVALUATION` stage nodes are handled by two different microservices, according to `StrategyProvider` type:

- **Strategy Runner Microservice** (`src/strategy/runner/`)
  - **Responsibility**: Asynchronously execute workflow's `EVALUATION` stage nodes of **PYCODE type**
  - **Execution Content**: Static Python strategy code
  - **Provider**: `StrategyProvider::PYCODE`
  - **Output**: Trading signals
  - **Features**: Execute pre-defined strategy logic, performance efficient

- **Evaluator Manager Microservice** (`src/evaluator/`)
  - **Responsibility**: Asynchronously execute workflow's `EVALUATION` stage nodes of **LLM type**
  - **Execution Content**: Multi-Agent LLM workflow
  - **Provider**: `StrategyProvider::LLM`
  - **Output**: Strategy hyperparameters
  - **Features**: Utilize LLM dynamic calculation, adaptability strong

**Architecture Integration**: Both microservices scan `t_workflow` table through `WorkflowManager`, automatically routing to corresponding microservices based on node `stage` and `provider` types.

## 3. Core Architecture

### Implementation Structure
- **Evaluator Manager** (`SigbotEvaluatorManager`): Module entry, responsible for starting and managing evaluator lifecycle, integrating WorkflowManager.
- **Evaluation Executor Factory** (`SigbotEvaluationExecutorFactory`): Create and manage executor instances based on workflow nodes.
- **Evaluation Executor** (`SigbotEvaluationExecutor`): Core implementation, it's simliar Google ADK-Go **Loop + Workflow Orchestrator** architecture, responsible for orchestrating Multi-Agent collaboration.
- **Generic Multi-Agent Orchestrator** (`GenericMultiAgentOrchestrator`): Generic Multi-Agent orchestrator, supports parallel and sequential execution.

### Trigger Mechanism (Dual Trigger)
1.  **Time-Driven (WorkflowManager)**: WorkflowManager it's timing scanning `t_workflow` table, automatically starting LLM type evaluation nodes.
2.  **Event-Driven (Messager)**: Subscribing messager events, supporting manual triggering of evaluation.

## 4. Multi-Agent Workflow (Orchestration Logic)

The workflow is driven by Orchestrator, divided into parallel and sequential two stages:

### Parallel Stage - Datafeed Agents
1.  **BootAgent (Boot Agent)**
    -   **Responsibility**: Actively query and collect recent market basic statistics.
    -   **Output**: **Guidance Info**.

2.  **LoaderAgent (Loader Agent)**
    -   **Input**: Guidance Info.
    -   **Responsibility**:根据 Skills Prompts 自主调用 **Fetch Tools** 加载近期多种类型的**温数据**（Warm Data，而非实时数据）。
    -   **Example Data**: Past 1d/4h/1h/30m Kline data, Twitter/TruthSocial news, etc.

### Sequential Stage - Strategy Agents with Loop
3.  **AlphaAgent (Alpha Agent)**
    -   **Input**: Warm data.
    -   **Responsibility**: 根据 Super Analysis Prompts 分析 data, and autonomously call **Math Tools** to precisely calculate **strategy hyperparameters**.
    -   **Example Calculation**: Dynamic support/resistance levels, RSI thresholds, etc.

4.  **AuditorAgent (Auditor Agent)**
    -   **Input**: Proposed hyperparameters.
    -   **Responsibility**: 根据 Risk Prompts 进行合规边界检查。
    -   **Logic**:
        -   If passed: output compliant hyperparameters.
        -   If failed (e.g. ETH support level too low): return to **AlphaAgent** for recalculation,附带失败原因。
        -   **Timeout Mechanism**: Prevents loop back and ensures data validity.

## 5. Result Distribution (Publishing)
-   **Messager**: Publish compliant **hyperparameters** to message bus (e.g. EMQX).
-   **Notification**: Notification Forwarder subscribes to updates and pushes to user notification channels (Email/Telegram).
-   **Strategy Runner**: Subscribes to hyperparameter update messages and dynamically applies to running Python strategy code.

## 6. Related File Paths
-   `src/evaluator/**/*.rs`: Evaluator module main code.
-   `src/evaluator/src/evaluator_manager.rs`: Evaluator Manager main entry, integrates WorkflowManager.
-   `src/evaluator/src/core/orchestrator.rs`: Generic Multi-Agent orchestrator.
-   `src/evaluator/src/agents/**/*.rs`: Each Agent implementation (BootAgent, LoaderAgent, AlphaAgent, AuditorAgent).
-   `src/core/src/modules/evaluator/**/*.rs`: Core shared logic.
-   `src/types/src/modules/evaluator.rs`: Evaluator related type definitions.
-   `src/types/src/modules/strategy/strategy.rs`: StrategyProvider enum definition (PYCODE, LLM).

## 7. Code Examples

### Start Handler (Filter LLM nodes)
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

### Workflow Node Configuration Example
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
