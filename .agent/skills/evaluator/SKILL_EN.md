---
name: evaluator
description: Evaluator module guide, responsible for orchestrating Multi-Agent workflows and dynamically calculating strategy hyperparameters using LLM.
---

# Evaluator Module Guide

## 1. Module Overview
The Evaluator module runs as a **sidecar system**, designed to dynamically analyze market data and adjust strategy hyperparameters (such as support and resistance levels) using LLMs (Large Language Models). This enhances the adaptability of Static Strategy Code. It adopts an asynchronous execution mode to avoid blocking the main trading flow and to reduce LLM token consumption.

## 2. Core Architecture

### Implementation Structure
- **Evaluator Runner** (`SigbotEvaluatorRunner`): Module entry point, responsible for starting and managing the evaluator lifecycle.
- **Evaluator Factory** (`SigbotEvaluatorFactory`): Responsible for building concrete evaluator instances based on configuration.
- **Default Evaluator** (`SigbotDefaultEvaluator`): Core implementation, adopting a **Loop + Workflow Orchestrator** architecture similar to Google ADK-Go, responsible for orchestrating Multi-Agent collaboration.

### Trigger Mechanism (Dual Trigger)
1.  **Cron Job**: Scheduled triggering (e.g., executing analysis every 1h/4h).
2.  **Event Driven**: Triggered by specific market data rule events.

## 3. Multi-Agent Workflow (Orchestration Logic)

The workflow is driven by the Main Loop, scheduling the following Agents to collaborate in sequence:

1.  **BootAgent (Guide)**
    -   **Responsibility**: Proactively query and collect recent market basic statistical data.
    -   **Output**: **Guidance Info**.

2.  **LoaderAgent (Loader)**
    -   **Input**: Guidance Info.
    -   **Responsibility**: Autonomously call **Fetch Tools** based on Skills Prompts to load various types of recent **Warm Data** (not real-time data).
    -   **Example Data**: Kline data, Twitter/TruthSocial news from the past 1d/4h/1h/30m.

3.  **AlphaAgent (Analyst)**
    -   **Input**: Warm Data.
    -   **Responsibility**: Analyze data based on Super Analysis Prompts and autonomously call **Math Tools** to precisely calculate **Strategy Hyperparameters**.
    -   **Example Calculations**: Dynamic support/resistance levels, RSI thresholds, etc.

4.  **AuditorAgent (Auditor)**
    -   **Input**: Proposed Hyperparameters.
    -   **Responsibility**: Perform compliance boundary checks based on Risk Prompts.
    -   **Logic**:
        -   If check passes: Output compliant hyperparameters.
        -   If check fails (e.g., ETH support level abnormally low): Send back to **AlphaAgent** for recalculation, with failure reasons attached.
        -   **Timeout Mechanism**: Prevents the loop from taking too long, which could cause data invalidation.

## 4. Result Distribution (Publishing)
-   **Messager**: Publishes compliant **Hyperparameters** to the message bus (e.g., EMQX).
-   **Notification**: Notification Forwarder subscribes to updates and pushes them to user notification channels (Email/Telegram).
-   **Strategy Runner**: Subscribes to hyperparameter update messages and dynamically applies them to the running Python strategy code.

## 5. Related File Paths
-   `src/evaluator/**/*.rs`: Main code for the evaluator module.
-   `src/core/src/modules/evaluator/**/*.rs`: Core shared logic.
-   `src/types/src/modules/evaluator.rs`: Evaluator-related type definitions.
