---
name: evaluator
description: 评估器模块编码指南，负责编排 Multi-Agent 工作流，利用 LLM 动态计算策略超参。
---

# Evaluator Module Guide

## 1. 模块概述
Evaluator 模块作为一个**旁路系统**运行，旨在通过 LLM (Large Language Models) 动态分析市场数据并调整策略超参（如支撑位、阻力位），从而增强静态策略代码（Static Strategy Code）的适应性。它采用异步执行模式，避免阻塞主交易流程，并减少 LLM token 消耗。

## 2. 核心架构

### 实现结构
- **Evaluator Runner** (`SigbotEvaluatorRunner`): 模块入口，负责启动和管理评估器生命周期。
- **Evaluator Factory** (`SigbotEvaluatorFactory`): 负责根据配置构建具体的评估器实例。
- **Default Evaluator** (`SigbotDefaultEvaluator`): 核心实现，采用类似 Google ADK-Go 的 **Loop + Workflow Orchestrator** 架构，负责编排 Multi-Agent 协作。

### 触发机制 (Dual Trigger)
1.  **Cron Job**: 定时触发（如每 1h/4h 执行一次分析）。
2.  **Event Driven**: 特定市场数据规则事件触发。

## 3. Multi-Agent 工作流 (Orchestration Logic)

工作流由 Main Loop 驱动，按顺序调度以下 Agents 协作：

1.  **BootAgent (引导者)**
    -   **职责**: 主动查询并收集最近的市场基础统计数据。
    -   **输出**: **引导信息 (Guidance Info)**。

2.  **LoaderAgent (加载者)**
    -   **输入**: 引导信息。
    -   **职责**: 根据 Skills Prompts 自主调用 **Fetch Tools** 加载近期多种类型的**温数据**（Warm Data，而非实时数据）。
    -   **示例数据**: 过去 1d/4h/1h/30m 的 Kline 数据、Twitter/TruthSocial 新闻等。

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

## 4. 结果分发 (Publishing)
-   **Messager**: 将合规的 **超参** 发布到消息总线 (如 EMQX)。
-   **Notification**: Notification Forwarder 订阅更新，推送到用户通知渠道 (Email/Telegram)。
-   **Strategy Runner**: 订阅超参更新消息，动态应用到运行中的 Python 策略代码。

## 5. 相关文件路径
-   `src/evaluator/**/*.rs`: 评估器模块主代码。
-   `src/core/src/modules/evaluator/**/*.rs`: 核心共享逻辑。
-   `src/types/src/modules/evaluator.rs`: 评估器相关类型定义。
