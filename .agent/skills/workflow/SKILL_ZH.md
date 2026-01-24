---
name: workflow
description: 工作流编排模块编码指南。相关模块：src/core/src/modules/workflow/**/*.rs (工作流管理器), src/types/src/modules/workflow/**/*.rs (工作流类型定义)
---
您正在开发工作流编排模块 (`SigbotWorkflowManager`)。该模块负责管理用户在 UI 自由拖拽创建的交易工作流，协调各微服务异步执行节点任务。

**架构核心**：
- **全异步事件驱动**：每种节点底层从 MQ 订阅固定 topic 并发布到固定 topic，各微服务独立执行，无同步依赖
- **工作流管理器**：定期扫描 `t_workflow` 表，根据节点 `stage` 和 `provider` 类型路由到对应微服务，管理节点生命周期

**工作流标准结构**：
```
Input Stage (数据源节点) ->
Evaluation Stage (评估节点) ->
Transaction Stage (交易执行节点) ->
Output Stage (外部输出节点) ->
Post Stage (通知节点)
```

- 示例：

```
Input Stage (
    Truthsoical Datafeed Node
    Twitter Datafeed Node
    Binance Datafeed Node
) ->
Analysis Stage (
    AI Evaluator Node (i.e: MAS (ADK multi-agents orchestration based)) ->
    Py Runner Node(i.e: PYCODE, custom strategy) ->
) ->
Transaction Stage (
    Order Executor Node(e.g Binance/Hyperliquid/Polymarket/IBKR) ->
) ->
Output Stage (
    External Writer Node(e.g google street) ->
) ->
Post Stage (
    Notification Node(telegram/..)
)
```

**节点类型与微服务映射**：

1. **Input Stage - Datafeed Nodes**
   - **节点类型**：`WorkflowStageType::INPUT`
   - **可配置**：数据源类型（Twitter/TruthSocial/Binance）、频率、订阅参数
   - **底层微服务**：`DatafeedIngestor` (`src/datafeed/`)
   - **执行逻辑**：扫描 `t_workflow` 提取 datafeed nodes，调用外部 API 获取数据
   - **发布 Topic**：`/{tenantId}/{workflowId}/datafeed/market`

2. **Evaluation Stage - AI Evaluator Nodes**
   - **节点类型**：`WorkflowStageType::EVALUATION(StrategyProvider::LLM)`
   - **可配置**：LLM model、assistant prompt、temperature
   - **底层微服务**：`EvaluatorManager` (`src/evaluator/`)
   - **执行逻辑**：
     - 订阅：`/{tenantId}/{workflowId}/datafeed/market`（市场数据）
     - 调用：Multi-Agent Orchestrator 分析生成策略超参
     - 发布：`/{tenantId}/{workflowId}/trading/alpha`（预交易信号）

3. **Evaluation Stage - Py Runner Nodes**
   - **节点类型**：`WorkflowStageType::EVALUATION(StrategyProvider::PYCODE)`
   - **可配置**：策略代码、自定义参数（如 kline_window_times）
   - **底层微服务**：`StrategyRunner` (`src/strategy/`)
   - **执行逻辑**：
     - 订阅：`/{tenantId}/{workflowId}/trading/alpha`（预交易信号）、`/{tenantId}/{workflowId}/datafeed/market`（市场数据）
     - 调用：执行 Python 策略代码
     - 发布：`/{tenantId}/{workflowId}/trading/signal`（交易信号）

4. **Transaction Stage - Order Executor Nodes**
   - **节点类型**：`WorkflowStageType::TRANSACTION`
   - **可配置**：交易所类型（Binance/Hyperliquid/Polymarket/IBKR）、风控参数（per_position_max/rate）
   - **底层微服务**：
     - `OrderManager` (`src/order/`)：
       - 订阅：`/{tenantId}/{workflowId}/trading/signal`（交易信号）
       - 调用：交易所 SDK 下单
       - 发布：`/{tenantId}/{workflowId}/trading/placed`（下单结果）
     - `WalletManager` (`src/wallet/`)：
       - 订阅：`/{tenantId}/{workflowId}/trading/placed`（下单结果）
       - 调用：事务计算并落库（ledger/position/balance）

5. **Output Stage - External Writer Nodes**
   - **节点类型**：`WorkflowStageType::OUTPUT`
   - **可配置**：输出渠道（Google Sheets/HTTP POST）
   - **底层微服务**：`DataSubscriber` (新增模块)
   - **执行逻辑**：
     - 订阅：`/{tenantId}/{workflowId}/trading/placed`（下单结果）
     - 调用：外部 API 推送数据

6. **Post Stage - Notification Nodes**
   - **节点类型**：`WorkflowStageType::POST`
   - **可配置**：通知渠道（Telegram/Email/WeCom）
   - **底层微服务**：`NotificationForwarder` (`src/notification/`)
   - **执行逻辑**：
     - 订阅：`/{tenantId}/{workflowId}/notify`（通知消息）
     - 调用：通知渠道 API 发送
     - 落库：`t_notification` 表（默认 WebSocket 推送到 Web 端）

**编码指南**：

1. **工作流生命周期管理**：
   - 定期扫描 `t_workflow` 表（默认 30s）
   - 根据 `stage` 和 `provider` 类型路由到对应微服务
   - 支持动态启动/停止工作流节点
   - 维护节点状态（RUNNING/STOPPED/ERROR）

2. **事件驱动架构**：
   - 使用 `ISigbotMessagerClient` 订阅/发布消息
   - Topic 命名规范：`/{tenantId}/{workflowId}/{stage}/{dataType}`
   - 确保消息幂等性和顺序性
   - 处理消息丢失和重试逻辑

3. **节点配置管理**：
   - 支持 JSON 格式节点配置
   - 验证配置完整性和合法性
   - 支持配置热更新
   - 记录配置变更历史

4. **微服务协调**：
   - 各微服务独立扫描 `t_workflow` 表
   - 根据节点类型过滤处理范围
   - 避免重复执行（使用分布式锁）
   - 支持微服务故障隔离

5. **错误处理**：
   - 记录节点执行错误到 `t_workflow_logs`
   - 支持节点级别重试策略
   - 发布错误通知到 `/{tenantId}/{workflowId}/notify`
   - 实现熔断机制防止级联失败

6. **性能优化**：
   - 使用批量扫描减少数据库查询
   - 缓存活跃工作流配置
   - 异步并发处理多个工作流
   - 监控节点执行延迟和吞吐量

7. **测试**：
   - 测试工作流完整执行链路
   - 验证节点间消息传递
   - 测试节点故障恢复
   - 验证配置变更生效

8. **监控与可观测性**：
   - 记录节点执行指标（延迟、成功率）
   - 发布工作流执行事件
   - 支持分布式追踪（Trace ID）
   - 提供工作流执行可视化
