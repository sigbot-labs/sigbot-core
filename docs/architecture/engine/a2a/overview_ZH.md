# A2A 协议出口

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 可选、已实现的 Workflow 控制面协议出口。

API 进程通过 Linux Foundation `a2a-lf` 与 `a2a-server-lf` 提供 Agent Card、JSON-RPC、
流式响应和协议错误。A2A 不是 Workflow 计算节点，也不拥有业务状态。

## 能力与边界

`WorkflowA2aApi` 只发布并执行以下已实现能力：

- 查询租户内的 Workflow 概览；
- 按 Workflow 查询有序事件；
- 幂等请求启动或停止 Workflow Run。

执行器调用 `src/core/src/modules/workflow/storage/` 的 Repository。它不直接写 SQL、访问组件私有表、
注入任意节点、调用 Exchange，或把模型响应转成订单。交易仍只能经过 Strategy、Order、Wallet 的正常边界。

## 身份与生命周期

- `/a2a` 的每个请求先由 Authguard 校验，可信 tenant/principal 仅作为请求内 Header 传给官方 A2A Handler。
- Agent Card 的 Security Scheme 声明 Authguard；Helm Gateway 保护 `/a2a`，公开
  `/.well-known/agent-card.json` 用于协议发现。
- A2A Provider 与 HTTP/MCP 共用同一个 `ApplicationContext`，进程关闭时停止监听并等待任务退出。
- 配置位于 `services.api.a2a`；`public_url` 是 Agent Card 对外公布的 JSON-RPC 地址。

## 可靠性规则

- 只接受类型化 JSON command，未知字段和未声明 operation 直接拒绝。
- Repository 保持 tenant、env、workflow 的所有权约束与幂等规则。
- 服务端错误对客户端脱敏，完整原因只进入结构化日志和 Trace。
- A2A Task ID 只属于协议任务；业务重试和恢复继续使用 `run_id`，不另建业务关联 ID。

## 源码地图

- 协议契约：`src/api/a2a/src/a2a.rs`
- Agent Card、Authguard、Workflow Executor：`src/api/a2a/src/a2a/workflow.rs`
- API 进程装配：`src/cmd/src/startup/api.rs`
- Helm 端口与路由：`deploy/helm/sigbot/templates/`
