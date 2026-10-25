# A2A Adapter

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 可选协议 Adapter；当前 Request Handler 尚不完整。

`SigbotA2aServer` 通过 Agent-to-Agent 协议暴露显式获准的 SigBot Agent Capability。
它是外部互操作 Adapter，不是 Trading Pipeline Stage，也不是另一套 Authorization
Boundary。

## 边界

- Agent Card 只声明已实现且通过 Policy 批准的 Capability 与 Protocol Version。
- 认证 Caller、解析 Tenant/Namespace Scope，并调用与 APIServer 相同的 Application
  Service 与 Policy Decision。
- 把 A2A JSON-RPC Message、Task Lifecycle、Error 与 Streaming Update 转换为内部类型化
  Request，不暴露内部 Message Topic。
- 跨协议边界传递 Correlation、Trace、Idempotency 与 Cancellation ID。

A2A 禁止访问组件私有数据库、注入任意 Pipeline Node、直接调用 Exchange，或把 AI
Response 直接变成 Order。未来若暴露交易动作，仍必须经过合法 Strategy/Order Contract
与正常硬风险边界。

## 扩展点

| 角色 | 契约 | 用途 |
|---|---|---|
| Protocol Root | `SigbotA2aServer` | Discovery 与 JSON-RPC Transport |
| Task Handler | `ISigbotA2aTaskHandler` | 把获准 A2A Skill 映射到 Application Service |
| Agent 实现 | `ISigbotAgent` | 复用获准 Evaluator/Research Agent 行为 |

Protocol Type 与 Authentication 留在 Domain Agent 外部。Handler 必须声明 Input/Output
Schema、Timeout、Cancellation、Idempotency、Side-effect Class 与 Required Permission。

## 安全与可靠性

- 拒绝未注册 Skill 与未声明 Side Effect。
- Dispatch 前校验 JSON-RPC Size、Schema、Content Type、Task Transition 与 Replay Token。
- Stream Backpressure/Cancellation 传递到所属 Task；Client 断连不能泄漏无界工作。
- Agent Card 禁止宣传 Placeholder Capability。
- Error 必须符合协议并脱敏 Prompt、Tool、Credential、私有 Strategy Code 与内部拓扑。

## 实现对齐

当前 `src/agents/a2a/` 已有 Server Builder 与 ADK Type Integration。主要 Handler 仍是
Placeholder，尚未调用完整 Agent Capability，已声明能力也有限。启用外部使用前仍需
完成端到端 Authentication、Scoped Authorization、Durable Task State、Streaming 与
Contract Test。

## 源码地图

- A2A Adapter：`src/agents/a2a/`
- 共享 Agent 抽象：`src/agents/`
- Evaluator Agent：`src/evaluator/src/executor/adk/`
