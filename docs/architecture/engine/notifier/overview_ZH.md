# Notifier

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前已有部分 Email 与 Telegram Adapter。

`SigbotNotifier` 是共享通知 daemon。它消费类型化 Operational Notification Event，
解析 Tenant/User Preference 与 Policy，渲染 Channel-specific 内容，并通过相互隔离的
Channel 投递。它不把 Trading Signal 当作交易指令，也不拥有 Order 或 Wallet Fact。

## 命名模型

Email、Telegram、QQ 等用户可选投递媒介统一使用 `Channel`：

```text
SigbotNotifier
  -> ISigbotNotifier
  -> ISigbotNotificationChannel
       -> SigbotEmailNotifierChannel
       -> SigbotTelegramNotifierChannel
       -> SigbotQqNotificationChannel
```

`Provider` 只保留给一个 Channel 内的多个供应商，例如 Email Channel 下的 SMTP、SES
或 SendGrid。因此具体投递媒介不命名为 `EmailNotifierProvider`。

## 输入与路由

`SigbotNotificationEvent` 包含 Event ID、Tenant/Pipeline/User Scope、Category、
Severity、Template ID/Version、Locale、结构化 Template Data、Deduplication Key、
Source/Correlation Reference 与 Expiry，不包含 Channel Credential。

`ISigbotNotifier` 从 APIServer 提供的 Configuration Snapshot 解析当前 Channel Binding
与 Preference，应用 Category/Severity Policy，并为每个选定 Channel 创建一个
Delivery Task。一个 Channel 失败不能阻塞其他 Channel。

典型事件包括 Order Rejection/Fill、Liquidation/Margin Warning、Portfolio Threshold、
Pipeline Condition、Backtest Completion 与 System Security Alert。Service 发布事实；
Notifier 拥有呈现与 Channel Routing。

## 投递状态

每个 Task 具有稳定 Delivery ID 与状态：

```text
PENDING -> DELIVERING -> DELIVERED
                    \-> RETRY_WAIT -> DELIVERING
                    \-> DEAD_LETTER
                    \-> EXPIRED
```

启用 Durable Delivery 时，daemon 在 Notifier 私有 Store 中保存 Inbox/Deduplication
Record、Retry Schedule、Provider Receipt 与已脱敏 Terminal Error。Retry 使用感知
Channel 的 Rate Limit、Exponential Backoff、Jitter 与 Maximum Attempt/Dead-letter
Policy。

## 扩展点

| 角色 | 契约 | 实现 |
|---|---|---|
| Service Root | `SigbotNotifier` | 共享 Routing 与 Delivery daemon |
| Routing | `ISigbotNotifier` | `SigbotPolicyNotifier` |
| 投递媒介 | `ISigbotNotificationChannel` | Email、Telegram、QQ Channel |
| 媒介内部 Vendor | Channel-specific Provider Port | 仅 Email 需要多个供应商时使用 SMTP/SES/SendGrid |
| Template Rendering | `ISigbotNotificationRenderer` | 感知 Locale 与 Channel 的 Renderer |

## 安全不变量

- Channel Credential 来自 Secret Reference，禁止进入 Message Event、Template、Log
  或 Delivery Receipt。
- Template 针对目标媒介 Escape 不可信内容，并限制 Payload Size/Attachment。
- Deduplication 按 Recipient、Channel 与 Semantic Event 定义；Task Retry 不能产生另一
  条用户可见通知。
- User/Tenant 不能选择其他 Scope 拥有的 Channel Binding。
- Rate-limit Exhaustion 只延迟投递，不能阻塞 Event Consumer。
- 过期低优先级 Notification 不迟到发送；关键 Security Policy 可以定义独立 Escalation。

## 实现对齐

当前源码暴露 `SigbotNotifier`、`INotifier` 及 Email/
Telegram Client。它消费 Raw UTF-8 Payload 并广播到所有已配置 Client。Typed Routing、
Preference、Idempotency、Durable Retry/Dead-letter State、Rate Limit 与 Receipt 均未
实现。QQ 属于保留设计，当前没有实现。

必须对齐到 `SigbotNotifier` Root、`ISigbotNotificationChannel` Adapter、类型化 Event
Contract、Policy Routing 与相互隔离的 Durable Delivery Lifecycle。

## 源码地图

- Notification Service 与 Adapter：`src/component/notifier/`
- Notification 类型：`src/types/src/component/notifier/`
