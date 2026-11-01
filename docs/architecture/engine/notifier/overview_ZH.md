# Notifier

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 已实现租户/Workflow 级 Email、Telegram 与站内持久投递；QQ 仍为保留设计。

`SigbotNotifier` 是 Controller 管理的租户/Workflow 组件。它消费类型化 Notification
Request，渲染选定模板，并通过相互隔离的 Channel 投递。它不把 Trading Signal 当作
交易指令，也不拥有 Order 或 Wallet Fact。

## 命名模型

Email、Telegram、QQ 等用户可选投递媒介统一使用 `Channel`：

```text
SigbotNotifier
  -> INotifier
  -> NotificationProviderConfig
       -> EmailNotifier
       -> TelegramNotifier
       -> in-app persistence
```

`Provider` 只保留给一个 Channel 内的多个供应商，例如 Email Channel 下的 SMTP、SES
或 SendGrid。具体投递媒介沿用当前实现导出的 Channel-oriented 名称。

## 输入与路由

Durable Envelope 包含 env/tenant/Workflow/node/run Scope 与 W3C Trace Context。
`NotificationRequest` 只包含 `channel`、`recipient`、`template` 和有界 `variables`，不含
Channel Credential。Envelope Identity 提供幂等，不另建业务关联 ID。

`INotifier` 校验请求 Channel 与 Recipient 已在当前节点配置，并创建对应 Delivery Task。
独立的 Channel Preference/Policy CRUD 尚不是当前 Runtime 子系统。

典型事件包括 Order Rejection/Fill、Liquidation/Margin Warning、Portfolio Threshold、
Pipeline Condition、Backtest Completion 与 System Security Alert。Service 发布事实；
Notifier 拥有呈现与 Channel Routing。

## 投递状态

每个 Task 具有稳定 Delivery ID 与状态：

```text
pending -> sending -> delivered
                  \-> pending（有界重试）
                  \-> rejected
                  \-> unknown
```

daemon 在 Notifier 私有 Store 中保存 Inbox/Deduplication Record、Attempt、Retry
Available Time、Provider Receipt 与终态。Retryable Delivery 使用指数退避且最多五次。
Durable Dead-letter、Expiry Policy 和 Per-channel Rate Limiter 属于后续增强，不是当前行为。

## 扩展点

| 角色 | 契约 | 实现 |
|---|---|---|
| Service Root | `SigbotNotifier` | 作用域 Routing 与 Delivery Runtime |
| Routing | `INotifier` | `NotifierService` |
| 投递媒介 | `NotificationProviderConfig` | `EmailNotifier`、`TelegramNotifier`、站内持久化 |
| Template Rendering | `NotificationTemplate` | 严格版本化 Channel Template |
| Persistence | Core Handler/Repository | `NotifierPostgresRepository`、`NotifierSQLiteRepository` |

## 安全不变量

- Channel Credential 来自 Secret Reference，禁止进入 Message Event、Template、Log
  或 Delivery Receipt。
- Template 针对目标媒介 Escape 不可信内容，并限制 Payload Size/Attachment。
- Deduplication 按 Recipient、Channel 与 Semantic Event 定义；Task Retry 不能产生另一
  条用户可见通知。
- Tenant 不能把请求路由到 Envelope Scope、Workflow Edge 或 Node Recipient Allowlist 之外。
- Retry Waiting 持久化，不能让 Event Consumer 忙循环。

## 实现对齐

`NotifierService` 只消费声明输入节点的类型化请求，严格渲染带版本模板，持久化投递与
站内已读状态，并发布类型化结果。Owner Lease、去重、崩溃恢复、有界 Attempt，以及
Provider 拒绝与未知接受状态的区分均已实现；Email/Telegram 凭据从中央 Secret 引用解析。

尚未以生产收件人验收真实 SMTP/Telegram；第三方未知结果也不能宣称 exactly-once。
QQ 仍为保留设计，未实现。

## 源码地图

- Notification Service 与 Adapter：`src/component/notifier/`
- Notification 类型：`src/types/src/notifier/`
