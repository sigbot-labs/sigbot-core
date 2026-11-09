# Notifier

[中文](notifier_CN.md) · [Architecture overview](../../overview.md)

**Status:** Implemented tenant/Workflow delivery component with Email,
Telegram, and in-product persistence; QQ remains reserved design.

`SigbotNotifier` is a Controller-managed tenant/Workflow component. It consumes
typed notification requests, renders the selected template, and delivers it
through independently isolated channels. It does not consume a trading signal
as an instruction to trade and does not own order or wallet facts.

## Naming Model

`Channel` is the correct extension term for user-selectable media such as
Email, Telegram, and QQ:

```text
SigbotNotifier
  -> INotifier
  -> NotificationProviderConfig
       -> EmailNotifier
       -> TelegramNotifier
       -> in-app persistence
```

`Provider` is reserved for multiple vendors behind one channel—for example
SMTP, SES, or SendGrid behind the Email channel. Concrete delivery media use
the channel-oriented names exported by the current implementation.

## Input and Routing

The durable envelope contains env/tenant/Workflow/node/run scope and W3C trace
context. `NotificationRequest` contains only `channel`, `recipient`, `template`,
and bounded template `variables`; it contains no channel credential. The
envelope identity supplies idempotency, and no second business correlation ID
is created.

`INotifier` validates that the requested channel is configured for this node
and creates the corresponding delivery task. Channel preference/policy CRUD is
not yet a separate runtime subsystem.

Typical events include order rejection/fill, liquidation or margin warning,
portfolio threshold, pipeline condition, backtest completion, and system
security alerts. Services publish facts; Notifier owns presentation and channel
routing.

## Delivery State

Each task has a stable delivery ID and state:

```text
pending -> sending -> delivered
                  \-> pending (bounded retry)
                  \-> rejected
                  \-> unknown
```

The daemon persists an inbox/deduplication record, attempt number, retry
availability time, provider receipt, and terminal state in the Notifier-owned
store. Retryable delivery uses exponential backoff and a maximum of five
attempts. A durable dead-letter queue, expiry policy, and per-channel rate
limiter are enhancements rather than current behavior.

## Extension Points

| Role               | Contract                     | Implementations                                          |
| ------------------ | ---------------------------- | -------------------------------------------------------- |
| Service root       | `SigbotNotifier`             | Scoped routing and delivery runtime                      |
| Routing            | `INotifier`                  | `NotifierService`                                        |
| Delivery medium    | `NotificationProviderConfig` | `EmailNotifier`, `TelegramNotifier`, in-app persistence  |
| Template rendering | `NotificationTemplate`       | Strict versioned channel template                        |
| Persistence        | Core Handler/Repository      | `NotifierPostgresRepository`, `NotifierSQLiteRepository` |

## Safety Invariants

- Channel credentials come from secret references and never enter message
  events, templates, logs, or delivery receipts.
- Templates escape untrusted content for the target medium and impose payload
  size/attachment limits.
- Deduplication is scoped to recipient, channel, and semantic event; retrying a
  task does not create another user-visible notification.
- A tenant cannot route a request to a node outside its envelope scope or
  configured Workflow edge.
- Retry waiting is persisted and does not busy-loop the event consumer.

## Implementation Alignment

`NotifierService` consumes typed requests only from declared input nodes,
strictly renders versioned templates, persists delivery/in-product read state,
and emits typed results. Owner leases, deduplication, crash recovery, bounded
attempts, and the distinction between provider rejection and unknown
acceptance are implemented. Email and Telegram credentials are resolved from
central secret references.

Real SMTP/Telegram delivery has not been accepted against production
recipients, and a third-party unknown result cannot be described as exactly
once. QQ remains reserved and is not implemented.

## Source Map

- Notification service and adapters: `src/component/notifier/`
- Notification types: `src/types/src/notifier/`
