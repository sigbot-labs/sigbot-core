# Notifier

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative target architecture with partial Email and Telegram
adapters.

`SigbotNotifier` is the shared notification daemon. It consumes typed
operational notification events, resolves tenant/user preferences and policy,
renders channel-specific content, and delivers it through independently
isolated channels. It does not consume a trading signal as an instruction to
trade and does not own order or wallet facts.

## Naming Model

`Channel` is the correct extension term for user-selectable media such as
Email, Telegram, and QQ:

```text
SigbotNotifier
  -> ISigbotNotifier
  -> ISigbotNotificationChannel
       -> SigbotEmailNotifierChannel
       -> SigbotTelegramNotifierChannel
       -> SigbotQqNotificationChannel
```

`Provider` is reserved for multiple vendors behind one channel—for example
SMTP, SES, or SendGrid behind the Email channel. A concrete delivery medium is
therefore not named `EmailNotifierProvider`.

## Input and Routing

`SigbotNotificationEvent` contains event ID, tenant/pipeline/user scope,
category, severity, template ID/version, locale, structured template data,
deduplication key, source/correlation references, and expiry. It contains no
channel credential.

`ISigbotNotifier` resolves the current channel bindings and preferences from an
APIServer-provided configuration snapshot, applies category/severity policy,
and creates one delivery task per selected channel. A failure in one channel
does not block another channel.

Typical events include order rejection/fill, liquidation or margin warning,
portfolio threshold, pipeline condition, backtest completion, and system
security alerts. Services publish facts; Notifier owns presentation and channel
routing.

## Delivery State

Each task has a stable delivery ID and state:

```text
PENDING -> DELIVERING -> DELIVERED
                    \-> RETRY_WAIT -> DELIVERING
                    \-> DEAD_LETTER
                    \-> EXPIRED
```

The daemon persists an inbox/deduplication record, retry schedule, provider
receipt, and redacted terminal error in a Notifier-owned store when durable
delivery is configured. Retries use channel-aware rate limits, exponential
backoff, jitter, and a maximum attempt/dead-letter policy.

## Extension Points

| Role | Contract | Implementations |
|---|---|---|
| Service root | `SigbotNotifier` | Shared routing and delivery daemon |
| Routing | `ISigbotNotifier` | `SigbotPolicyNotifier` |
| Delivery medium | `ISigbotNotificationChannel` | Email, Telegram, QQ channels |
| Vendor inside a medium | Channel-specific provider port | SMTP/SES/SendGrid only when multiple email vendors are needed |
| Template rendering | `ISigbotNotificationRenderer` | Locale- and channel-aware renderer |

## Safety Invariants

- Channel credentials come from secret references and never enter message
  events, templates, logs, or delivery receipts.
- Templates escape untrusted content for the target medium and impose payload
  size/attachment limits.
- Deduplication is scoped to recipient, channel, and semantic event; retrying a
  task does not create another user-visible notification.
- A user or tenant cannot select a channel binding owned by another scope.
- Rate-limit exhaustion delays delivery without blocking the event consumer.
- Expired low-priority notifications are not sent late; critical security
  policy may define separate escalation behavior.

## Implementation Alignment

Current source exposes `SigbotNotifier`,
`INotifier`, and Email/Telegram clients. It consumes raw UTF-8
payloads and broadcasts them to all configured clients. Typed routing,
preferences, idempotency, durable retry/dead-letter state, rate limits, and
receipts are absent. QQ is reserved design and is not currently implemented.

Required alignment is the `SigbotNotifier` root,
`ISigbotNotificationChannel` adapters, typed event contract, policy routing,
and isolated durable delivery lifecycle.

## Source Map

- Notification service and adapters: `src/component/notifier/`
- Notification types: `src/types/src/component/notifier/`
