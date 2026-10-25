# Messager

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative embedded SDK architecture. Local and MQTT clients exist,
but their observable subscription behavior is not yet equivalent.

Messager is SigBot's internal asynchronous communication SDK. It is not a
business service and owns no trading payload. All-in-one mode uses a local
in-memory implementation; distributed mode uses MQTT through EMQX. Services
program only against `ISigbotMessagerClient`.

## Async Port

```text
ISigbotMessagerClient
  async publish(topic, envelope, delivery_options)
  async subscribe(SubscriptionSpec, handler) -> SubscriptionHandle
  async close()

SubscriptionSpec
  topic_filter
  consumer_group?
  qos
  ordering_key?
  capacity / backpressure_policy
```

`SubscriptionHandle` owns cancellation and graceful drain. A callback finishes
only when its processing outcome is known; durable transports acknowledge
according to the selected delivery contract.

Implementations are `SigbotLocalMessagerClient` and
`SigbotMqttMessagerClient`. Local mode must model the same fan-out and consumer
group semantics as MQTT, not a simplified single-handler queue that changes
application behavior.

## Topic and Consumer-group Semantics

Canonical internal topics follow:

```text
/internal/v1/{tenant_id}/{pipeline_id}/{domain}/{event}
```

A publisher always uses a normal topic. For MQTT, a consumer group is encoded
only in the subscription filter:

```text
$share/evaluator-{pipeline_id}/internal/v1/.../feed/event
$share/strategy-{pipeline_id}/internal/v1/.../feed/event
```

Evaluator and Strategy Runner require different groups so both receive every
feed event. Replicas of one component share that component's group to divide
work. Stateful consumers additionally use a stable partition/ownership scheme;
random shared-subscription delivery alone cannot preserve rolling windows.

Without a group, every active subscription receives its own copy. With a
group, one active member receives each message. These rules are identical in
Local and MQTT modes.

## Envelope

`SigbotEventEnvelope<T>` contains event ID, tenant ID, pipeline ID, producer,
payload schema/version, event time, publication time, correlation and causation
IDs, partition key, and trace context. It excludes broker credentials and
provider secrets.

The producer owns `T` and its compatibility policy:

- Datafeed owns `SigbotFeedEvent`.
- Evaluator owns `SigbotEvaluationSnapshot`.
- Strategy Runner owns `SigbotTradeSignal`.
- Order Manager owns `SigbotExecutionEvent`.
- Wallet Manager owns `SigbotPortfolioSnapshot`.

Messager owns serialization framing, size limits, subscription lifecycle,
delivery metadata, and transport error mapping—not the business schemas.

## Delivery and Backpressure

- Default processing is at-least-once; consumers are idempotent by event ID and
  domain key.
- Ordering is guaranteed only within an explicitly declared partition and
  transport capability. Global order is not promised.
- Bounded queues are mandatory. `block`, `reject`, `coalesce`, or `drop` policy
  is explicit per event class; money/order facts cannot use lossy policies.
- Retry distinguishes transport failure from handler failure. Poison messages
  reach a bounded retry/dead-letter or operator-visible failure contract.
- Reconnect restores all subscriptions before health becomes ready and exposes
  gaps when the broker cannot replay them.
- Slow handlers execute outside the network event loop and cannot stall
  keepalive or unrelated subscriptions.

## Security Invariants

- Topic authorization enforces tenant/pipeline scope at both publisher and
  subscriber boundaries.
- MQTT uses TLS and scoped credentials in distributed environments.
- Payload size, decompression, schema, and deserialization are validated before
  invoking a handler.
- Trace/log metadata is redacted and never includes payload secrets.
- Wildcard subscriptions require explicit service policy; user-provided topic
  filters cannot escape their scope.

## Implementation Alignment

Current source exposes `ISigbotMessagerClient`, Local and MQTT clients, and
asynchronous publish/subscribe calls. It has no explicit consumer-group model.
Local mode effectively allows one receiver per exact topic and lacks equivalent
fan-out/wildcards; MQTT's event loop can route later subscriptions through the
first handler. Current topic placeholder expansion is inconsistent across
producers and consumers, and the repository does not yet use `$share`.

Required alignment is `SubscriptionSpec`, multiple independent handlers,
consumer groups, equivalent Local/MQTT behavior, canonical topic expansion,
bounded backpressure, cancellation, and contract tests shared by both
implementations.

## Source Map

- Messager SDK and clients: `src/messager/`
- Topic constants and message types: `src/types/src/modules/messager/`
