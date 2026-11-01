# Messager

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** The durable v2 runtime uses MQTT/EMQX and PostgreSQL Inbox/Outbox.
Local transport remains a single-process compatibility adapter and is not a
durability-equivalent financial transport.

Messager is SigBot's internal asynchronous communication SDK. It is not a
business service and owns no trading payload. Distributed services use MQTT
through EMQX and program only against `IMessager`; each domain owns its
transactional Inbox/Outbox boundary.

## Async Port

```text
IMessager
  async publish(topic, message)
  async publish_confirmed(topic, message)  // broker PUBACK boundary
  async subscribe_scoped(topic, consumer, handler)
  async unsubscribe_scoped(topic, consumer)
```

`SubscriptionSet` owns named subscription cancellation. A callback succeeds
only after the caller has durably committed its processing result; the MQTT
transport then acknowledges the delivery.

## Topic and Consumer-group Semantics

Canonical internal topics follow:

```text
internal/v2/{env}/{tenant_id}/workflow/{workflow_id}/{workflow_suffix}
internal/v2/{env}/{tenant_id}/account/{account_id}/{account_suffix}
```

The single generator exposes 17 Workflow suffixes and 5 Account suffixes.
Published topics never contain `$share`; subscriber grouping is a broker
filter concern. The envelope and topic scope must match, and Workflow edges
authorize source/target delivery in addition to broker ACLs.

## Envelope

`MessageEnvelope<T>` is the wire DTO and `DurableMessage<T>` is the only form
that may be persisted. They contain schema version, env/tenant scope,
Workflow/Account ID, source/target node, `run_id`, seq, attempt, event/
available/produced times, and W3C `traceparent`. No separate correlation ID is
created. `requestSpanId` exists only on the wire for bounded request/reply
pairing and is deliberately absent from `DurableMessage`.

The producer owns `T` and its compatibility policy:

- Datafeed owns `DatafeedEvent`.
- Evaluator owns `HyperparametersUpdate`.
- Strategy Runner owns `TradeIntent` payloads published on StrategySignal topics.
- Order owns `ExecutionFact` and `OrderStatusFact`.
- Wallet owns the versioned portfolio payload schema.

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

- Topic authorization enforces env/tenant/Workflow or Account scope at both publisher and
  subscriber boundaries.
- MQTT uses TLS and scoped credentials in distributed environments.
- Payload size, decompression, schema, and deserialization are validated before
  invoking a handler.
- Trace/log metadata is redacted and never includes payload secrets.
- Wildcard subscriptions require explicit service policy; user-provided topic
  filters cannot escape their scope.

## Implementation Alignment

MQTT uses QoS 1, non-clean start, a seven-day session expiry, successful SUBACK
before readiness, manual consumer ACK, and PUBACK before Outbox sent state.
PostgreSQL keys include scope, owner, node, run, and sequence. Explicit replay
retains business identity, increments `attempt_no`, and creates a new producer
span. Named handlers on one topic can unsubscribe independently; unacceptable
but well-formed inputs may be durably quarantined before ACK.

Open gaps are broker process/disk disaster recovery, sustained-load
backpressure acceptance, and role-level production ACL hardening. Local mode
does not provide the persistent-session/replay guarantees above.

## Source Map

- Messager SDK and clients: `src/messager/`
- Topic constants and message types: `src/types/src/messager/`
