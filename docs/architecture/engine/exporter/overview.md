# Exporter

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative target architecture with placeholder Google Sheets and
Kafka writers.

`SigbotExporter` is the shared export daemon. It consumes approved,
schema-versioned business events and delivers them to user-configured
downstream systems for analysis, warehousing, and automation. The initial
canonical input is the Order Manager execution stream.

## Boundary and Terminology

An `ExportTarget` is user configuration; an `ISigbotExportSink` is the adapter
that performs delivery. One daemon hosts many independently isolated target
instances:

```text
SigbotExporter
  -> ISigbotExporter
  -> ISigbotExportSink
       -> SigbotKafkaExportSink
       -> SigbotMqttExportSink
       -> SigbotHttpExportSink
       -> SigbotGoogleSheetsExportSink
```

The product name is **Google Sheets**, not “Google Street”. An external MQTT
sink is distinct from the internal Messager transport even if both use MQTT.

## Input and Delivery

An export envelope contains event ID, tenant/pipeline scope, business schema
and version, event/publication times, causation, and the approved payload. It
does not expose exchange credentials, private strategy code, or fields excluded
by the target's data policy.

For every configured target, Exporter:

1. validates tenant ownership, event category, schema compatibility, and field
   projection;
2. derives a stable `{target_id, event_id}` delivery key;
3. buffers or batches according to sink semantics and user policy;
4. writes through the selected sink without blocking the message callback;
5. stores checkpoint/receipt state before acknowledging durable progress;
6. retries transient failure and dead-letters permanent or exhausted failure.

Kafka and MQTT are streaming sinks. HTTP supports idempotency keys and signed
requests. Google Sheets is a rate-limited batch sink with stable row identity;
it is not used as a transactional system of record.

## Extension Points

| Role | Contract | Implementations |
|---|---|---|
| Service root | `SigbotExporter` | Shared subscription and target supervisor |
| Routing/orchestration | `ISigbotExporter` | `SigbotPolicyExporter` |
| Destination | `ISigbotExportSink` | Kafka, MQTT, HTTP, Google Sheets sinks |
| Projection | `ISigbotExportTransformer` | Versioned event-to-target schema mapping |

Adding a sink does not create a new microservice or subscribe independently to
arbitrary internal topics. The Exporter owns subscription, routing, policy,
checkpointing, and lifecycle once.

## Reliability and Security Invariants

- At-least-once input never creates duplicate target effects when the target
  supports idempotency; otherwise duplicate behavior is explicit and auditable.
- Backpressure and bounded queues isolate a slow target from unrelated targets
  and the trading path.
- Schema incompatibility stops that target and surfaces a condition; data is
  never silently dropped or reshaped.
- Target credentials use tenant-scoped secret references and are redacted from
  events, logs, and status.
- HTTP destinations enforce HTTPS, allowlist/private-network policy, timeout,
  redirect, and response-size limits to prevent SSRF and resource exhaustion.
- Replaying a range uses the same delivery key and projection version as live
  delivery.

## Implementation Alignment

Current source exposes `SigbotExporterServer`,
`IExporter`, Google Sheets and Kafka managers, and subscription to
the placed-trade topic. The writers currently log intended writes rather than
performing real delivery. MQTT and HTTP sinks, typed target routing,
idempotency, checkpointing, retry/dead-letter state, and isolation are not yet
implemented. Some source filenames and aliases still use “googlestreet”.

Required alignment is the `SigbotExporter` root, one subscription supervisor,
`ISigbotExportSink` adapters, correct Google Sheets naming, and durable
per-target delivery semantics.

## Source Map

- Exporter service and managers: `src/component/exporter/`
- Export configuration and event types: `src/types/src/`
