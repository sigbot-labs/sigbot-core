# Exporter

[中文](exporter_CN.md) · [Architecture overview](../../overview.md)

**Status:** Implemented tenant/Workflow export component for Kafka and Google
Sheets; MQTT/HTTP destinations remain reserved design.

`SigbotExporterServer` is a Controller-managed tenant/Workflow component. It consumes approved,
schema-versioned business events and delivers them to user-configured
downstream systems for analysis, warehousing, and automation. The initial
canonical input is the Order Manager execution stream.

## Boundary and Terminology

`ExportNodeConfig` is user configuration and `ExportSinkConfig` selects the
delivery adapter. One daemon hosts independently isolated target instances:

```text
SigbotExporterServer
  -> IExporter
  -> ExportSinkConfig
       -> KafkaExporter
       -> GoogleSheetsExporter
```

The product name is **Google Sheets**, not “Google Street”. An external MQTT
sink is distinct from the internal Messager transport even if both use MQTT.

## Input and Delivery

An export envelope contains env/tenant/Workflow/node/run scope, business schema
and version, event/available/publication times, W3C trace context, and the approved payload. It
does not expose exchange credentials, private strategy code, or fields excluded
by the target's data policy.

For every configured target, Exporter:

1. validates tenant ownership, event category, schema compatibility, and field
   projection;
2. derives a stable key from envelope scope, run, source node, and sequence;
3. validates an already bounded `ExportBatch` supplied by the producer;
4. writes through the selected sink;
5. stores checkpoint/receipt state before acknowledging durable progress;
6. retries transient failure and dead-letters permanent or exhausted failure.

Kafka is a streaming sink. Google Sheets is a rate-limited batch sink with
stable row identity; it is not used as a transactional system of record.
MQTT and HTTP sinks are reserved extensions, not current implementations.

## Extension Points

| Role                  | Contract                | Implementations                                          |
| --------------------- | ----------------------- | -------------------------------------------------------- |
| Service root          | `SigbotExporterServer`  | Component startup and Provider registration              |
| Routing/orchestration | `IExporter`             | `ExportService`                                          |
| Destination           | `ExportSinkConfig`      | `KafkaExporter`, `GoogleSheetsExporter`                  |
| Persistence           | Core Handler/Repository | `ExporterPostgresRepository`, `ExporterSQLiteRepository` |

Adding a sink does not create a new microservice or subscribe independently to
arbitrary internal topics. The Exporter owns subscription, routing, policy,
checkpointing, and lifecycle once.

## Reliability and Security Invariants

- At-least-once input never creates duplicate target effects when the target
  supports idempotency; otherwise duplicate behavior is explicit and auditable.
- Kafka's bounded producer queue provides backpressure. The current component
  processes one claimed request through its configured sink; per-target worker
  isolation is not yet implemented.
- Batch shape and size incompatibility reject the request; data is never
  silently dropped or reshaped.
- Target credentials use tenant-scoped secret references and are redacted from
  events, logs, and status.
- A future HTTP destination must enforce HTTPS, allowlist/private-network
  policy, timeout, redirect, and response-size limits.
- Replaying a range uses the same delivery key and projection version as live
  delivery.

## Implementation Alignment

`SigbotExporterServer` consumes typed export requests and routes them to Kafka
or Google Sheets sinks. Kafka uses an idempotent producer, `acks=all`, stable
business keys, a bounded producer queue and a durable checkpoint; Google Sheets
uses a fixed-range ValueRange PUT and secret references. Scope, checkpoint,
identity-conflict and restart behavior are persisted. Retry comes from
at-least-once message redelivery with the same business key; there is no
separate durable Exporter retry scheduler.

Kafka's broker ACK and PostgreSQL commit cannot be one transaction, so a crash
can still duplicate a business key. Real Google credentials, Kafka multi-
broker failover, and reserved MQTT/HTTP sinks have not been accepted.

## Google Sheets protocol boundary

Google Sheets uses `reqwest::Client` and the official ValueRange JSON shape for fixed-range values.update PUT, with optional explicit `proxy_url`. It does not require a second SDK/client/factory or an append operation. Credentials are central Secret-map references, never direct business-module environment reads. Fixed range and stable identity must survive retries and replay. There is no independent durable retry scheduler; permanent/exhausted delivery failure remains explicit rather than being reported as success.

## Source Map

- Exporter component and sinks: `src/component/exporter/`
- Exporter repositories: `src/core/src/modules/exporter/storage/`
- Export configuration and event types: `src/types/src/exporter/`
