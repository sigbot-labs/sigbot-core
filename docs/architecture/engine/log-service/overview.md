# Log Service

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Auxiliary observability service with partial real-time and archive
paths.

`SigbotLogService` collects structured operational logs, fans them out to
authorized real-time clients, and archives them to a dedicated observability
store. It is outside the trading decision path and never becomes a dependency
for placing an order or committing wallet accounting.

## Log Contract

`SigbotLogEvent` contains event ID, timestamp, severity, tenant and pipeline
scope, component, instance, trace/correlation IDs, message, and structured
redacted fields. Producers emit one event per logical record; multi-line text
is represented explicitly rather than parsed back into structure downstream.

The event schema is distinct from audit events and business facts. Security
audit records use their owning audit contract; trade, fill, and ledger state is
queried from its source of truth rather than reconstructed from logs.

## Processing Paths

```text
component logs -> Messager
  -> real-time consumer -> bounded per-scope stream -> WebSocket/API clients
  -> archive consumer group -> batch writer -> observability store
```

Real-time fan-out may drop/coalesce low-priority records for a slow client after
surfacing a gap marker. The archive path uses a distinct consumer group and
durable checkpoint; the two paths must each receive the event rather than share
one group.

## Extension Points

| Role | Contract | Examples |
|---|---|---|
| Service root | `SigbotLogService` | Real-time and archive supervisor |
| Processing | `ILogservice` | `DefaultLogservice` |
| Archive | `ISigbotLogArchive` | PostgreSQL/TimescaleDB, object storage, or a log backend selected explicitly |
| Live stream | `ISigbotLogStream` | Bounded WebSocket fan-out |

The observability store is owned by Log Service. It is not the core
control-plane database. APIServer may authenticate/proxy log queries but other
components do not share the archive tables.

## Invariants

- Producers redact credentials, keys, tokens, prompts, private account data,
  and strategy source before publication; the service applies a second policy
  filter.
- Tenant/pipeline authorization is enforced when ingesting, querying, and
  streaming logs.
- Archive idempotency is keyed by event ID. Retry cannot create duplicate
  records.
- A slow archive or WebSocket client cannot backpressure the trading message
  loop without a bounded, observable policy.
- Retention, legal hold, deletion, and export rules are explicit per data class.
- Health and metrics expose lag, drops, gap markers, archive failures, and
  connected clients without logging payload content.

## Implementation Alignment

The current source registers `ILogservice` providers through `SigbotLogServer`.
The component owns capture, WAL buffering, immutable S3 chunks, and cursor
reads; the core `LogserviceHandler` owns the business boundary, while database
operations remain inside Logservice repositories. `append` is logical append:
ACK follows a SQLite WAL `FULL` durable commit, then every chunk is created at a
new conditional S3 key. Existing S3 objects are never modified.

## Source Map

- Log service: `src/component/logservice/`
- Log handler and repositories: `src/core/src/modules/logservice/`
- Shared log types: `src/types/src/logservice/`
