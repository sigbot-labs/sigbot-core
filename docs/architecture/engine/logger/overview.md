# Logger

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Implemented tenant/Workflow log archive component with durable
local spooling, immutable object chunks, and cursor reads. A live WebSocket
fan-out API is not implemented.

`SigbotLogServer` collects structured operational logs and archives them to a
dedicated observability store. It is outside the trading decision path and
never becomes a dependency for placing an order or committing wallet
accounting.

## Log Contract

`LogLine` contains timestamp, severity, env/tenant/Workflow/node/run
scope, component, instance, W3C trace/span IDs, message, and structured
redacted fields. Producers emit one event per logical record; multi-line text
is represented explicitly rather than parsed back into structure downstream.

The event schema is distinct from audit events and business facts. Security
audit records use their owning audit contract; trade, fill, and ledger state is
queried from its source of truth rather than reconstructed from logs.

## Processing Paths

```text
component logs -> Messager
  -> archive consumer -> durable SQLite WAL
  -> immutable object chunk -> PostgreSQL cursor index
```

The component currently implements only the archive path. A future real-time
fan-out path must use a distinct named subscription and bounded per-client
policy so it receives every eligible event without stealing archive delivery.

## Extension Points

| Role | Contract | Examples |
|---|---|---|
| Service root | `SigbotLogServer` | Component startup and capture lifecycle |
| Processing | `ILogger` | `S3Logger` |
| Capture | `ArchiveCapture`, `ArchiveTracingLayer` | Python streams and Rust tracing |
| Persistence | `LoggerSQLiteRepository`, `LoggerPostgresRepository` | Durable spool and chunk index |

Logger owns its `log_chunk` table domain and object keys. The current final-
state schema may physically co-locate that table with control-plane tables, but
other components do not write it.

## Invariants

- Producers redact credentials, keys, tokens, prompts, private account data,
  and strategy source before publication; the service applies a second policy
  filter.
- Env/tenant/Workflow authorization is enforced when ingesting, querying, and
  streaming logs.
- Archive idempotency is keyed by event ID. Retry cannot create duplicate
  records.
- A slow archive cannot backpressure the trading message loop without a
  bounded, observable policy. The producer-side capture queue is bounded.
- Retention, legal hold, deletion, and export rules are explicit per data class.
- Health and capture statistics expose durable/rejected/write-failure state;
  richer archive lag and live-client metrics remain enhancements.

## Implementation Alignment

The current source registers `ILogger` providers through `SigbotLogServer`.
The component owns capture, WAL buffering, immutable S3 chunks, and cursor
reads; the core `LogHandler` owns the business boundary, while database
operations remain inside Logger repositories. `append` is logical append:
ACK follows a SQLite WAL `FULL` durable commit, then every chunk is created at a
new conditional S3 key. Existing S3 objects are never modified.

Strategy-host Rust tracing and Python stdout/stderr/exception capture use this
path. Capture from every other Rust host, host-volume loss recovery, and
production bucket replication/retention remain open operational work.

## Source Map

- Log service: `src/component/logger/`
- Log handler and repositories: `src/core/src/modules/logger/`
- Shared log types: `src/types/src/logger/`
