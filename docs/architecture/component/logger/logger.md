# Logger

[中文](logger_CN.md) · [Architecture overview](../../overview.md)

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

| Role         | Contract                                             | Examples                                |
| ------------ | ---------------------------------------------------- | --------------------------------------- |
| Service root | `SigbotLogServer`                                    | Component startup and capture lifecycle |
| Processing   | `ILogger`                                            | `S3Logger`                              |
| Capture      | `ArchiveCapture`, `ArchiveTracingLayer`              | Python streams and Rust tracing         |
| Persistence  | `LoggerSQLiteRepository`, `LoggerPostgresRepository` | Durable spool and chunk index           |

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

<a id="rust-log-capture"></a>

## Rust tracing capture and durability

`StrategyNodeConfig.log_archive` enables host Rust tracing capture, sharing
`LogArchive`, SQLite WAL, immutable S3 objects and PostgreSQL cursor indexes
with Python stdout/stderr/exceptions. Logical append never modifies an S3
object: ACK follows a complete local WAL commit, then bounded chunks use
`PutMode::Create`; an existing key requires matching SHA-256 before the index
commits and local payload is released. S3 upload and index completion are
archive completion, not prerequisites for local append ACK.

Core's existing subscriber keeps a reload handle for an empty route layer and
only exposes generic install/remove operations. Reload preserves the original
FilterId. Logger implements `ArchiveTracingLayer`; Strategy composes it. Core
and common-telemetry never depend on Logger. Existing stderr, formatting and
OTel layers remain installed.

### Fixed identity and capture scope

Each capture fixes `env/tenant_id/workflow_id/node_id/run_id` from host runtime
configuration. Event fields cannot override it; there is no global user
context. A second process archive route is rejected to avoid mixing identities.
The first host is Strategy with `log_archive`, capturing INFO/WARN/ERROR targets
`sigbot_` and `common_telemetry`. Third-party targets, other hosts, DEBUG/TRACE,
`sigbot_archive_internal` diagnostics and startup events before installation are
outside this scope.

MQTT handler decoration uses incoming `traceparent` as the OTel consumer span's
parent and propagates it across async polls with `Instrument`. Archive appends
retain that trace. Events without an ambient span are marked `background=true`
and use a standard root context, without a new log correlation ID.

### Durability and backpressure

- Synchronous `on_event` only collects bounded fields, redacts and enqueues in
  memory. Return from a tracing macro is not a durable ACK.
- Defaults are 256 queued records, 1024 bytes per field, 24 fields and 16 KiB
  final content; truncation is explicit.
- `flush_durable()` is a FIFO barrier for all previously accepted records to
  complete SQLite WAL `FULL` commit. Strategy calls it before startup readiness
  and every successful MQTT handler return, with a default five-second timeout.
- A full queue increments rejection counts, fails capture readiness and makes
  subsequent barriers fail. The integrity failure remains until capture rebuild.
  Temporary write failure retains and retries the queue head and can recover.
- Queue rejection or capture task death also fails liveness; temporary WAL
  errors affect readiness only. The task guard exists before spawn, and normal
  shutdown is successful only after the final WAL barrier.
- Capture exposes `accepted/durable/rejected/write_failures`. Internal fault
  diagnostics use a separate fixed target to prevent recursive archiving.
- Shutdown stops MQTT intake, closes admission under a very short synchronous
  lock, then awaits the final barrier. The lock covers admission/enqueue only,
  excluding formatting and async I/O; successful shutdown cannot admit new
  undurable events.

The interval between memory admission and WAL commit is explicitly unconfirmed.
A forced process exit before the barrier cannot be called durable. Restart,
conditional upload, checksum deduplication and cursor guarantees apply only
after WAL commit.

### Sequence allocation and recovery

Rust and Python generated logs use `LogAppend` / `append_generated`. SQLite
`log_sequence` allocates each stream's sequence in the transaction saving the
complete `log_buffer` row, independently of PostgreSQL/S3 availability. Initial
upgrade scans WAL rows and idempotency tombstones for the high watermark,
without renumbering old records.

`append(&DurableMessage<LogLine>)` retains fixed-identity deduplication; explicit
sequences also advance the local watermark. If generated COMMIT confirmation
is lost, retry first checks the tentative key and immutable fact hash with
`BEGIN IMMEDIATE`. An identical committed record deduplicates; an absent key
or another fact at that sequence proves this draft did not commit, so it
allocates again without changing the other fact. Confirmed records never
change sequence. One stream requires the same persistent `spool_path` and
single host owner; independent WAL files cannot allocate sequences for the
same node/run concurrently.

### Field handling and verification

Temporary pairing fields such as `requestSpanId` and `request_span_id` are
discarded, including nested JSON message fields. Unparseable Debug text with
pairing/sensitive assignments is omitted as a whole. Sensitive field names,
Bearer values, URI user information and common credential parameters are
redacted. Arbitrary unlabeled prose has no universal secret detector; callers
must never put credentials in messages.

`sigbot-logger --test tracing_archive` covers field policy, real SQLite
tentative-key races/restart, queue exhaustion and shutdown races with controlled
formatting pauses. The ignored
`actual_tracing_offline_wal_restart_minio_index_and_cursor` exercises actual
Core subscriber install/remove/reinstall, WAL ACK while PG/S3 are offline,
reopen of the same WAL, and actual MinIO/PG checks for hashes, redaction,
traces, cursors, restart sequence and tenant isolation. The `logs` service
scenario requires both Python and Rust records in immutable objects and PG
indexes. Only actual runner results establish execution success.

## Source Map

- Log service: `src/component/logger/`
- Log handler and repositories: `src/core/src/modules/logger/`
- Shared log types: `src/types/src/logger/`
