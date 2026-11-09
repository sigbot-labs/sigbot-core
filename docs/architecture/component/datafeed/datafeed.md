# Datafeed

[中文](datafeed_CN.md) · [Architecture overview](../../overview.md)

**Status:** Implemented tenant/Workflow data-plane component with persisted
event-time contracts, multiple ingestion transports, and bounded query paths.

`SigbotDatafeedIngestor` is a Controller-managed tenant/Workflow market and news
ingestion component. It owns
external-source sessions, normalization, data-quality checks, and publication
of canonical feed events. The current deployment isolates instances by
tenant/Workflow; future connection pooling must preserve those boundaries.

## Reserved Logical Binding and Physical Sharing

A future source-sharing optimization may give each Workflow a logical source
binding and convert compatible bindings into reference-counted source workers
keyed by a normalized `feed_key`:

```text
feed_key = hash(
  provider,
  credential_scope,
  entitlement_scope,
  market,
  instrument,
  channel,
  interval,
  normalization_version
)
```

`workflow_id` is intentionally absent from this reserved key. Public market/news feeds can be shared
when all key fields match. Private account streams include credential and
entitlement scope and can never be shared across security boundaries. The last
binding release drains and closes its worker.

Controller creates Datafeed nodes from the frozen Workflow snapshot. Within
one instance, Datafeed owns source connection pooling, rate limits,
retry/backoff, health, and bounded history/query state. Cross-Workflow source
sharing is reserved optimization, not current runtime behavior.

## Event Contract

`DatafeedEvent` is an immutable normalized payload carried by the durable
envelope. It contains `source`, stable `source_event_id`, `symbol`, `event_type`,
event and availability times, sequence, source revision, optional normalized
content, and a JSON `raw` payload. Envelope version, tenant/Workflow/run/node
scope, production time, and trace context remain in `DurableMessage`.

Strategies and evaluators consume the normalized fields first. Provider detail
needed by a particular event type remains in `raw`; it is therefore a bounded
compatibility boundary, not yet a fully typed enum for every market/news shape.
UTC is the canonical time basis and decimal parsing must preserve source
precision.

One normalized event is published once to a normal topic. Evaluator and
Strategy Runner create distinct subscription groups so each role receives the
event; replicas within one role may share its group. `$share` is never part of
the published topic. Delivery semantics belong to [Messager](../../messager/messager.md).

## Extension Points

| Role                 | Contract                                | Examples                                                                    |
| -------------------- | --------------------------------------- | --------------------------------------------------------------------------- |
| Service root         | `SigbotDatafeedIngestor`                | Component startup and Provider registration                                 |
| Source contract      | `IDatafeed`                             | `BinanceDatafeed`, `TwitterDatafeed`, `TruthSocialDatafeed`, policy sources |
| Persistence boundary | `PersistedDatafeedIngestor`             | Canonical facts and Outbox                                                  |
| Normalization        | `NewsNormalizer`                        | Provider-to-canonical news mapping                                          |
| Query runtime        | `MarketQueryService`, `ComputeExecutor` | LATEST/AS_OF and bounded compute                                            |

An external source handles provider transport and authentication. A normalizer
owns provider-to-canonical mapping. Neither is allowed to run strategy or order
logic.

## Reliability and Security Invariants

- Reconnect resumes from a provider cursor or emits an explicit gap; silently
  inventing continuity is forbidden.
- Duplicate source messages produce the same deduplication key.
- Per-source backpressure policies are explicit. Lossy coalescing is allowed
  only for declared snapshot streams, never for ordered trades or account
  events.
- Public and private feeds use separate credential, cache, log, and sharing
  boundaries. Events never contain provider secrets.
- One failing source is isolated from unrelated workers; retries honor provider
  rate limits and circuit state.
- Historical archiving, when enabled, writes to a Datafeed-owned market-history
  store, not the core control-plane database.

## Implementation Alignment

`SigbotDatafeedIngestor` and `IDatafeed` publish normalized, versioned events
through the canonical topic generator. Implemented boundaries include
Twitter/X and TruthSocial REST/MCP/A2A ingestion, Federal Reserve and Treasury
RSS, Binance public market events, persisted revisions/watermarks, historical
queries, compute results, and LATEST/AS_OF snapshots. Binance and Hyperliquid
also have explicit active-snapshot mappings.

The remaining gap is breadth: complete liquidation heatmaps,
lending/funding/position-ratio feeds, more venues, and production-scale source
pooling are not implied by the existing adapters.

## Source facts and bounded compute

A source without an explicit revision never gains a fabricated version from fetch time. Treasury RSS currently covers issuance announcements, not every Treasury policy/news item. TruthSocial fixtures are explicitly a Mastodon-compatible subset; no publicly verifiable official OpenAPI has been obtained. X fixtures use the saved official schema snapshot with recorded version/hash, not current real posts.

History/window/group/aggregate/predict queries use bounded plans and persisted facts. Supported aggregation can be pushed to PostgreSQL/TimescaleDB with identical bucket origins and late-revision/future-data checks; prediction requires a registered deterministic model. The current implementation recomputes bounded windows per request. Redis hot caching, distributed incremental state and arbitrary model-shard equivalence remain unimplemented; sum/count merge tests do not establish equivalence for every model.

<a id="active-market-snapshots"></a>

## Active multi-venue market snapshots

`ctx.market.snapshot(query)` creates a Workflow request. The host Datafeed
performs networking through the read-only [Exchange SDK](../order/order.md#exchange-sdk);
the Python worker has no connection or trading credentials. Datafeed alone
persists and publishes market facts.

### Explicit configuration

Set `active_sources` in the Datafeed node's `datafeed_config.properties`. This
JSON value parses as a strict `Vec<PublicMarketSourceConfig>`. Absent or empty
configuration retains the existing PostgreSQL query path.

```yaml
properties:
  snapshot_input_node_ids: "5"
  active_sources: >-
    [{"source":"binance","provider":"binance_spot",
      "symbols":{"HYPEUSDC":"HYPEUSDC"},
      "timeout_ms":5000,"min_request_interval_ms":1000},
     {"source":"hyperliquid","provider":"hyperliquid_spot",
      "symbols":{"HYPEUSDC":"@107"},
      "timeout_ms":5000,"min_request_interval_ms":1000}]
```

Tenant, environment, Workflow, node and run retain the existing runtime scope.
`source` must match the query; `symbols` explicitly maps query instruments to
venue instruments. This example was checked against Binance `exchangeInfo`
and Hyperliquid `spotMeta` on 2026-10-28 for HYPE/USDC. New mappings must verify
the actual base/quote assets; spot, perpetual, wrapped assets and USDT/USDC are
never assumed interchangeable.

Providers are `binance_spot`, `hyperliquid_spot` and
`hyperliquid_perpetual`. Hyperliquid spot uses metadata `@index` identifiers
with its PURR/USDC exception; perpetuals use native coin names. Adapters use
fixed official public endpoints, with no trading keys or arbitrary URLs.
Binance uses the official `binance-sdk`; Hyperliquid uses official `InfoClient`,
`InfoRequest`, HTTP transport and response models. SDK 0.6 omits trade `tid`;
a composed model adds it instead of deduplicating by a potentially zero `hash`.

### Query and time semantics

- With live time and active sources, `LATEST` fetches the latest actual trade
  for every `sources × symbols` pair. Missing configuration/data, failures or
  invalid timing produce `QueryFailure`, without partial success or stale-cache fallback.
- `AS_OF` always queries PostgreSQL with both event and availability cutoffs.
  Virtual time creates no active adapters; the SDK freezes `LATEST` to `AS_OF(clock)`.
- `price` means `last_trade`. Event time is venue trade time; availability is
  the host's first receipt of that fact. Repeated fetches never refresh availability.
- Actual completion time validates `max_age_ms` and `max_skew_ms`. An illiquid
  instrument may fail because its last trade is old; fetch time cannot replace trade time.
- Venues are sampled independently, without an atomic simultaneous-snapshot
  guarantee. Limits are 32 active points, 4 concurrent reads and a 15-second
  total deadline, with per-source intervals and SDK timeouts. Multiple node
  instances sharing a public IP must respect the venue's total quota.

### Persistence and restart

A short transaction first saves Inbox pending, then releases the transaction
before SDK I/O. Success commits `mkt_event`, original delivery mapping, source
checkpoint, `W/feed/market` Outbox, snapshot-result Outbox and Inbox done in one
transaction, before MQTT ACK.

Native trade identity is `{provider}:{venue_symbol}:trade:{id}`, with external
`seq` equal to trade ID and initial `revision=0`; this is not another correlation
identifier. Repeated facts reuse persisted event/availability times and wire
identity. Changed content requires an explicit revision, never overwrite.
The response contains the facts fetched and normalized by this request, not
another concurrent writer's latest row.

Trade checkpoints partition by `market:{source}:{symbol}:last_trade`, determined
by normalized provider and price type. Existing tickers use
`market:{source}:{symbol}`; symbols' trade IDs and ticker millisecond sequences
cannot overwrite each other. Old source-only market checkpoints remain
historical projections; news retains source partitions. Batch facts acquire
row locks in source/symbol/native-identity order; query ordering cannot change
result ordering.

Completed redeliveries reuse durable results without HTTP. A crash during fetch
leaves the request unacknowledged; persistent MQTT redelivery resumes Inbox
pending. Inbox locks and market-fact keys converge concurrent attempts.
`requestSpanId` exists only in temporary reply pairing, never market facts,
Inbox or Outbox.

### Verification boundary

`src/exchange/tests/market/market_test.rs` covers configuration, trade identity,
future time and missing fields; ignored tests read actual public APIs without
trading credentials. `src/component/datafeed/tests/query/market_test.rs` uses
actual PostgreSQL to check AS_OF isolation, no partial writes, duplicates and
restart; a second ignored case also reads both public APIs and verifies the
atomic durable result. Ignored does not mean passed; execution evidence belongs
to the unified runner. This feature implements no depth matching, liquidity
commitment, liquidation heatmap or derivative indicator.

Protocol references: [Binance public market endpoints](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only),
[Hyperliquid Info and instrument rules](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint),
[Hyperliquid Rust SDK](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk).

## Source Map

- Datafeed component and providers: `src/component/datafeed/`
- Datafeed repositories: `src/core/src/modules/datafeed/storage/`
- Feed and message types: `src/types/src/datafeed/`
- Messager SDK: `src/messager/`
