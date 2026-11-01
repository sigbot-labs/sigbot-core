# Datafeed

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative target architecture with partial provider scaffolding.

`SigbotDatafeed` is the shared market and news ingestion daemon. It owns
external-source sessions, normalization, data-quality checks, and publication
of canonical feed events. Multiple tenants and strategies reuse the same
public upstream connection instead of creating one Datafeed pod per strategy.

## Logical Binding and Physical Sharing

Each `StrategyPipeline` owns a logical `DatafeedBinding`. The daemon converts
all active bindings into reference-counted source workers keyed by a normalized
`feed_key`:

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

`pipeline_id` is intentionally absent. Public market/news feeds can be shared
when all key fields match. Private account streams include credential and
entitlement scope and can never be shared across security boundaries. The last
binding release drains and closes its worker.

Controller creates and releases bindings; it does not create duplicate daemon
instances. Datafeed owns source connection pooling, upstream subscription
diffing, rate limits, retry/backoff, health, and worker reclamation.

## Event Contract

`SigbotFeedEvent` is an immutable normalized envelope. It includes:

- stable event and feed IDs, provider, market, instrument, channel, and schema
  version;
- source event time, ingest time, sequence/watermark, and provenance;
- a typed payload such as trade, order-book update, kline, ticker, funding,
  social/news item, or sentiment evidence;
- quality flags for gaps, duplicates, lateness, correction, and source status.

Raw provider fields are preserved only in an optional versioned extension.
Strategies and evaluators consume the canonical payload and do not depend on a
provider's JSON shape. UTC is the canonical time basis; decimal values preserve
the source precision.

One normalized event is published once to a normal topic. Evaluator and
Strategy Runner create distinct subscription groups so each role receives the
event; replicas within one role may share its group. `$share` is never part of
the published topic. Delivery semantics belong to [Messager](../messager/overview.md).

## Extension Points

| Role | Contract | Examples |
|---|---|---|
| Service root | `SigbotDatafeed` | Shared daemon and worker registry |
| External source | `ISigbotDatafeedSource` | `SigbotBinanceMarketDataSource`, `SigbotTwitterNewsSource`, `SigbotTruthSocialNewsSource` |
| Historical adapter | `ISigbotDatafeedSource` | `SigbotHistoricalReplaySource` composed by Backtest, not by the live daemon |
| Normalization | `ISigbotFeedNormalizer` | Provider- and schema-version-specific normalizers |

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

Current source exposes `SigbotDatafeedIngestor` and
`IDatafeed`, with Binance, Twitter, and TruthSocial provider
scaffolding. Several subscription/process paths are empty or unimplemented,
events are not normalized into the contract above, and publication can retain
unexpanded topic placeholders. Shared source keys, logical bindings,
backpressure, and reliable fan-out are not yet present.

The target public contracts are `SigbotDatafeed` and
`ISigbotDatafeedSource`. Additional sources such as Coinbase or other news
providers are supported only after a concrete adapter and contract tests
exist.

## Source Map

- Datafeed daemon and providers: `src/component/datafeed/`
- Feed and message types: `src/types/src/`
- Messager SDK: `src/messager/`
