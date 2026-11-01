# Datafeed

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

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
the published topic. Delivery semantics belong to [Messager](../messager/overview.md).

## Extension Points

| Role | Contract | Examples |
|---|---|---|
| Service root | `SigbotDatafeedIngestor` | Component startup and Provider registration |
| Source contract | `IDatafeed` | `BinanceDatafeed`, `TwitterDatafeed`, `TruthSocialDatafeed`, policy sources |
| Persistence boundary | `PersistedDatafeedIngestor` | Canonical facts and Outbox |
| Normalization | `NewsNormalizer` | Provider-to-canonical news mapping |
| Query runtime | `MarketQueryService`, `ComputeExecutor` | LATEST/AS_OF and bounded compute |

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

## Source Map

- Datafeed component and providers: `src/component/datafeed/`
- Datafeed repositories: `src/core/src/modules/datafeed/storage/`
- Feed and message types: `src/types/src/datafeed/`
- Messager SDK: `src/messager/`
