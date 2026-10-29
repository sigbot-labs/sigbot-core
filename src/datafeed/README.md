# Sigbot Datafeed

The existing Datafeed factory starts `BINANCE`, `TWITTER`, `TRUTHSOCIAL`, `FED` and `TREASURY` providers. Each normalized fact enters the same PostgreSQL ingestor transaction: source fact, canonical delivery identity, Workflow checkpoint and shared Outbox. Retries retain the first observed availability and wire identity. Different sources and explicit revisions receive distinct message sequences even if their external sequence values coincide.

Twitter and TruthSocial support configured REST, MCP and A2A boundaries. Their bounded historical pages are replayed after restart, preserving late arrivals through source identity deduplication. Binance obtains public exchange snapshots; prices are never invented. Snapshot/compute requests use the existing query runtime with event and availability cutoffs.

The policy providers use `feed-rs` for RSS/Atom parsing and these official defaults:

| Provider | Stable source | Official feed |
| --- | --- | --- |
| `FED` | `fed` | [Monetary policy releases](https://www.federalreserve.gov/feeds/press_monetary.xml) |
| `TREASURY` | `treasury` | [Offering announcements](https://www.treasurydirect.gov/TA_WS/securities/announced/rss) |

These endpoints are listed in the [Federal Reserve RSS directory](https://www.federalreserve.gov/feeds/feeds.htm) and [TreasuryDirect RSS directory](https://www.treasurydirect.gov/rss/). Treasury offering announcements are securities announcements, not a claim to include all Treasury press releases.

Policy nodes use the same `env`, `tenant_id`, `space_id`, `workflow_id`, `node_id`, `run_id`, `database_url_env` and `traceparent` properties as other Datafeed nodes. Optional `fed_endpoint` / `treasury_endpoint` override HTTPS endpoints; `fed_poll_interval_ms` / `treasury_poll_interval_ms` default to 300000 and must be 60000–3600000. A response is capped at 2 MiB and 1,000 entries. HTTP timeout is 20 seconds; polling backs off on failure, with no fabricated facts or checkpoint advancement. Publication/update dates come from the feed; availability is measured after collection. Missing dates, future timestamps and entries lacking both source ID and link are rejected.

`revision` defaults to zero for existing DTOs and is omitted from their serialized form. Only explicitly versioned upstream facts may provide a later revision. These RSS sources do not provide integer revision numbers, so changed content reusing an existing source ID is rejected for operator review; collection time is never used to invent a version.

Tests mirror `client/news` and `server`. `policy_news` contains parser contracts and ignored, read-only official HTTPS tests; network refusal remains a failure. `contracts` additionally tests actual PostgreSQL source revisions, canonical retries, late data and bounded aggregation. The E2E runner exposes `rust-policy` and `rust-boundaries`, with `--reuse-build` recording actual test binary hashes.
