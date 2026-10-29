# Backtest runtime

`runtime_v2=true` starts the deterministic execution adapter through the existing Backtest service. `replay_v2=true` additionally enables the finite historical coordinator using `replay_config`. It publishes historical inputs through the real shared Outbox and MQTT transport; Strategy, Order and Wallet retain their normal transactions. No LLM request is made during replay: recorded evaluation snapshots are read at their actual availability time.

Replay requires a separate `replay`, `backtest`, or `replay-*` environment, a separate run with `workflow_run.replay_of` pointing to the source run, an independently funded wallet, and Simulated execution configuration. Migration 7 stores actual evaluation availability, frozen-history boundaries, timers and replay checkpoints; migration 10 preserves the current admitted message across a restart. Its original node/run/sequence is reused. The coordinator holds a dedicated PostgreSQL advisory-lock session and fails if that session is lost.

Configure Strategy and Order with `runtime_config.virtual_clock=true`, Order with the Simulated adapter, and Backtest with `matching_config.virtual_clock=true`. Configure the query-only Datafeed service with `query_only=true` and `clock_mode=virtual`; this creates no external feed clients. These services read the coordinator's persisted clock. SDK `LATEST` snapshots become `AS_OF(ctx.now)` before commit. Historical facts enter the isolated market domain only through `ReplayIngestor`, which checks source run, source identity, revision, availability and the frozen recording cutoff. Historical pages use SQL keyset pagination and bounded records, never a complete-history load. Optional `warmup_from` and `warmup_limit` import only older market facts before the first decision; batching is confined to this warmup and bounded computation.

At the same timestamp the order is recorded evaluation, timer, market, then news. Timer state and decision state commit atomically. A timer uses an existing workflow sequence and enters the same `on_data` callback through MQTT. Every admitted input waits for the configured recipient Inbox commits and their recursively produced messages before the clock advances. The barrier takes a repeatable-read snapshot; broker PUBACK alone cannot satisfy it. Unknown order outcomes, quarantined inputs, missing recipients or an expired barrier stop progress.

`replay_config` composes:

```json
{
  "identity":{"scope":{"env":"replay-case","tenant_id":1,"space_id":1},"workflow_id":10002,"node_id":7,"run_id":"replay-run"},
  "history":{"scope":{"env":"paper","tenant_id":1,"space_id":1},"workflow_id":10001,"run_id":"recorded-run","sources":["binance","twitter","truthsocial"],"from":"2026-10-28T10:00:00Z","until":"2026-10-28T10:01:00Z","frozen_at":"2026-10-28T10:02:00Z"},
  "data_node_id":3,"evaluation_node_id":4,
  "page_size":64,"max_steps":10000,"database_url_env":"SIGBOT_DATABASE_URL",
  "barrier":{"scope":{"env":"replay-case","tenant_id":1,"space_id":1},"run_id":"replay-run","subscriptions":[],"terminal_topics":[],"max_messages":100000,"timeout_seconds":60}
}
```

The empty subscriptions above must be expanded for the actual Workflow. Each subscription contains `producer_node_id`, `target_node_id`, an exact v2 topic, the exact `consumer` below, and optional `payload_kind` (`any`, `execution`, `funding`, `order_status`). A topic can have several recipients. Source feed and recorded evaluation topics must route to the Strategy node. Snapshot/compute requests route to Datafeed and results route back to Strategy. Order signals route to Order, reservation requests to Wallet, reservation results back to Order, and execution facts to both Order and Wallet.

| Participant | Inbox consumer |
| --- | --- |
| Strategy | `strategy.{node}` |
| Datafeed query | `datafeed.query:{env}:{tenant}:{space}:{workflow}:{node}` |
| Order intent | `order:{env}:{tenant}:{space}:{node}` |
| Order reservation result | `order:{env}:{tenant}:{space}:{node}:reservation` |
| Order execution tracker | `order.execution:{env}:{tenant}:{space}:{node}` |
| Wallet reservation | `wallet:{env}:{tenant}:{space}:{account}:{node}.reservation` |
| Wallet execution | `wallet.execution.{node}` |
| Wallet terminal status | `wallet:{env}:{tenant}:{space}:{account}:{node}.status` |

Use `payload_kind=order_status` for the Wallet order-event route, since an acceptance receipt is not a cumulative venue status. Portfolio snapshots and ordinary receipt events may be declared terminal outputs when no Strategy account callback consumes them. Query and execution outputs require their real consumers. The replay scenario must verify those routes, actual fills, balanced postings and equal results across repeated isolated runs before claiming complete replay acceptance.

Migration 16 persists each simulated order's original message, matching owner and frozen instrument/fee/slippage settings. Market and immediately crossing Limit orders fill at acceptance. Resting Limit orders match subsequent persisted market ticks in acceptance/node/sequence/account order. A late tick whose event time predates acceptance cannot fill the order. Replay invokes matching in the same transaction that admits history and its clock; the barrier waits for the resulting execution consumers. Paper mode can subscribe through `properties.matching_feed_config={"workflow_id":100,"run_id":"paper-run","input_node_ids":[3]}` (JSON string). The deployer replaces these identities from the Workflow graph. Price slippage rounds adversely to the configured tick, and exact gross quote/fees enter Wallet through execution facts. Repeated ticks and restarts cannot produce another fill. This deterministic full-fill model does not model market depth, queue priority, partial liquidity or exchange latency.
