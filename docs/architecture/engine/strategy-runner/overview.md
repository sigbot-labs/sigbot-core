# Strategy Runner

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Implemented deterministic Python Effects runtime with transactional
state and sandboxing; distributed incremental/model sharding remains open.

`SigbotStrategyRunner` is the deterministic decision boundary of one strategy
pipeline. It joins normalized market data, the latest valid AI evaluation, and
the current portfolio snapshot, runs versioned Python strategy code, and
returns typed `Effects { intents, requests, timers }`; validated `TradeIntent`
items are published on the StrategySignal topic. It does not call an exchange.

## Deterministic Execution Context

For each decision event, the runner assembles one immutable
SDK `StrategyContext`:

```text
env / tenant_id / workflow_id / node_id / run_id
decision_time
market_window + terminal_feed_event_id
eval_snapshot + evaluation_id
portfolio_snapshot + portfolio_version
strategy_code_version
validated_hyperparameters
risk_limits
```

The evaluation is eligible only when it was published and effective at the
decision time, its input watermark does not look beyond the terminal market
event, and it has not expired. Hyperparameters are validated against the
strategy's versioned schema before execution. The portfolio snapshot must meet
the strategy's freshness and account requirements.

The same context and runtime version must yield the same output. Randomness,
current wall time, network access, and hidden mutable process state are absent
or explicitly seeded and recorded.

## Decision Result

`Effects` contains bounded `TradeIntent`, service request, and timer lists. A
`TradeIntent` carries instrument, side, quantity/exposure, order constraints,
execution mode, and the immutable input/version references that caused it.

The runner emits no opening intent when:

- evaluation regime is `UNKNOWN` or confidence is below policy;
- the evaluation or portfolio snapshot is missing, stale, future-dated, or
  incompatible;
- recommended parameters fail schema/range validation;
- strategy execution times out, fails validation, or returns an invalid signal.

Only validated intents are published to Order. No-intent decisions may be
retained as state/audit metrics without entering the execution queue.

## Runtime and State

- Controller owns one worker set per strategy pipeline.
- The runner uses a strategy-specific feed consumer group distinct from the
  Evaluator group, plus subscriptions for evaluation and portfolio snapshots.
- Inputs are joined by tenant, pipeline, strategy, account, instrument, event
  time, and version. Late updates cannot rewrite an already published decision.
- Stateful rolling windows have one active owner per partition and checkpoint
  enough state for deterministic recovery.
- Configuration changes create a new generation; an execution uses one
  generation from start to finish.

## Extension Points

| Role | Contract | Implementations |
|---|---|---|
| Service root | `SigbotStrategyRunner` | Component startup |
| Execution engine | `IStrategy` | `PythonStrategy` |
| Node runtime | `StrategyNode` / `StrategyService` | Serial routing and typed input handling |
| State transaction | `CheckpointStrategyTransaction` | Draft, checkpoint and Outbox commit |
| Effect validation | `Effects::validate` plus Order hard-risk checks | Bounded output before publication |

The Python Strategy SDK defines the stable user-code surface. AI code
generation belongs to the external research process; the runtime only executes
an approved, immutable strategy artifact.

## Isolation and Safety Invariants

- Strategy code has explicit CPU, memory, time, filesystem, and network limits.
- Exchange credentials and private trading clients are never available inside
  the strategy sandbox.
- Float values are not used for money, quantity, or price contracts when exact
  decimal semantics are required.
- A strategy cannot weaken hard risk limits supplied by Order Manager policy.
- Every decision is traceable to immutable inputs and a content-addressed code
  artifact.
- One failing strategy is isolated from other pipelines and cannot block the
  shared Datafeed or message event loop.

## Implementation Alignment

`SigbotStrategyRunner` serializes one decision instance and invokes
`on_hyperparameters(ctx, update)` or `on_data(ctx, event)`. The Python SDK
returns `Effects {intents, requests, timers}`; mutable state is a transactional
draft committed atomically with checkpoints and outgoing requests. Market,
evaluation, portfolio, execution, timer, snapshot, compute, notification, and
export results are typed inputs. Strategy publishes intents and cannot call an
exchange.

The Rust host applies process/file/network/time/resource limits. Current
bounded compute can push supported aggregation to PostgreSQL/TimescaleDB, but
Redis hot caching, generic distributed incremental workers, automatic Python
parallelism, and arbitrary model-shard equivalence are not implemented.

## Source Map

- Runner: `src/component/strategy/runner/`
- Python executor and SDK: `src/component/strategy/sdk/`
- Shared strategy types: `src/types/src/strategy/`
