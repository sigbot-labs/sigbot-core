# Strategy Runner

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative target architecture with partial Python execution.

`SigbotStrategyRunner` is the deterministic decision boundary of one strategy
pipeline. It joins normalized market data, the latest valid AI evaluation, and
the current portfolio snapshot, runs versioned Python strategy code, and
publishes the final `SigbotTradeSignal`. It does not call an exchange.

## Deterministic Execution Context

For each decision event, the runner assembles one immutable
`SigbotStrategyExecutionContext`:

```text
tenant_id / pipeline_id / strategy_id
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

`SigbotTradeSignal` contains signal ID, action (`BUY`, `SELL`, `CLOSE`, or
`NONE`), instrument, target quantity or exposure, optional order constraints,
and references to the feed event, evaluation, portfolio, strategy, and risk
versions that caused it.

The runner emits a typed `NONE` decision and opens no new position when:

- evaluation regime is `UNKNOWN` or confidence is below policy;
- the evaluation or portfolio snapshot is missing, stale, future-dated, or
  incompatible;
- recommended parameters fail schema/range validation;
- strategy execution times out, fails validation, or returns an invalid signal.

Only actionable signals are published to Order Manager. `NONE` decisions are
retained as audit/metrics records without entering the execution queue.

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
| Service root | `SigbotStrategyRunner` | Strategy-scoped runtime |
| Execution engine | `IStrategy` | `PythonStrategy`; future sandboxed runtimes require explicit contracts |
| Input join | `ISigbotStrategyContextAssembler` | Event-time and version-aware assembler |
| Signal validation | `ISigbotTradeSignalValidator` | Schema and hard boundary validation before publication |

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

Current source exposes `SigbotStrategyRunner`,
`IStrategy`, and `PythonStrategy`. Its execution
input currently lacks the complete evaluation and portfolio contracts, does
not subscribe to the evaluation stream, and includes a path that calls a
Binance exchange client directly after Python execution. Batch execution is
incomplete.

Required alignment is the three-input deterministic join, `NONE` fail-safe,
version/provenance fields, sandbox guarantees, and publication exclusively to
Order Manager. Rust and other strategy languages remain reserved until a real
executor and compatibility tests exist.

## Source Map

- Runner: `src/component/strategy/runner/`
- Python executor and SDK: `src/component/strategy/sdk/`
- Shared strategy types: `src/types/src/strategy/`
