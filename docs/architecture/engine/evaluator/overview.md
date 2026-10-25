# Evaluator

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative target architecture with a partial multi-agent executor.

`SigbotEvaluator` is the non-deterministic side path of one strategy pipeline.
It observes normalized market/news events, evaluates market structure with a
bounded multi-agent process, and publishes an immutable recommendation for the
deterministic Strategy Runner. It never emits an order or trading signal.

## Input and Output

Evaluator consumes `SigbotFeedEvent` through a strategy-specific consumer
group and maintains a bounded, event-time-ordered context window. A trigger may
be time-based, event-count-based, volatility-based, or explicitly requested,
but every run records the exact input watermark.

Its only trading-path output is `SigbotEvaluationSnapshot`:

```text
evaluation_id
tenant_id / pipeline_id / strategy_id
regime: RANGING | TRENDING_BULLISH | TRENDING_BEARISH | UNKNOWN
confidence
hyperparameter_overrides_or_weights
evaluated_through_event_id / evaluated_through_at
valid_from / expires_at
model_ref / prompt_version / tool_versions
evidence_refs / created_at
```

Hyperparameters must conform to the strategy-declared schema, allowed ranges,
and version. Evaluator cannot change strategy code, risk hard limits, account,
venue, or order instructions. Low confidence, conflicting evidence, or an
unclassifiable market produces `UNKNOWN`; Strategy Runner then defaults to
`NONE`.

## Multi-agent Implementation

`ISigbotMarketEvaluator` is the replaceable capability. The primary target
implementation is `SigbotMultiAgentMarketEvaluator`, with a fixed internal
review sequence rather than a user-authored DAG:

1. `BootAgent` initializes the run, policy, model, and reproducibility metadata.
2. `LoaderAgent` assembles approved feed windows and optional external evidence.
3. `AlphaAgent` proposes regime, confidence, and bounded parameter values.
4. `AuditorAgent` verifies evidence, schema, time boundaries, risk constraints,
   and contradictory claims.
5. Alpha and Auditor may iterate up to a configured maximum; exhaustion yields
   `UNKNOWN`, never an unreviewed recommendation.

Boot and Loader preparation may run concurrently when their inputs are
independent. The Alpha/Auditor loop is sequential and bounded. MCP tools may
add contextual evidence but cannot replace canonical Datafeed events or access
future/live data during replay.

## Runtime and Scaling

- Controller owns one Evaluator worker set per strategy pipeline.
- Replicas of that worker set share an evaluator-specific consumer group.
- Evaluator and Strategy Runner must not use the same group; both need the full
  feed stream.
- Stateful windows are partitioned by stable feed/instrument key. Scaling must
  preserve ordering and avoid two active owners for one window.
- Evaluation runs are asynchronous. Strategy Runner uses the latest valid
  snapshot and does not block every market tick on an LLM call.

## Extension Points

| Role | Contract | Implementation |
|---|---|---|
| Service root | `SigbotEvaluator` | Strategy-scoped evaluation runtime |
| Market evaluation | `ISigbotMarketEvaluator` | `SigbotMultiAgentMarketEvaluator` |
| Agent | `ISigbotAgent` | Boot, Loader, Alpha, and Auditor agents |
| Model/tool access | Provider-specific ports | LLM and MCP adapters constrained by run policy |

## Safety and Observability Invariants

- Every snapshot is immutable and links to input watermark, model, prompt,
  tools, and evaluator version.
- The output is published only after schema and policy validation.
- Timeouts, tool errors, malformed model output, or incomplete review produce a
  typed failure/`UNKNOWN`; stale output is never silently extended.
- Prompts and evidence are protected against untrusted-instruction escalation;
  tools use an explicit allowlist and least-privilege credentials.
- Metrics separate queue delay, context loading, model latency, review rounds,
  validation failure, regime distribution, and snapshot staleness.
- Logs never contain secrets or unredacted private feed content.

## Implementation Alignment

Current source exposes `SigbotEvaluatorRunner`,
`ISigbotEvaluationExecutor`, `SigbotMasEvaluationExecutor`, and the Boot,
Loader, Alpha, and Auditor agent structure. Event-driven Datafeed subscription
is incomplete; some data preparation is placeholder behavior, and published
hyperparameter events lack the complete regime, confidence, watermark, expiry,
and provenance contract. Some identifiers are currently hard-coded, and the
runtime can access the core database directly.

Required alignment is `SigbotEvaluator` with `ISigbotMarketEvaluator`,
APIServer-provided immutable configuration, a real feed subscription, bounded
agent review, and typed `SigbotEvaluationSnapshot` publication.

## Source Map

- Evaluator runtime and executor: `src/evaluator/src/`
- Multi-agent implementation: `src/evaluator/src/executor/adk/`
- Shared evaluation/message types: `src/types/src/modules/evaluator/`
