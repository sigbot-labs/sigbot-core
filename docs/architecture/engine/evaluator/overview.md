# Evaluator

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Implemented asynchronous evaluation side path with typed snapshots;
real-model quality and production memory retrieval remain evidence gaps.

`SigbotEvaluator` is the non-deterministic side path of one strategy pipeline.
It observes normalized market/news events, evaluates market structure with a
bounded multi-agent process, and publishes an immutable recommendation for the
deterministic Strategy Runner. It never emits an order or trading signal.

## Input and Output

Evaluator consumes `DatafeedEvent` envelopes through a strategy-specific consumer
group and maintains a bounded, event-time-ordered context window. A trigger may
be time-based, event-count-based, volatility-based, or explicitly requested,
but every run records the exact input watermark.

Its only trading-path output is `HyperparametersUpdate`:

```text
DurableMessage: env / tenant_id / workflow_id / node_id / run_id / seq / traceparent / produced_at
HyperparametersUpdate: version / as_of / valid_until
probabilities: trend / range / stabilizing / suppression / unknown
parameters
model_version / classification_version
case_refs
```

Hyperparameters must conform to the strategy-declared schema, allowed ranges,
and version. Evaluator cannot change strategy code, risk hard limits, account,
venue, or order instructions. Low confidence, conflicting evidence, or an
unclassifiable market produces `UNKNOWN`; Strategy Runner then emits no opening
intent.

## Evaluation Implementation

`IEvaluator` is the replaceable capability and `LlmEvaluator` is the current
implementation. It claims one admitted source event, loads persisted facts and
historical cases available at that event's watermark, calls one bounded
OpenAI-compatible endpoint, validates `EvaluationAnswer`, and atomically commits
the typed update and Outbox. Facts cited by the model must match facts supplied
at this boundary. A future multi-agent review loop must be introduced behind
`IEvaluator`; it is not part of the current implementation.

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
| Service root | `SigbotEvaluatorRunner` | Component startup and Provider registration |
| Evaluation contract | `IEvaluator` | `LlmEvaluator` |
| Model result | `EvaluationAnswer` | Validated probabilities, evidence and parameters |
| Persistence | Core Handler/Repository | `EvaluatorPostgresRepository`, `EvaluatorSQLiteRepository` |

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

`SigbotEvaluatorRunner` consumes scoped feed events, loads only facts/cases
available at the decision watermark, calls the bounded LLM/MAS path, validates
the five-state probability result, and commits a versioned
`HyperparametersUpdate` plus Outbox. Facts and inference are stored
separately and cited cases must come from the admitted input.

The E2E boundary uses an explicit OpenAI-compatible mock. It proves contracts,
not model profitability, calibration, or the quality of a production pgvector
case corpus.

## Source Map

- Evaluator runtime and LLM adapter: `src/component/evaluator/src/`
- Evaluator repositories: `src/core/src/modules/evaluator/storage/`
- Shared evaluation/message types: `src/types/src/evaluator/`
