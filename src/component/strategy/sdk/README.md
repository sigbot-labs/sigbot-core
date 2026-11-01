# Sigbot Strategy SDK

The v2 SDK preserves the existing crate and legacy `TradingSignal` APIs. New strategies implement a class with `on_hyperparameters(ctx, update)` and `on_data(ctx, event)`, each returning `Effects(intents, requests, timers)`. The v2 host requires this return type; the legacy streaming executor remains an explicit migration path.

```python
from sigbot_sdk import Effects

class Strategy:
    def on_hyperparameters(self, ctx, update):
        ctx.state["position_controls"]["max_exposure"] = update.parameters.get("max_exposure", "0")
        return Effects()

    def on_data(self, ctx, event):
        ctx.state["user"]["last_source"] = event.source
        return Effects()
```

`ctx.identity`, `ctx.clock`, `ctx.configuration`, `ctx.hyperparameters`, and `ctx.portfolio` are recursively read-only. Only `ctx.state` is mutable. It contains regime, features, position controls, pending request state and user state. `ctx.market.snapshot(query)` and `ctx.compute.submit(plan)` enqueue bounded effects; their eventual results enter `on_data`. These methods do not synchronously query external services. Snapshot freshness and cross-source skew are checked; a multi-source snapshot is not atomic. Compute plans declare model partitioning, bounded rows and bytes; ordinary Python is never automatically sharded.

`SerialStrategy` serializes callbacks per instance. It loads the last committed checkpoint, creates a draft, and commits inbox, checkpoint and outgoing effects in one PostgreSQL transaction. A callback failure discards the draft. Database version comparison fences concurrent owners. Hyperparameter versions route deterministically; older updates cannot revert the active regime. Market availability, case availability and validity intervals prevent using future information through the supplied SDK inputs.

The Rust host runs each Python callback in the separate `sigbot-strategy-worker` Rust/PyO3 binary. Only code and data cross bounded stdio; host credentials are removed from the worker environment and inherited descriptors are closed on exec. The worker requires Linux Landlock ABI 3 and seccomp, denies networking/process creation/host file access, and limits address space, CPU, files and wall time. Missing isolation fails startup/callback rather than falling back to unrestricted execution. Worker state lives in the transactional context; Python globals do not survive a callback. Arbitrary strategy code is still responsible for making deterministic decisions from the supplied clock and inputs.

Build and install the worker beside the main binary, or set the absolute host-only `SIGBOT_STRATEGY_WORKER_BINARY`. The existing strategy service enables v2 through `properties.runtime_v2="true"` and a JSON `properties.runtime_config` matching `StrategyNodeConfig`. `edges` explicitly authorizes source nodes and topic kinds. The account portfolio is read from the wallet projection. Configure `log_archive` to durably capture Python stdout, stderr and exceptions before acknowledging input; archive credentials remain in the host.

Keep the MQTT client identity stable across a restart of the same run and distinct across different runs. A Workflow topic can carry several nodes and runs. After validating scope and Workflow, the host durably quarantines deliveries for another target, an unconfigured input edge or an inactive run in a separate routing Inbox partition before ACK. Migration 15 adds the auditable routing reason. This does not consume the intended node/run's normal strategy Inbox or invoke its code; conflicting contents under an existing message identity still fail.

Account inputs are explicit: `execution_node_ids`, `order_node_ids`, and `wallet_node_ids` authorize producers on this account's execution, order, and portfolio topics. Workflow graph edges from Backtest/Order/Wallet to Strategy populate these lists. The host validates scope, account, workflow, run and producer before invoking `on_data`; an order acceptance receipt is not an execution. Portfolio version, balances and positions are read in one repeatable-read transaction.

Notification and export requests use the same atomic Effects path:

```python
return Effects(requests=[
    {"kind": "notification", "query": {
        "channel": "in_app", "recipient": "trader", "template": "fill",
        "variables": {"symbol": event.symbol}}},
    {"kind": "export", "query": {
        "range": "Trades!A2:B2", "rows": [[event.symbol, "filled"]]}}
])
```

Configure Strategy→Notification/Exporter edges and their reverse result edges. The response enters `on_data` as `query_result`, with `operation` and the provider result. Exporter derives its idempotency key from the existing scoped message identity. Wire-only `requestSpanId` remains absent from checkpoint, Inbox and Outbox payloads. In replay, external delivery needs an explicitly isolated sink; never replay into a live recipient or live spreadsheet.

Focused checks:

```sh
python3 -m unittest discover -s src/strategy/sdk/tests/sdk -p '*_test.py'
cargo test -p sigbot-strategy-sdk --test runtime_transactions
cargo test -p sigbot-strategy-runner --test worker_sandbox
cargo test -p sigbot-strategy-runner --test service_effects -- --include-ignored
```
