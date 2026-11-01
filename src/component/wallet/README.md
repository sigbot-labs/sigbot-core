# Sigbot Wallet Manager

Wallet is the only writer of financial facts, reservations and portfolio projections. The v2 runtime subscribes to the configured account's `internal/v2/{env}/{tenant_id}/account/{account_id}/execution/event`. An order receipt is not an execution and is rejected by the fact DTOs.

`ExecutionFact` contains the real exchange execution ID, decimal quantity/price/fee, event time and the originating reservation's node and sequence. When a venue supplies `quote_quantity`, settlement and balanced postings use that exact gross quote-asset movement; adapters without that venue field derive it with checked decimal price × quantity arithmetic. `FundingFact` contains an observed signed asset movement and its external source event ID. Neither invents a correlation ID. The account, wallet, exchange, mode and tenant must agree with the persisted wallet binding.

Position reconstruction also carries the remaining gross quote cost independently of the displayed average price. Partial closes allocate cost by quantity; the last close consumes the entire remaining cost, and a reversal carries only the unallocated quote amount into the new position. Realized PnL therefore does not reconstruct cash from a recurring rounded price. Fees remain separate immutable postings.

The wallet transaction commits inbox deduplication, immutable journal, per-asset balanced postings, reservation settlement, balance/position projections, portfolio version and an outgoing portfolio snapshot together. Transport PUBACK follows successful commit. An external execution or funding ID replay cannot debit the account twice, even if another MQTT business sequence carries it. Reusing an identity with different facts fails. Position projections replay executions by event time and exchange fact ID, so late fills produce the same basis and realized P&L. A terminal fill releases the remaining reserve only after the journal accounts for the complete intended quantity; receiving the final fill before earlier partials does not release funds prematurely.

`wlt_wallet`, immutable `wlt_ledger`, rebuildable `wlt_balance`, `wlt_position`, and equity snapshots form the public wallet model. The decimal journal and balanced postings retain external string execution IDs without lossy integer or floating-point conversion. Equity requires prices in one numeraire; summing different asset quantities is not a valuation.

`rebuild_balances` derives total assets from journal postings and active locks from `wlt_reservation.remaining`; available equals total minus locks. An opening balance must be journaled first. Reserving and releasing funds change availability, not asset ownership. Immutable facts are corrected by appending compensating facts. The distributed runtime consumes `A/wallet/reservation/request` from configured order nodes and atomically commits Inbox, reservation, portfolio and `A/wallet/reservation/result` Outbox. Rejected reservations also produce a durable result. The original intent node/run/sequence remains the reservation identity; `requestSpanId` exists only in the bounded temporary reply cache.

The wallet also consumes typed `OrderStatusFact` from the configured order nodes. A terminal status arriving before its execution details is acknowledged after durable Inbox intake, then completed after the execution journal reaches the venue's cumulative quantity. This allows fills to continue arriving and prevents early release after a partial cancellation. A late reserve request cannot acquire funds for a rejected order.

The service receives its provider configuration through `properties.runtime_config`:

```json
{"scope":{"env":"paper","tenant_id":"0192f7b2-5c00-7000-8000-000000000801"},"account_id":"0192f7b2-5c00-7000-8000-000000000802","node_id":6,"order_node_ids":[5]}
```

Financial database credentials and MQTT password environment references stay on the trusted host. Shared messager and log components have no independent deployment. Focused projection tests are in `src/core/tests/wallet_projection.rs`; the marketpulse case drives real PostgreSQL, broker, funding, order execution and wallet transactions.
