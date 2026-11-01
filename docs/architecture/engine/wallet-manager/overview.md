# Wallet Manager

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Implemented authoritative reservation, immutable accounting, and
portfolio-projection component; broad external-account reconciliation remains open.

`SigbotWalletServer` is the accounting and portfolio source of truth. It owns
funds/margin reservations, an immutable ledger, balances, positions, realized
and unrealized P&L, equity snapshots, and the versioned portfolio snapshots
consumed by strategies. It never infers money movement from a strategy signal
or order acknowledgement.

## Monetary Input Contract

Wallet consumes authoritative, immutable execution facts owned by Order
Manager, including fills, fees, funding, settlement, transfer, and liquidation.
Each fact carries a stable event ID, account, asset/instrument, quantity,
decimal amount/price, venue timestamp, and causation identifiers.

Before placement, Order publishes an idempotent reservation request and waits
for the scoped result. A
reservation reduces available funds or margin without pretending a fill has
occurred. Fill, reject, cancel, and expiry facts consume or release the
reservation according to an explicit state machine.

## Atomic Accounting Transaction

For each monetary event, one Wallet database transaction:

1. inserts an inbox/idempotency record;
2. appends balanced immutable ledger entries;
3. consumes or releases reservations;
4. updates balance and position projections with optimistic or row-level
   concurrency control;
5. records realized P&L, fees, funding, and settlement effects;
6. writes a new portfolio-snapshot/outbox record;
7. commits before acknowledging the message.

If the inbox insert reports a duplicate, no ledger or projection delta is
applied again. Ledger entries are never edited; corrections use compensating
entries.

## Owned Models

| Model | Purpose |
|---|---|
| Reservation | Idempotent hold for funds or margin before an external order |
| Ledger | Immutable double-entry accounting facts |
| Balance | Materialized available, reserved, and total amount per asset/account |
| Position | Materialized quantity, cost basis, margin, realized/unrealized P&L per instrument |
| Equity snapshot | Time-series view for analysis and risk |
| Versioned portfolio payload | Strategy-facing JSON view with account, balances, positions, exposure, event watermark, and effective time |

The `wlt_*` table domain has one Wallet Repository writer. Other components use
typed messages/snapshots and never query Wallet tables directly. The physical
schema and migration file may be shared with other domains without sharing
write ownership.

## Extension Points

| Role | Contract | Examples |
|---|---|---|
| Service root | `SigbotWalletServer` | Component startup and Provider registration |
| Wallet behavior | `IWallet` | `WalletAccountProcessor` |
| Reservation | `WalletReservationProcessor` | Idempotent hold/release processing |
| Persistence | Core Handler/Repository | `WalletPostgresRepository`, `WalletSQLiteRepository` |
| Reconciliation source | Exchange account capability | Venue balance/position/fill queries |

## Reconciliation and Consistency

Periodic reconciliation compares local order/fill watermarks, balances, and
positions with authoritative venue APIs. Differences create explicit
reconciliation cases and corrective facts; they are not silently overwritten.
Distributed ownership or stable account sharding prevents concurrent reconcilers
from applying the same range.

Order and Wallet maintain separate transactional ownership and converge through
outbox/inbox, replay, and reconciliation. Their current tables share one
physical PostgreSQL schema, but no component may issue cross-domain writes.

## Safety Invariants

- Money, price, quantity, fee, and rate use explicit decimal scale and currency
  or instrument metadata; binary floating point is not authoritative.
- Every ledger transaction balances by currency and accounting policy.
- Negative available balance or margin is rejected unless the product and risk
  policy explicitly allow it.
- Portfolio snapshots are immutable, monotonic by account version, and include
  the applied execution watermark.
- No balance, credential, or private account detail is logged without explicit
  redaction and access policy.
- Database transactions never span exchange network calls.

## Implementation Alignment

`SigbotWalletServer` is the only writer for wallet reservations, immutable
ledger/journal/posting facts, balances, positions, equity snapshots, and
versioned portfolio output. Funding and execution facts enter through an
Inbox; the balanced postings, reservation release, projections, and Outbox
commit in one PostgreSQL transaction. Duplicate and out-of-order terminal
events retain factual consistency, and projections are rebuildable from the
immutable record.

Real-account transfers, corporate actions, full derivatives margin/liquidation
facts, continuous external reconciliation, and complete multi-asset NAV are
not yet accepted.

## Source Map

- Wallet component: `src/component/wallet/`
- Wallet repositories: `src/core/src/modules/wallet/storage/`
- Wallet domain types: `src/types/src/wallet/`
- Wallet migrations: `migration/`
