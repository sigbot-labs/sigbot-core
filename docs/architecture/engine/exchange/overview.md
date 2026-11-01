# Exchange

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative embedded SDK architecture. Provider capabilities vary and
must be discovered explicitly.

Exchange is not a standalone daemon. It is a capability-based async SDK
composed by Datafeed for public market streams, Order Manager for private
trading, Wallet Manager for account reconciliation, and Backtest for deterministic
execution. Provider details never leak into the fixed pipeline contracts.

## Capability Model

`IExchange` identifies a venue/account adapter. The currently exported
capability boundaries are:

| Port | Responsibility |
|---|---|
| `IOrderBookExchange` | Prices, klines, and order-book market reads exposed by a venue adapter |
| `IStockExchange` | Stock order submission and query operations |
| `IExecutionAdapter` | Durable prepare, submit, reconcile, and execution-fact reads |
| `IPublicMarketAdapter` | Read-only normalized public market snapshots |

Market types—spot, margin, perpetual/futures, options, equities, and prediction
markets—declare separate capabilities and product metadata. Unsupported
operations return a typed capability error; they never panic or silently map to
another market type.

## Adapter Structure

```text
IExchange
  -> SigbotBinanceExchangeClient
       -> Spot capabilities
       -> USD-M futures capabilities
  -> SigbotHyperliquidExchangeClient
  -> SigbotIbkrExchangeClient
  -> SigbotPolymarketExchangeClient
  -> SigbotKalshiExchangeClient
  -> PaperExchange
  -> BacktestExchange
```

These are the current adapter directories. Capability completeness differs:
live order submission is currently accepted only through the explicitly wired
Binance, IBKR, and Polymarket execution factory paths; other adapters must return
typed unsupported-capability errors instead of panicking or silently rerouting.

## Normalization

Adapters map provider-specific instruments, order types, statuses, precision,
error codes, timestamps, sequence numbers, and fills into versioned SigBot
types. Exact raw payloads may be retained as redacted diagnostic extensions but
cannot be the business contract.

Instrument metadata owns tick size, lot size, minimum notional, currency,
contract multiplier, margin mode, and supported order features. All price,
quantity, fee, and balance values use explicit decimal semantics.

## Order and Stream Semantics

- Order Manager supplies a stable `client_order_id`; retries first reconcile by
  that ID.
- Placement acknowledgement, order state, trade fill, settlement, fee, funding,
  and liquidation are distinct facts.
- Private WebSocket streams are preferred for low-latency facts; REST queries
  reconcile gaps, reconnects, and uncertain outcomes.
- Stream events expose provider sequence/cursor and connection generation so
  consumers can detect duplicate, gap, and out-of-order delivery.
- Rate limiting is shared per provider/account credential scope, not recreated
  independently by every request path.

## Security and Reliability Invariants

- Credentials are resolved from secret references inside the adapter and never
  returned through public configuration or events.
- Signing uses the exact provider timestamp/nonce policy and a bounded clock
  offset. Sensitive request/response fields are redacted.
- Timeouts are classified as unknown outcome until reconciliation proves
  success or failure.
- Retry policy distinguishes safe reads, idempotent writes, and non-idempotent
  external side effects.
- Sandbox/testnet and production endpoints cannot share an account identifier
  or credential object accidentally.
- WebSocket reconnect performs subscription restoration and gap recovery before
  declaring the stream healthy.

## Implementation Alignment

Live orders use `ExchangeFactory` plus `ExecutionAdapterConfig` to select the
official Binance Spot SDK, the mature `ibapi` TWS/Gateway SDK, or the official
Polymarket V2/V3 SDK. Backtest execution selects the dedicated historical
adapter; it never rewrites a production exchange URL. Existing market-source
interfaces remain under their original directories and are not these live
order paths.

Binance disables SDK submission retries and derives its client order ID from the
existing business key. IBKR and Polymarket require `prepare`, durable persistence
by Order after Wallet reservation, and one claimed submission attempt. Recovery
only reconciles the persisted venue ID. IBKR preparation retains the SDK Order
and Contract; Polymarket retains the official Solidity ABI bytes, signature and
EIP-712 order hash, so restart cannot silently regenerate salts or signatures.

OpenSSL AES-256-GCM seals the prepared payload. Authenticated data binds scope,
account, workflow, node, run/sequence, intent, provider and venue order ID. Configure
`prepared_keys.active_key` and a `prepared_keys.keys` mapping from key versions to
central Secret-map keys containing 32-byte hex keys. Keep old key versions
while any nonterminal order still references them. Neither these keys nor trading
credentials enter the strategy process.

IBKR requires an explicit gateway, exclusive API client ID, broker-account
Secret key reference and resolved SDK Contracts with positive contract IDs.
Connections verify account ownership; SDK reconnection is disabled for submissions.
Decimal values that cannot round-trip through its f64 boundary are rejected.
Polymarket uses GTC for limit orders and FOK for market intents, with fixed shares
and a price ceiling/floor from the risk-checked market evidence. Its `matched`
status is acceptance, not confirmed on-chain settlement.

Order polls these adapters for actual execution facts, with the venue cursor,
deduplication record and MQTT Outbox committed together. Binance uses official
`myTrades` amounts and commissions; IBKR joins execution IDs to official commission
reports and supports stock/ETF contracts. A missing commission or an execution
correction requires reconciliation rather than a fabricated zero fee.

Polymarket requires `settlement: {rpc_url_secret: polygon_rpc_url, min_confirmations: 64}`.
The central Secret-map entry contains an HTTPS Polygon RPC URL. Missing configuration
prevents adapter construction. Before any order POST the adapter checks chain 137,
the official exchange contract, collateral decimals and finalized-block support.
The prepared submission also seals the initial block number. Recovery scans
bounded block ranges using the exact persisted order hash, at most 32 receipts per
page so a busy order can advance its cursor within the RPC timeout. Each log must
belong to a successful receipt on a canonical finalized block with the required
confirmation depth. The event maker, token, side and exchange must match the order.
The instrument identifies `base_asset: outcome:<token_id>` and `quote_asset: pUSD`.
An old prepared order without a scan boundary requires manual reconciliation.

Actual gross collateral and fee come from the official
[OrderFilled ABI](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/interfaces/ITrading.sol)
and its [collateral fee definition](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/mixins/Fees.sol).
`ExecutionFact.quote_quantity` preserves this exact amount when the displayed unit
price is a repeating decimal; Wallet uses it before the legacy price-times-quantity
fallback. An execution's natural identity is its transaction hash and log index.
Order status alone cannot release a reservation until the corresponding actual
executions and commissions have been recorded.

The settlement tests use explicitly synthetic external RPC receipt fixtures and
cover decoding, exact cash amounts, reverts, finality and identity rejection. A real
Alloy HTTP client also talks to a local Axum fixture to verify preflight, pagination,
repeatable execution identities and refusal of a different chain.
These tests, SDK round trips and database tests do not demonstrate live financial
execution. No real order is submitted by the test suite.

## Source Map

- Exchange SDK and provider adapters: `src/exchange/`
- Shared exchange types: `src/types/src/exchange/`
