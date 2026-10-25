# Exchange

[中文](overview_ZH.md) · [Architecture overview](../../overview.md)

**Status:** Normative embedded SDK architecture. Provider capabilities vary and
must be discovered explicitly.

Exchange is not a standalone daemon. It is a capability-based async SDK
composed by Datafeed for public market streams, Order Manager for private
trading, Wallet Manager for account reconciliation, and Backtest for simulated
execution. Provider details never leak into the fixed pipeline contracts.

## Capability Model

`ISigbotExchangeClient` identifies a venue/account adapter and exposes only the
capability ports it implements:

| Port | Responsibility |
|---|---|
| `ISigbotMarketDataClient` | REST snapshots, instruments, klines, trades, funding, and public reference data |
| `ISigbotTradingClient` | Place, cancel, replace, and query orders with stable client IDs |
| `ISigbotAccountClient` | Balances, positions, fills, transfers, fees, and account configuration |
| `ISigbotExchangeStream` | Public and private WebSocket/session streams with cursor and reconnect semantics |

Market types—spot, margin, perpetual/futures, options, equities, and prediction
markets—declare separate capabilities and product metadata. Unsupported
operations return a typed capability error; they never panic or silently map to
another market type.

## Adapter Structure

```text
ISigbotExchangeClient
  -> SigbotBinanceExchangeClient
       -> Spot capabilities
       -> USD-M futures capabilities
  -> SigbotBitgetExchangeClient
  -> SigbotCoinbaseExchangeClient
  -> SigbotIbkrExchangeClient
  -> SigbotSchwabExchangeClient
  -> SigbotSimulatedExchangeClient
```

Provider names in this diagram express the target adapter boundary, not a claim
that every adapter is currently implemented. A provider factory validates
configuration and returns a capability descriptor before callers start.

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

Current source exposes `ISigbotExchangeClient` plus CEX, DEX, stock,
prediction-market, and order-book subtraits. Source adapters exist for Binance,
Hyperliquid, IBKR, Polymarket, and Kalshi, but the factory currently constructs
only Binance and IBKR paths. Binance support is primarily USD-M futures; spot
must not be claimed complete. Bitget, Coinbase, and Schwab are target adapters,
not current implementations. Several configured providers can currently reach
unimplemented or panic paths.

Required alignment is capability discovery, typed unsupported behavior,
complete REST/WebSocket normalization, shared rate limits, stable external
idempotency, and contract tests per provider/product combination.

## Source Map

- Exchange SDK and provider adapters: `src/exchange/`
- Shared exchange types: `src/types/src/modules/exchange/`
