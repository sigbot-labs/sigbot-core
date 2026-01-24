---
name: exchange
description: Exchange client module coding guidelines. Related modules: src/exchange/**/*.rs (exchange client SDK implementations), src/core/src/modules/exchange/**/*.rs (exchange core modules)
---
You are working on the Exchange Client module. This module provides SDK for interacting with various exchanges (CEX, DEX, Stock, Predict).

**Architecture & Responsibilities**:
- Provides exchange client SDK used by Order Manager to execute orders
- Supports multiple exchange types: CEX (Centralized Exchanges), DEX (Decentralized Exchanges), Stock exchanges, Prediction markets
- Handles exchange-specific API interactions and data normalization

**Key Responsibilities**:
1. **Exchange Client SDK**:
   - Provide unified interface for all exchange types
   - Implement exchange-specific clients (Binance, Uniswap, etc.)
   - Support order placement, cancellation, modification
   - Support position and balance queries

2. **Market Data Access**:
   - Fetch market data (klines, orderbook, trades)
   - Support real-time data streams (WebSocket)
   - Normalize data from different exchanges

3. **Order Execution**:
   - Place orders via exchange APIs
   - Cancel pending orders
   - Modify existing orders
   - Query order status

4. **Account Management**:
   - Query account balances
   - Query positions
   - Query trade history
   - Support multi-currency accounts

**Coding Guidelines**:

1. **Exchange Abstraction**:
   - Use trait-based design (`IExchangeClient` or `ExchangeProvider`)
   - Implement trait for all exchange types (CEX, DEX, Stock, Predict)
   - Normalize exchange-specific data structures to common types (`sigbot-types`)
   - Support factory pattern for exchange creation

2. **Exchange Types**:
   - **CEX (Centralized)**: REST API + WebSocket (Binance, etc.)
   - **DEX (Decentralized)**: Blockchain interactions (Uniswap, etc.)
   - **Stock**: Stock exchange APIs
   - **Predict**: Prediction market APIs

3. **API Integration**:
   - Handle exchange-specific authentication (API keys, signatures)
   - Implement request signing for authenticated endpoints
   - Support both REST and WebSocket connections
   - Handle exchange-specific rate limits

4. **Error Handling**:
   - Map exchange-specific errors to `ExchangeError` enum
   - Handle rate limiting with exponential backoff
   - Handle network failures and timeouts
   - Log exchange API errors with full context (exchange, endpoint, error)
   - Distinguish between transient and permanent errors

5. **Rate Limiting**:
   - Respect exchange-specific rate limits
   - Implement per-exchange rate limit tracking
   - Queue requests when rate limit exceeded
   - Support rate limit recovery

6. **Async Operations**:
   - All exchange API calls must be async
   - Use connection pooling for HTTP clients (reqwest)
   - Implement timeout handling for external calls
   - Support concurrent API calls with proper rate limiting

7. **Data Normalization**:
   - Use `sigbot-types` for shared types (Order, Trade, Kline, Balance, Position)
   - Normalize timestamps to UTC
   - Handle different exchange symbol formats (normalize to common format)
   - Convert exchange-specific order types to common types
   - Normalize price and quantity formats (decimal handling)

8. **Order Execution**:
   - Support different order types (market, limit, stop-loss, etc.)
   - Handle order status updates
   - Support partial fills
   - Track order execution history

9. **Security**:
   - Never log API keys or secrets
   - Use secure storage for credentials
   - Implement proper request signing
   - Validate exchange responses

10. **Performance**:
    - Optimize API calls (batch when possible)
    - Use efficient serialization/deserialization
    - Cache exchange metadata (symbols, fees)
    - Monitor API call latency

11. **Testing**:
    - Mock exchange APIs in unit tests
    - Test error scenarios (network failures, rate limits, invalid responses)
    - Verify data normalization correctness
    - Test order execution flows
    - Test concurrent API calls
    - Use test fixtures for consistent testing

12. **Configuration**:
    - Support per-exchange configuration
    - Configure API endpoints and credentials
    - Configure rate limits and timeouts
    - Support environment-specific settings
