---
name: order
description: Order management module coding guidelines. Related modules: src/order/**/*.rs (order manager service), src/core/src/modules/order/**/*.rs (order core modules)
---
You are working on the Order Manager module (SigbotOrderServer). This microservice subscribes to trading signals and executes orders via exchange client SDK.

**Architecture & Responsibilities**:
- **Order Manager**: Subscribes to trading signals from EMQX (ISigbotMessagerClient), calls exchange client SDK to place/cancel/modify orders, and publishes trade signal results back to EMQX

**Key Responsibilities**:
1. **Signal Subscription**:
   - Subscribe to trading signal topics (TOPIC_TRADING_SIGNALS) from EMQX
   - Parse `SigbotTradeSignal` messages
   - Handle wildcard topic subscriptions (multi-tenant support)

2. **Order Execution**:
   - Place orders via exchange client SDK
   - Cancel pending orders
   - Modify existing orders
   - Handle different order types (market, limit, stop-loss, etc.)

3. **Result Publishing**:
   - Publish trade results (`SigbotTradeEvent`) to EMQX (TOPIC_TRADING_RESULTS)
   - Include order status, fills, and execution details
   - Ensure at-least-once delivery semantics

**Coding Guidelines**:

1. **Message-Driven Architecture**:
   - Process signals asynchronously using tokio::spawn
   - Handle signal deserialization errors gracefully
   - Log all received signals with context (signal_id, tenant_id)
   - Support concurrent signal processing

2. **Exchange Integration**:
   - Use exchange client SDK through factory pattern
   - Handle exchange-specific error types
   - Map exchange responses to internal types
   - Support multiple exchanges (CEX, DEX)

3. **Order Lifecycle**:
   - Use state machine pattern for order states
   - Track order history and state transitions
   - Handle partial fills correctly
   - Support order modifications and cancellations

4. **Concurrency & Threading**:
   - Use atomic operations for order state updates
   - Implement proper locking for order modifications
   - Handle race conditions in order execution
   - Process orders concurrently but maintain ordering per symbol

5. **Error Handling**:
   - Handle exchange rejections gracefully
   - Implement retry logic for transient failures (network issues)
   - Log order failures with full context (signal_id, order_id, error)
   - Publish error events to notification system
   - Don't retry permanent failures (invalid order, insufficient balance)

6. **Persistence**:
   - Persist orders to database for recovery
   - Use transactions for order operations
   - Implement order replay mechanism
   - Store order execution history

7. **Testing**:
   - Test signal subscription and parsing
   - Test order execution with mock exchange clients
   - Test order state transitions
   - Verify concurrent order handling
   - Test order persistence and recovery
   - Test error scenarios (exchange failures, network issues)

8. **Performance**:
   - Optimize for low-latency order execution
   - Use connection pooling for exchange clients
   - Batch order operations when possible
   - Monitor order execution latency
