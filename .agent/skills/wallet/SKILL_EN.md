---
name: wallet
description: Wallet manager module coding guidelines. Related modules: src/wallet/**/*.rs (wallet manager service), src/core/src/modules/wallet/**/*.rs (wallet core modules)
---
You are working on the Wallet Manager module (SigbotWalletServer). This microservice manages wallet balances, positions, and equity snapshots.

**Architecture & Responsibilities**:
- **2.1.5 Wallet Manager**: 
  - Subscribes to trade signal results from EMQX (ISigbotMessagerClient)
  - Updates s_ledger, s_balance, s_position, s_equity_snapshot entities/tables in transaction
  - Runs async job to periodically sync position/balance data from exchange APIs to database (for offline reconciliation and account management)

**Key Responsibilities**:
1. **Trade Event Processing**:
   - Subscribe to trade result topics (TOPIC_TRADING_RESULTS) from EMQX
   - Parse `SigbotTradeEvent` messages
   - Process trade events in transaction order

2. **Ledger Management**:
   - Update `s_ledger` table with transaction records
   - Implement double-entry bookkeeping
   - Ensure transaction atomicity

3. **Balance Updates**:
   - Update `s_balance` table atomically
   - Handle concurrent balance updates correctly
   - Support multi-currency balances

4. **Position Tracking**:
   - Update `s_position` table with current positions
   - Track position changes per symbol
   - Calculate unrealized P&L

5. **Equity Snapshots**:
   - Update `s_equity_snapshot` table periodically
   - Capture equity at specific points in time
   - Support historical equity analysis

6. **Exchange Synchronization**:
   - Periodic async job to sync positions/balances from exchange APIs
   - Reconcile local database with exchange data
   - Detect and log discrepancies
   - Support offline reconciliation

**Coding Guidelines**:

1. **Message-Driven Architecture**:
   - Process trade events asynchronously using tokio::spawn
   - Handle event deserialization errors gracefully
   - Log all received events with context (trade_id, order_id, tenant_id)
   - Support concurrent event processing with proper ordering

2. **Transaction Management**:
   - Use database transactions for atomic updates
   - Update s_ledger, s_balance, s_position in single transaction
   - Handle transaction rollbacks correctly
   - Use ON CONFLICT clauses for idempotency

3. **Idempotency**:
   - Use database ON CONFLICT to handle duplicate events
   - Track processed event IDs to prevent reprocessing
   - Handle message redelivery gracefully
   - Ensure at-least-once processing semantics

4. **Balance Management**:
   - Use atomic database operations for balance updates
   - Implement double-entry bookkeeping principles
   - Handle balance reconciliation with exchange data
   - Track balance history for audit trail

5. **Multi-Currency Support**:
   - Support multiple cryptocurrencies and fiat currencies
   - Handle currency conversions correctly
   - Use decimal types for precise calculations (avoid f64 for money)
   - Store currency codes consistently

6. **Exchange Synchronization Job**:
   - Use distributed lock to prevent concurrent sync jobs
   - Query exchange APIs for current positions/balances
   - Compare with local database state
   - Log discrepancies for manual review
   - Update database with exchange data

7. **Error Handling**:
   - Implement retry logic with exponential backoff
   - Handle database errors gracefully
   - Log errors with full context
   - Don't lose trade events on transient failures
   - Support dead letter queue for failed events

8. **Security**:
   - Never log sensitive information (private keys, passwords, balances)
   - Use secure random number generation for IDs
   - Implement proper access control
   - Encrypt sensitive data at rest

9. **Performance**:
   - Optimize database queries (use indexes)
   - Batch database updates when possible
   - Use connection pooling
   - Monitor processing latency
   - Cache frequently accessed data

10. **Testing**:
    - Test trade event processing with mock events
    - Test transaction atomicity
    - Test concurrent balance updates
    - Test idempotency (duplicate events)
    - Test exchange synchronization logic
    - Test error scenarios (database failures, network issues)
