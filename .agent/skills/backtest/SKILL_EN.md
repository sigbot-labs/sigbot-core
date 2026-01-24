---
name: backtest
description: Backtest server module coding guidelines. Related modules: src/backtest/**/*.rs (backtest server implementation), src/core/src/modules/backtest/**/*.rs (backtest core modules)
---
You are working on the Backtest Server module (SigbotBacktestServer). This microservice provides historical data backtesting capabilities.

**Architecture & Responsibilities**:
- **Backtest Server**: 
  - Loads historical kline/tick/market/news data from TimescaleDB
  - Publishes historical data to EMQX (ISigbotMessagerClient) to simulate real-time market data
  - Strategy Runner subscribes to this mock market data and executes strategy logic
  - Provides mock exchange order API (receives trading signals from Order Manager)
  - Checks positions periodically for simulated liquidation
  - Updates s_bt_ledger, s_bt_balance, s_bt_position tables
  - Provides mock exchange position query API for Wallet Manager synchronization

**Key Responsibilities**:
1. **Historical Data Loading**:
   - Load historical data from TimescaleDB (klines, ticks, market data, news)
   - Support time range queries
   - Handle data gaps and missing periods
   - Optimize queries for large datasets

2. **Data Publishing**:
   - Publish historical data to EMQX at simulated real-time pace
   - Maintain proper time progression
   - Support different playback speeds
   - Handle data publishing errors

3. **Mock Exchange API**:
   - Implement mock order placement API
   - Simulate order execution with delays and slippage
   - Handle order cancellations and modifications
   - Return realistic exchange responses

4. **Position Management**:
   - Track positions in s_bt_position table
   - Check positions for liquidation conditions
   - Execute simulated liquidations
   - Update position states

5. **Ledger & Balance Management**:
   - Update s_bt_ledger with simulated transactions
   - Update s_bt_balance with balance changes
   - Maintain transaction history
   - Support balance reconciliation

6. **Mock Exchange Position API**:
   - Provide API for Wallet Manager to query positions
   - Return position data matching exchange format
   - Support periodic synchronization

**Coding Guidelines**:

1. **Historical Data Management**:
   - Efficiently load and cache historical data from TimescaleDB
   - Support multiple data sources (Klines, Trades, News)
   - Handle data gaps and missing periods gracefully
   - Optimize data queries for performance (use indexes, time partitioning)
   - Support streaming data loading for large datasets

2. **Time Simulation**:
   - Implement proper time progression (simulated real-time)
   - Support configurable playback speed
   - Maintain time consistency across all data streams
   - Handle time jumps and pauses

3. **Mock Exchange Implementation**:
   - Simulate realistic market conditions
   - Handle order execution delays and slippage
   - Support different order types (market, limit, stop-loss)
   - Simulate partial fills and order rejections
   - Return realistic exchange error responses

4. **Order Execution Simulation**:
   - Calculate realistic execution prices (with slippage)
   - Handle order matching logic
   - Support different market conditions (normal, volatile, low liquidity)
   - Simulate exchange latency

5. **Position & Liquidation**:
   - Track positions accurately
   - Check liquidation conditions periodically
   - Execute liquidations when conditions met
   - Update positions atomically

6. **Database Updates**:
   - Update s_bt_ledger, s_bt_balance, s_bt_position in transactions
   - Ensure data consistency
   - Support concurrent updates
   - Use proper indexes for queries

7. **Metrics Calculation**:
   - Calculate standard trading metrics (Sharpe ratio, drawdown, win rate, etc.)
   - Support custom metric definitions
   - Store metrics efficiently
   - Provide metric visualization data
   - Calculate metrics in real-time during backtest

8. **Performance**:
   - Optimize for large historical datasets
   - Use streaming for data processing
   - Implement parallel backtest execution when possible
   - Cache intermediate results
   - Monitor memory usage

9. **Error Handling**:
   - Handle data loading errors gracefully
   - Handle mock exchange API errors
   - Log errors with full context
   - Support error recovery mechanisms

10. **Testing**:
    - Test with various historical data scenarios
    - Verify metric calculations
    - Test backtest accuracy and reproducibility
    - Test mock exchange API behavior
    - Test position liquidation logic
    - Validate performance with large datasets
    - Test concurrent backtest execution

11. **Configuration**:
    - Support configurable playback speed
    - Configure slippage and execution delays
    - Configure liquidation parameters
    - Support backtest-specific settings
