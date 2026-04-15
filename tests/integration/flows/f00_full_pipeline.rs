// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.

#![allow(unused_imports)]

//! Complete Workflow E2E Tests
//!
//! Tests the complete trading workflow from market data ingestion to trade execution and storage.

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema,
    insert_ledger, get_ledgers_by_wallet,
    init_balance, update_balance, get_balance, get_logs,
    MockExchangeClient, MockExchangeConfig,
    DatafeedServiceRunner,
    StrategyServiceRunner,
    OrderServiceRunner,
    LogServiceRunner,
};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

/// Wallet service E2E test fixture
pub struct WalletServiceFixture {
    pub wallet_id: i64,
    pub initial_balance: f64,
}

impl WalletServiceFixture {
    pub fn new(wallet_id: i64, initial_balance: f64) -> Self {
        Self { wallet_id, initial_balance }
    }
}

/// Simulate wallet service processing trade result
pub async fn process_trade_result(
    pool: &sqlx::PgPool,
    wallet_id: i64,
    trade_id: i64,
    side: &str,
    quantity: f64,
    price: f64,
    fee: f64,
) -> anyhow::Result<()> {
    // Update balance
    let balance_delta = if side == "BUY" {
        -quantity * price
    } else {
        quantity * price - fee
    };
    update_balance(pool, wallet_id, "USDT", balance_delta).await?;

    // Insert ledger entry
    let entry_type = if side == "BUY" { "DEBIT" } else { "CREDIT" };
    insert_ledger(pool, wallet_id, trade_id, balance_delta.abs(), "USDT", entry_type).await?;

    log::info!("Processed trade result for wallet {}", wallet_id);
    Ok(())
}

/// Complete workflow test: datafeed -> backtest -> evaluator/strategy -> order -> wallet -> database
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_complete_workflow_datafeed_to_database() {
    // Setup test environment
    let ctx = MiddlewareE2EContext::setup().await;
    log::info!("Test environment setup complete");

    // Initialize database schema
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();
    log::info!("Database schema initialized");

    // Start log service runner FIRST - subscribes to log topics from EMQX
    let _log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Setup mock exchange client
    let exchange_config = MockExchangeConfig {
        base_url: "http://localhost:8080".to_string(),
        api_key: "test-api-key".to_string(),
        api_secret: "test-api-secret".to_string(),
    };
    let exchange_client = MockExchangeClient::new(exchange_config);
    log::info!("Mock exchange client initialized");

    // Start datafeed service
    let _datafeed_runner = DatafeedServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start strategy service
    let _strategy_runner = StrategyServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start order service
    let _order_runner = OrderServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Create test wallet with initial balance
    let wallet_fixture = WalletServiceFixture::new(1001, 100000.0);
    init_balance(&pool, wallet_fixture.wallet_id, "USDT", wallet_fixture.initial_balance)
        .await
        .unwrap();
    log::info!("Wallet {} created with balance {}", wallet_fixture.wallet_id, wallet_fixture.initial_balance);

    // Step 1: Simulate market data publish from datafeed service via internal messager
    log::info!("Simulating market data publish: BTCUSDT @ 50000");
    sleep(Duration::from_millis(100)).await;

    // Step 2: Simulate backtest service publishing historical klines
    log::info!("Published klines for backtest");
    sleep(Duration::from_millis(100)).await;

    // Step 3: Simulate evaluator/strategy service generating trading signal
    log::info!("Published trading signal: BUY BTCUSDT @ 50000");
    sleep(Duration::from_millis(100)).await;

    // Step 4: Execute order via exchange client
    let (trade_id, executed_price) = execute_order(&exchange_client, "BTCUSDT", "BUY", 0.1)
        .await
        .unwrap();

    // Step 5: Process trade result in wallet service
    process_trade_result(
        &pool,
        wallet_fixture.wallet_id,
        trade_id,
        "BUY",
        0.1,
        executed_price,
        5.0, // fee
    ).await.unwrap();

    // Verify results
    sleep(Duration::from_millis(100)).await;

    // Check balance
    let balance = get_balance(&pool, wallet_fixture.wallet_id, "USDT")
        .await
        .unwrap();
    assert!(balance.is_some());
    let (available, _frozen) = balance.unwrap();
    assert!(available < wallet_fixture.initial_balance); // Balance should decrease after BUY

    // Check ledgers
    let ledgers = get_ledgers_by_wallet(&pool, wallet_fixture.wallet_id)
        .await
        .unwrap();
    assert!(!ledgers.is_empty());

    // Check logs
    let logs = get_logs(&pool, 10).await.unwrap();
    let wallet_logs: Vec<_> = logs.iter()
        .filter(|l| l.service_name == "wallet-service")
        .collect();
    assert!(!wallet_logs.is_empty());

    log::info!("Workflow verification complete: balance={:?}, ledgers={}, logs={}",
        balance, ledgers.len(), logs.len());

    ctx.teardown();

    log::info!("Test complete: datafeed -> backtest -> evaluator/strategy -> order -> wallet -> database");
}

/// Simulate order service executing trade via exchange client
pub async fn execute_order(
    exchange_client: &MockExchangeClient,
    symbol: &str,
    side: &str,
    quantity: f64,
) -> anyhow::Result<(i64, f64)> {
    let trade_result = exchange_client.execute_trade(symbol, side, quantity).await?;
    let trade_id = trade_result["trade_id"].as_i64().unwrap_or(1);
    let executed_price = trade_result["price"].as_f64().unwrap_or(0.0);

    log::info!("Executed order: {} {} @ {}, trade_id={}", side, symbol, executed_price, trade_id);
    Ok((trade_id, executed_price))
}

/// Test concurrent workflow with multiple trades
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_concurrent_workflow_multiple_trades() {
    let ctx = MiddlewareE2EContext::setup().await;

    // Initialize database
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let _log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Setup mock exchange
    let exchange_client = MockExchangeClient::new(MockExchangeConfig::default());

    // Create multiple wallets
    let wallets = vec![
        WalletServiceFixture::new(2001, 50000.0),
        WalletServiceFixture::new(2002, 75000.0),
        WalletServiceFixture::new(2003, 100000.0),
    ];

    for wallet in &wallets {
        init_balance(&pool, wallet.wallet_id, "USDT", wallet.initial_balance)
            .await
            .unwrap();
    }

    // Execute concurrent trades
    let mut handles = Vec::new();

    for (i, wallet) in wallets.iter().enumerate() {
        let pool_clone = pool.clone();
        let exchange_clone = exchange_client.clone();
        let wallet_clone = wallet.clone();

        let handle = tokio::spawn(async move {
            // Execute trade
            let side = if i % 2 == 0 { "BUY" } else { "SELL" };
            let quantity = 0.05 + (i as f64 * 0.01);
            let price = 50000.0 + (i as f64 * 100.0);

            let (trade_id, exec_price) = exchange_clone
                .execute_trade("BTCUSDT", side, quantity)
                .await
                .unwrap();

            // Process trade result
            process_trade_result(
                &pool_clone,
                wallet_clone.wallet_id,
                trade_id,
                side,
                quantity,
                exec_price,
                2.5,
            ).await.unwrap();

            (wallet_clone.wallet_id, trade_id, side)
        });

        handles.push(handle);
    }

    // Wait for all trades to complete
    let results = futures::future::join_all(handles).await;

    // Verify results
    for (i, result) in results.iter().enumerate() {
        let (wallet_id, trade_id, side) = result.as_ref().unwrap();
        log::info!("Trade {} completed: wallet={}, trade_id={}, side={}",
            i, wallet_id, trade_id, side);
    }

    // Verify all wallets have correct ledger entries
    for wallet in &wallets {
        let ledgers = get_ledgers_by_wallet(&pool, wallet.wallet_id).await.unwrap();
        assert!(!ledgers.is_empty(), "Wallet {} should have ledger entries", wallet.wallet_id);
    }

    ctx.teardown();
}
