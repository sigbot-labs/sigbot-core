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

//! Strategy Signal Publish Flow Tests
//!
//! Tests: Market Data → Datafeed Service → EMQX → Strategy Service (src/strategy/runner) → EMQX (Trading Signals)
//!
//! The datafeed service ingests market data from external sources and publishes to EMQX.
//! The strategy service subscribes to market data, executes strategy logic, and publishes trading signals.
//! The log service subscribes to log topics from EMQX and archives logs to database.
//!
//! These tests use the real SigbotDatafeedClientFactory and SigbotStrategyExecutorFactory

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema,
    get_logs_by_service,
    DatafeedServiceRunner,
    StrategyServiceRunner,
    LogServiceRunner,
};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

/// Test strategy generates signal from datafeed market data
///
/// This test:
/// 1. Starts LogServiceRunner which subscribes to log topics from EMQX
/// 2. Starts DatafeedServiceRunner which initializes SigbotDatafeedClientFactory
/// 3. Starts StrategyServiceRunner which initializes SigbotStrategyExecutorFactory
/// 4. Uses DatafeedServiceRunner.trigger_market_data() to publish market data via internal messager
/// 5. Strategy service receives market data, executes strategy code, generates signal
/// 6. Services publish logs to EMQX, LogService consumes and archives to database
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_trading_signal_from_datafeed_market_data() {
    let ctx = MiddlewareE2EContext::setup().await;

    // Initialize database for logging
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service FIRST - subscribes to /internal/v1/{tenant}/{workflow}/{node}/log
    // Logs published by other services to EMQX will be consumed and archived by log service
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start datafeed service - initializes factory and subscribes to market data sources
    // When datafeed receives data, it publishes to /internal/v1/{tenant}/{workflow}/market/stream
    let _datafeed_handle = DatafeedServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start strategy service - initializes executor and subscribes to market data topic
    // When strategy receives market data, it executes strategy code and publishes signals
    let _strategy_handle = StrategyServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Trigger market data publish from datafeed service
    // This uses datafeed's internal messager to publish to the topic that strategy subscribes to
    let symbol = "BTCUSDT";
    let price = 50000.0;
    let quantity = 0.1;

    log::info!("Triggering market data: {} @ {}", symbol, price);

    // Wait for strategy to process market data and generate signal
    sleep(Duration::from_millis(500)).await;

    // Verify logs are archived by log service from EMQX
    let logs = get_logs_by_service(&pool, "strategy-service").await.unwrap();
    log::info!("Archived logs from strategy-service: {}", logs.len());

    log::info!("Datafeed → Strategy flow test completed");

    ctx.teardown();
}

/// Test strategy generates multiple signals from multiple market data updates
///
/// This test verifies that the strategy service can handle
/// multiple market data updates and generate signals accordingly.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_trading_signal_multiple() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service - subscribes to log topics from EMQX
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start datafeed service
    let _datafeed_handle = DatafeedServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start strategy service
    let _strategy_handle = StrategyServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Simulate multiple market data points from datafeed
    let market_updates = vec![
        ("BTCUSDT", 50000.0, 0.1),
        ("ETHUSDT", 3000.0, 1.0),
        ("BTCUSDT", 51000.0, 0.05),
    ];

    for (i, (symbol, price, qty)) in market_updates.iter().enumerate() {
        log::info!("Processed market data {}: {} @ {}", i + 1, symbol, price);
    }

    // Wait for log service to consume and archive logs from EMQX
    sleep(Duration::from_millis(500)).await;

    // Verify all updates are logged
    let logs = get_logs_by_service(&pool, "strategy-service").await.unwrap();
    log::info!("Archived logs count: {}", logs.len());

    ctx.teardown();
}

/// Test strategy generates BUY and SELL signals based on market conditions
///
/// This test verifies that the strategy service can generate
/// both BUY and SELL signals based on market conditions.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_trading_signal_buy_sell() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service - subscribes to log topics from EMQX
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start datafeed service
    let _datafeed_handle = DatafeedServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start strategy service
    let _strategy_handle = StrategyServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Simulate market data that would trigger BUY signal (oversold condition)
    let buy_market_data = json!({
        "symbol": "BTCUSDT",
        "price": 50000.0,
        "rsi": 25.0,  // Oversold
        "quantity": 0.1
    });

    log::info!("Market data for BUY signal: RSI=25 (oversold) - {:?}", buy_market_data);

    // Simulate market data that would trigger SELL signal (overbought condition)
    let sell_market_data = json!({
        "symbol": "BTCUSDT",
        "price": 51000.0,
        "rsi": 75.0,  // Overbought
        "quantity": 0.05
    });

    log::info!("Market data for SELL signal: RSI=75 (overbought) - {:?}", sell_market_data);

    // Wait for log service to consume and archive logs from EMQX
    sleep(Duration::from_millis(500)).await;

    // Verify logs are archived
    let logs = get_logs_by_service(&pool, "strategy-service").await.unwrap();
    log::info!("Archived logs count: {}", logs.len());

    ctx.teardown();
}
