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

//! Audit Log Service Flow Tests
//!
//! Tests: All Services → Log Service (src/logservice) → PostgreSQL
//!
//! Audit log coverage:
//! - Datafeed: Market data ingestion, news events (e.g., Trump tweets)
//! - Strategy: Signal calculation based on market data and strategy logic
//! - Order: Order placement, execution, cancellation
//! - Wallet: Balance updates, ledger entries (using REAL business logic)
//! - Export: External system exports (Kafka, HTTP, Google Sheets)

use std::time::Duration;
use tokio::time::sleep;
use serde_json::json;

/// Test audit log for Trump tweet news impact on crypto market
///
/// This test:
/// 1. Starts LogServiceRunner which subscribes to log topics from EMQX
/// 2. Publishes news event logs via internal messager to EMQX
/// 3. Verifies audit trail for compliance
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_datafeed_news_event() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner - subscribes to log topics from EMQX
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Simulate Trump tweet about crypto
    let news_event = json!({
        "event_id": "news_001",
        "source": "twitter",
        "author": "realDonaldTrump",
        "content": "Bitcoin is a scam!",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "impact_analysis": {
            "sentiment": "negative",
            "affected_markets": ["crypto", "BTCUSDT"],
            "predicted_volatility": "high"
        }
    });

    // Publish news ingestion log via internal messager to EMQX
    log_runner.publish_log(
        "datafeed-service",
        "INFO",
        &format!("Ingested news event: {}", news_event["event_id"]),
        Some(&news_event.to_string()),
    ).await.unwrap();

    // Publish impact analysis log via internal messager to EMQX
    log_runner.publish_log(
        "datafeed-service",
        "WARN",
        "High impact news detected - Trump tweet about crypto",
        Some(&json!({
            "event_type": "political_statement",
            "impact_level": "high",
            "affected_symbols": ["BTCUSDT", "ETHUSDT"]
        }).to_string()),
    ).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    log::info!("Logged Trump tweet news event: news_001");

    // Verify audit logs
    let logs = get_logs_by_service(&pool, "datafeed-service").await.unwrap();
    assert!(!logs.is_empty(), "Should have audit logs for news event");

    let warn_logs: Vec<_> = logs.iter().filter(|l| l.level == "WARN").collect();
    assert!(!warn_logs.is_empty(), "Should have WARN level log for high impact news");

    ctx.teardown();
}

/// Test audit log for strategy signal calculation
///
/// This test:
/// 1. Strategy receives market data
/// 2. Calculates trading signal based on strategy logic
/// 3. Logs signal calculation details for audit
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_strategy_signal_calculation() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Simulate strategy signal calculation
    let market_data = json!({
        "symbol": "BTCUSDT",
        "price": 50000.0,
        "rsi": 35.0,
        "macd": -100.0,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    let strategy_state = json!({
        "strategy_id": "momentum_001",
        "position": "flat",
        "entry_signals": {
            "rsi_oversold": true,
            "macd_crossover": false,
            "volume_spike": true
        }
    });

    let signal_result = json!({
        "signal_id": "sig_001",
        "action": "BUY",
        "confidence": 0.75,
        "reason": "RSI oversold + volume spike"
    });

    // Publish logs via internal messager to EMQX
    log_runner.publish_log(
        "strategy-service",
        "DEBUG",
        "Received market data for signal calculation",
        Some(&market_data.to_string()),
    ).await.unwrap();

    log_runner.publish_log(
        "strategy-service",
        "DEBUG",
        "Strategy state before calculation",
        Some(&strategy_state.to_string()),
    ).await.unwrap();

    log_runner.publish_log(
        "strategy-service",
        "INFO",
        &format!("Generated trading signal: {}", signal_result["signal_id"]),
        Some(&signal_result.to_string()),
    ).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    log::info!("Logged strategy signal calculation: sig_001");

    // Verify audit logs
    let logs = get_logs_by_service(&pool, "strategy-service").await.unwrap();
    assert!(!logs.is_empty(), "Should have audit log entries");

    ctx.teardown();
}

/// Test audit log for order service execution
///
/// This test:
/// 1. Order service receives trading signal
/// 2. Places order on exchange
/// 3. Logs order placement and execution
/// 4. Notifies notification service
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_order_execution() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    let order_request = json!({
        "order_id": "ord_001",
        "signal_id": "sig_001",
        "wallet_id": 7001,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "MARKET",
        "quantity": 0.1,
        "estimated_value": 5000.0
    });

    let order_response = json!({
        "order_id": "ord_001",
        "exchange_order_id": "binance_12345",
        "status": "filled",
        "fill_price": 50100.0,
        "fill_quantity": 0.1,
        "fee": 0.5,
        "fee_currency": "USDT"
    });

    // Publish logs via internal messager to EMQX
    log_runner.publish_log(
        "order-service",
        "INFO",
        &format!("Placing order: {}", order_request["order_id"]),
        Some(&order_request.to_string()),
    ).await.unwrap();

    log_runner.publish_log(
        "order-service",
        "INFO",
        &format!("Order executed: {}", order_response["order_id"]),
        Some(&order_response.to_string()),
    ).await.unwrap();

    log_runner.publish_log(
        "order-service",
        "INFO",
        "Notification sent to notification-service",
        Some(&json!({
            "notification_type": "order_filled",
            "wallet_id": 7001,
            "order_id": "ord_001"
        }).to_string()),
    ).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    log::info!("Logged order execution: ord_001");

    // Verify audit logs
    let logs = get_logs_by_service(&pool, "order-service").await.unwrap();
    assert!(!logs.is_empty(), "Should have audit log entries for order");

    ctx.teardown();
}

/// Test audit log for wallet balance update using REAL business logic
///
/// This test:
/// 1. Wallet receives trade event
/// 2. Calls real PostgresWalletUpdater from src/core
/// 3. Logs all changes for audit trail
/// 4. Verifies database consistency
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_wallet_update() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Create wallet service runner with real business logic
    let wallet_runner = WalletServiceRunner::start_for_direct_calls(&ctx.postgres_url)
        .await
        .unwrap();

    let trade_event = SigbotTradeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        lifecycle_id: uuid::Uuid::new_v4().to_string(),
        wallet_id: 7001,
        order_id: 70001,
        trade_id: 700001,
        symbol: "BTCUSDT".to_string(),
        side: "BUY".to_string(),
        price: 50100.0,
        qty: 0.1,
        fee: 0.5,
        fee_asset: Some("USDT".to_string()),
        ts: chrono::Utc::now().timestamp_millis(),
        ..Default::default()
    };

    // Publish trade event received log via internal messager to EMQX
    log_runner.publish_log(
        "wallet-service",
        "INFO",
        &format!("Received trade event: {}", trade_event.id),
        Some(&json!({
            "trade_id": trade_event.trade_id,
            "wallet_id": trade_event.wallet_id,
            "symbol": trade_event.symbol,
            "side": trade_event.side,
            "price": trade_event.price,
            "qty": trade_event.qty,
            "fee": trade_event.fee
        }).to_string()),
    ).await.unwrap();

    // Process trade using REAL business logic from src/core
    wallet_runner.process_trade_event(&trade_event).await.unwrap();

    log::info!("Processed trade through real wallet business logic: trade_id={}", trade_event.trade_id);

    // Log balance after update via internal messager to EMQX
    let (usdt_balance, _) = get_balance(&pool, trade_event.wallet_id, "USDT").await.unwrap().unwrap();
    let (btc_balance, _) = get_balance(&pool, trade_event.wallet_id, "BTC").await.unwrap().unwrap();

    log_runner.publish_log(
        "wallet-service",
        "DEBUG",
        "Balance after update",
        Some(&json!({
            "wallet_id": trade_event.wallet_id,
            "USDT": usdt_balance,
            "BTC": btc_balance,
            "change_usdt": -(trade_event.price * trade_event.qty + trade_event.fee)
        }).to_string()),
    ).await.unwrap();

    // Log ledger entry created via internal messager to EMQX
    let ledgers = get_ledgers_by_wallet(&pool, trade_event.wallet_id).await.unwrap();
    log_runner.publish_log(
        "wallet-service",
        "INFO",
        &format!("Ledger entries created: {}", ledgers.len()),
        Some(&json!({
            "ledger_count": ledgers.len(),
            "trade_id": trade_event.trade_id
        }).to_string()),
    ).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    log::info!("Logged wallet update: trade_id={}", trade_event.trade_id);

    // Verify audit logs
    let logs = get_logs_by_service(&pool, "wallet-service").await.unwrap();
    assert!(!logs.is_empty(), "Should have audit log entries for wallet update");

    // Verify ledger was created by real business logic
    assert_eq!(ledgers.len(), 1, "Should have 1 ledger entry created by real business logic");

    ctx.teardown();
}

/// Test complete audit trail from datafeed to export using REAL business logic
///
/// This test verifies the complete audit trail across all services
/// for a single trade lifecycle, with wallet updates using real code.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_complete_trade_lifecycle() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    let trade_lifecycle_id = "lifecycle_001";

    // 1. Datafeed ingests market data - publish via internal messager to EMQX
    log_runner.publish_log(
        "datafeed-service",
        "INFO",
        "Market data ingested",
        Some(&json!({
            "lifecycle_id": trade_lifecycle_id,
            "stage": 1,
            "symbol": "BTCUSDT",
            "price": 50000.0
        }).to_string()),
    ).await.unwrap();

    // 2. Strategy calculates signal - publish via internal messager to EMQX
    log_runner.publish_log(
        "strategy-service",
        "INFO",
        "Signal calculated",
        Some(&json!({
            "lifecycle_id": trade_lifecycle_id,
            "stage": 2,
            "action": "BUY",
            "confidence": 0.8
        }).to_string()),
    ).await.unwrap();

    // 3. Order service executes - publish via internal messager to EMQX
    log_runner.publish_log(
        "order-service",
        "INFO",
        "Order executed",
        Some(&json!({
            "lifecycle_id": trade_lifecycle_id,
            "stage": 3,
            "fill_price": 50100.0
        }).to_string()),
    ).await.unwrap();

    // 4. Wallet updates balance using REAL business logic
    let wallet_runner = WalletServiceRunner::start_for_direct_calls(&ctx.postgres_url)
        .await
        .unwrap();

    let trade_event = SigbotTradeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        lifecycle_id: trade_lifecycle_id.to_string(),
        wallet_id: 7011,
        order_id: 70011,
        trade_id: 700011,
        symbol: "BTCUSDT".to_string(),
        side: "BUY".to_string(),
        price: 50100.0,
        qty: 0.1,
        fee: 0.5,
        fee_asset: Some("USDT".to_string()),
        ts: chrono::Utc::now().timestamp_millis(),
        ..Default::default()
    };

    wallet_runner.process_trade_event(&trade_event).await.unwrap();

    log_runner.publish_log(
        "wallet-service",
        "INFO",
        "Balance updated (via real PostgresWalletUpdater)",
        Some(&json!({
            "lifecycle_id": trade_lifecycle_id,
            "stage": 4,
            "trade_id": trade_event.trade_id,
            "debit_amount": trade_event.price * trade_event.qty + trade_event.fee
        }).to_string()),
    ).await.unwrap();

    // 5. Export to external systems - publish via internal messager to EMQX
    log_runner.publish_log(
        "export-service",
        "INFO",
        "Trade exported to external systems",
        Some(&json!({
            "lifecycle_id": trade_lifecycle_id,
            "stage": 5,
            "destinations": ["kafka", "google_sheets"]
        }).to_string()),
    ).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    log::info!("Logged complete trade lifecycle: lifecycle_001");

    // Verify complete audit trail
    let all_logs = get_logs(&pool, 100).await.unwrap();
    let lifecycle_logs: Vec<_> = all_logs.iter()
        .filter(|l| l.message.contains("lifecycle_001"))
        .collect();
    assert!(!lifecycle_logs.is_empty(), "Should have log entries for complete lifecycle");

    // Verify wallet update actually happened via real business logic
    let ledgers = get_ledgers_by_wallet(&pool, trade_event.wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 1, "Should have 1 ledger entry created by real business logic");

    ctx.teardown();
}

/// Test audit log query and filtering
///
/// This test verifies that audit logs can be queried and filtered
/// for compliance and investigation purposes.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_audit_log_query_and_filter() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Publish various audit logs via internal messager to EMQX
    let test_logs = vec![
        ("datafeed-service", "INFO", "Market data ingested"),
        ("strategy-service", "INFO", "Signal calculated"),
        ("order-service", "WARN", "Slippage detected"),
        ("wallet-service", "ERROR", "Insufficient balance"),
        ("export-service", "INFO", "Trade exported"),
    ];

    for (service, level, message) in &test_logs {
        log_runner.publish_log(service, level, message, None).await.unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // Test filtering by service
    for (service, _, _) in &test_logs {
        let logs = get_logs_by_service(&pool, service).await.unwrap();
        assert!(!logs.is_empty(), "Should have logs for {}", service);
    }

    // Test filtering by level
    let error_logs = get_logs(&pool, 100).await.unwrap()
        .iter()
        .filter(|l| l.level == "ERROR")
        .collect::<Vec<_>>();
    assert!(!error_logs.is_empty(), "Should have ERROR log");

    log::info!("Verified audit log query and filtering");

    ctx.teardown();
}
