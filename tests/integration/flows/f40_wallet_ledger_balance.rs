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

//! Wallet Ledger & Balance Flow Tests
//!
//! Tests: Trade Event → Wallet Service (src/wallet) → Ledger + Balance + Position
//!
//! These tests use the real PostgresWalletUpdater from src/core/src/modules/wallet/store/transaction/trade_postgres.rs


/// Test wallet ledger and balance update for single trade using real business logic
///
/// This test:
/// 1. Creates a SigbotTradeEvent
/// 2. Calls WalletServiceRunner.process_trade_event() which uses PostgresWalletUpdater
/// 3. Verifies the real business logic correctly updated ledger and balance
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_wallet_ledger_balance_single() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Create wallet service runner with real business logic
    let wallet_runner = WalletServiceRunner::start_for_direct_calls(&ctx.postgres_url)
        .await
        .unwrap();

    // Create test trade event
    let trade_event = SigbotTradeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        lifecycle_id: uuid::Uuid::new_v4().to_string(),
        wallet_id: 4001,
        order_id: 40011,
        trade_id: 400111,
        symbol: "BTCUSDT".to_string(),
        side: "BUY".to_string(),
        price: 50000.0,
        qty: 0.1,
        fee: 5.0,
        fee_asset: Some("USDT".to_string()),
        exchange_order_id: Some("BINANCE_ORDER_001".to_string()),
        exchange_trade_id: Some("BINANCE_TRADE_001".to_string()),
        ts: chrono::Utc::now().timestamp_millis(),
        ..Default::default()
    };

    // Process trade event using REAL business logic from src/core/src/modules/wallet/store/transaction/trade_postgres.rs
    wallet_runner.process_trade_event(&trade_event).await.unwrap();

    info!("Processed trade event through real wallet business logic: wallet_id={}", trade_event.wallet_id);

    // Verify ledger was created by real business logic
    let ledgers = get_ledgers_by_wallet(&pool, trade_event.wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 1, "Should have 1 ledger entry created by real business logic");
    assert_eq!(ledgers[0].trade_id, trade_event.trade_id);
    assert_eq!(ledgers[0].symbol, trade_event.symbol);
    assert_eq!(ledgers[0].side, trade_event.side);

    // Verify balance was updated by real business logic (BUY: debit quote asset, credit base asset)
    let (usdt_balance, _) = get_balance(&pool, trade_event.wallet_id, "USDT").await.unwrap().unwrap();
    let expected_usdt = -(trade_event.price * trade_event.qty + trade_event.fee); // -5005.0
    assert!((usdt_balance - expected_usdt).abs() < 0.01, "USDT balance should be {}", expected_usdt);

    let (btc_balance, _) = get_balance(&pool, trade_event.wallet_id, "BTC").await.unwrap().unwrap();
    let expected_btc = trade_event.qty; // 0.1
    assert!((btc_balance - expected_btc).abs() < 0.0001, "BTC balance should be {}", expected_btc);

    ctx.teardown();
}

/// Test wallet ledger and balance for multiple trades using real business logic
///
/// This test verifies that multiple trades are correctly processed
/// by the real business logic with accurate ledger and balance updates.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_wallet_ledger_balance_multiple() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Create wallet service runner
    let wallet_runner = WalletServiceRunner::start_for_direct_calls(&ctx.postgres_url)
        .await
        .unwrap();

    let wallet_id = 4002;
    let trades = vec![
        (40021, "BUY", 0.01, 50000.0, 0.5),
        (40022, "BUY", 0.02, 51000.0, 1.02),
        (40023, "SELL", 0.015, 49500.0, 0.74),
        (40024, "BUY", 0.025, 50500.0, 1.26),
        (40025, "SELL", 0.03, 51500.0, 1.55),
    ];

    for (trade_id, side, qty, price, fee) in &trades {
        let trade_event = SigbotTradeEvent {
            id: uuid::Uuid::new_v4().to_string(),
            lifecycle_id: uuid::Uuid::new_v4().to_string(),
            wallet_id,
            order_id: *trade_id,
            trade_id: *trade_id,
            symbol: "BTCUSDT".to_string(),
            side: side.to_string(),
            price: *price,
            qty: *qty,
            fee: *fee,
            fee_asset: Some("USDT".to_string()),
            ts: chrono::Utc::now().timestamp_millis(),
            ..Default::default()
        };

        // Process through REAL business logic
        wallet_runner.process_trade_event(&trade_event).await.unwrap();
    }

    info!("Processed {} trades through real wallet business logic for wallet_id={}", trades.len(), wallet_id);

    // Verify ledger entries (1 per trade)
    let ledgers = get_ledgers_by_wallet(&pool, wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), trades.len(), "Should have {} ledger entries", trades.len());

    // Verify final balance
    let (usdt_balance, _) = get_balance(&pool, wallet_id, "USDT").await.unwrap().unwrap();

    // Calculate expected USDT balance
    let mut expected_usdt = 0.0;
    for (_, side, qty, price, fee) in &trades {
        if *side == "BUY" {
            expected_usdt -= qty * price + fee;
        } else {
            expected_usdt += qty * price - fee;
        }
    }

    assert!((usdt_balance - expected_usdt).abs() < 0.01, "Final USDT balance should be {}", expected_usdt);

    ctx.teardown();
}

/// Test wallet ledger, balance and position update using real business logic
///
/// This test verifies that BUY and SELL trades are correctly tracked
/// by the real business logic.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_wallet_ledger_balance_with_position() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Create wallet service runner
    let wallet_runner = WalletServiceRunner::start_for_direct_calls(&ctx.postgres_url)
        .await
        .unwrap();

    let wallet_id = 4003;

    // BUY trade: Acquire BTC
    let buy_event = SigbotTradeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        lifecycle_id: uuid::Uuid::new_v4().to_string(),
        wallet_id,
        order_id: 40031,
        trade_id: 40031,
        symbol: "BTCUSDT".to_string(),
        side: "BUY".to_string(),
        price: 50000.0,
        qty: 0.1,
        fee: 5.0,
        fee_asset: Some("USDT".to_string()),
        ts: chrono::Utc::now().timestamp_millis(),
        ..Default::default()
    };

    wallet_runner.process_trade_event(&buy_event).await.unwrap();
    info!("Processed BUY trade through real business logic: wallet_id={}", wallet_id);

    // Verify BTC balance after BUY
    let (btc_balance, _) = get_balance(&pool, wallet_id, "BTC").await.unwrap().unwrap();
    assert!((btc_balance - 0.1).abs() < 0.0001, "BTC balance should be 0.1 after BUY");

    // SELL trade: Dispose BTC
    let sell_event = SigbotTradeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        lifecycle_id: uuid::Uuid::new_v4().to_string(),
        wallet_id,
        order_id: 40032,
        trade_id: 40032,
        symbol: "BTCUSDT".to_string(),
        side: "SELL".to_string(),
        price: 51000.0,
        qty: 0.05,
        fee: 5.1,
        fee_asset: Some("USDT".to_string()),
        ts: chrono::Utc::now().timestamp_millis(),
        ..Default::default()
    };

    wallet_runner.process_trade_event(&sell_event).await.unwrap();
    info!("Processed SELL trade through real business logic: wallet_id={}", wallet_id);

    // Verify BTC balance after SELL
    let (btc_balance, _) = get_balance(&pool, wallet_id, "BTC").await.unwrap().unwrap();
    let expected_btc = 0.1 - 0.05;
    assert!((btc_balance - expected_btc).abs() < 0.0001, "BTC balance should be {} after SELL", expected_btc);

    // Verify USDT balance
    let (usdt_balance, _) = get_balance(&pool, wallet_id, "USDT").await.unwrap().unwrap();
    // BUY: -5000 - 5 = -5005, SELL: +2550 - 5.1 = +2544.9
    let expected_usdt = -5005.0 + 2544.9;
    assert!((usdt_balance - expected_usdt).abs() < 0.01, "USDT balance should be {}", expected_usdt);

    // Verify ledger entries
    let ledgers = get_ledgers_by_wallet(&pool, wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 2, "Should have 2 ledger entries");

    ctx.teardown();
}
