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

//! Scenario Tests: Concurrent Signals
//!
//! High volume and concurrent signal processing tests

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema, init_balance, update_balance, get_balance,
};
use std::time::Duration;
use tokio::time::sleep;

/// Test high volume signal processing
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_concurrent_high_volume() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1001;
    init_balance(&pool, wallet_id, "USDT", 1000000.0).await.unwrap();

    // Simulate high volume trades
    let mut handles = Vec::new();

    for i in 0..100 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let amount = 100.0;
            update_balance(&pool_clone, wallet_id, "USDT", -amount).await.unwrap();
            sleep(Duration::from_millis(1)).await;
            i
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let results = futures::future::join_all(handles).await;
    assert_eq!(results.len(), 100);

    // Verify balance decreased
    let (balance, _) = get_balance(&pool, wallet_id, "USDT").await.unwrap().unwrap();
    assert!(balance < 1000000.0);

    ctx.teardown();
}

/// Test concurrent buy and sell mix
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_concurrent_buy_sell_mix() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1002;
    init_balance(&pool, wallet_id, "USDT", 100000.0).await.unwrap();

    let mut handles = Vec::new();

    // Concurrent buys
    for _ in 0..10 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            update_balance(&pool_clone, wallet_id, "USDT", -1000.0).await.unwrap();
            "BUY"
        });
        handles.push(handle);
    }

    // Concurrent sells
    for _ in 0..10 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            update_balance(&pool_clone, wallet_id, "USDT", 1000.0).await.unwrap();
            "SELL"
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    // Balance should be back to initial (10 buys * -1000 + 10 sells * 1000 = 0)
    let (balance, _) = get_balance(&pool, wallet_id, "USDT").await.unwrap().unwrap();
    assert!((balance - 100000.0).abs() < 0.01);

    ctx.teardown();
}

/// Test concurrent deduplication
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_concurrent_deduplication() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    use std::collections::HashSet;
    use tokio::sync::Mutex;

    let processed_ids = Arc::new(Mutex::new(HashSet::new()));
    let mut handles = Vec::new();

    // Simulate duplicate message IDs
    let duplicate_id = "msg-001";

    for i in 0..10 {
        let processed_clone = processed_ids.clone();
        let id = if i < 5 { duplicate_id } else { &format!("msg-{:03}", i) };
        let id_str = id.to_string();

        let handle = tokio::spawn(async move {
            let mut set = processed_clone.lock().await;
            let is_duplicate = !set.insert(id_str.clone());
            (i, is_duplicate)
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let duplicates: usize = results.iter().filter(|(_, is_dup)| *is_dup).count();

    // Some messages should be detected as duplicates
    assert!(duplicates > 0);

    ctx.teardown();
}
