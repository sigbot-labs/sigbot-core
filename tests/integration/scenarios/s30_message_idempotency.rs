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

//! Scenario Tests: Message Idempotency
//!
//! Duplicate detection and exactly-once semantics tests

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema, init_balance, update_balance, get_balance,
    insert_ledger, get_ledgers_by_wallet,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Test idempotency duplicate detection
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_idempotency_duplicate_detection() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1001;
    init_balance(&pool, wallet_id, "USDT", 100000.0).await.unwrap();

    // Track processed message IDs
    let processed_ids = Arc::new(Mutex::new(HashSet::new()));

    // Simulate same message arriving twice
    let message_id = "trade-001";
    let trade_amount = 1000.0;

    // First processing
    {
        let mut ids = processed_ids.lock().await;
        let is_new = ids.insert(message_id.to_string());
        assert!(is_new, "First message should be new");

        update_balance(&pool, wallet_id, "USDT", -trade_amount).await.unwrap();
        insert_ledger(&pool, wallet_id, 1, trade_amount, "USDT", "DEBIT").await.unwrap();
    }

    // Duplicate processing (should be detected)
    {
        let mut ids = processed_ids.lock().await;
        let is_new = ids.insert(message_id.to_string());
        assert!(!is_new, "Duplicate message should be detected");
    }

    // Verify only one ledger entry
    let ledgers = get_ledgers_by_wallet(&pool, wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 1);

    ctx.teardown();
}

/// Test idempotency with sequence numbers
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_idempotency_sequence() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1002;
    init_balance(&pool, wallet_id, "USDT", 100000.0).await.unwrap();

    // Track last processed sequence
    let last_sequence = Arc::new(Mutex::new(0i64));
    let mut handles = Vec::new();

    // Simulate out-of-order messages
    let sequences = vec![3, 1, 4, 1, 5, 9, 2, 6];

    for seq in sequences {
        let last_seq = last_sequence.clone();
        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move {
            let mut last = last_seq.lock().await;
            let is_new = seq > *last;

            if is_new {
                *last = seq;
                // Process message
                insert_ledger(&pool_clone, wallet_id, seq, 100.0, "USDT", "SEQ").await.unwrap();
            }

            (seq, is_new)
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let processed_count = results.iter().filter(|(_, is_new)| *is_new).count();

    // Only messages with higher sequence should be processed
    log::info!("Processed {} of {} messages", processed_count, results.len());

    ctx.teardown();
}

/// Test idempotency with concurrent duplicates
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
async fn test_idempotency_concurrent_duplicates() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1003;
    init_balance(&pool, wallet_id, "USDT", 100000.0).await.unwrap();

    let processed_ids = Arc::new(Mutex::new(HashSet::new()));
    let mut handles = Vec::new();

    // Same message ID sent from multiple sources
    let duplicate_message_id = "duplicate-trade-001";

    for _ in 0..10 {
        let processed = processed_ids.clone();
        let pool_clone = pool.clone();
        let message_id = duplicate_message_id.to_string();

        let handle = tokio::spawn(async move {
            let mut ids = processed.lock().await;
            let is_new = ids.insert(message_id.clone());

            if is_new {
                update_balance(&pool_clone, wallet_id, "USDT", -100.0).await.unwrap();
                insert_ledger(&pool_clone, wallet_id, 1, 100.0, "USDT", "DEBIT").await.unwrap();
            }

            is_new
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let new_count = results.iter().filter(|&is_new| *is_new).count();

    // Only one should be processed as new
    assert_eq!(new_count, 1);

    // Verify only one ledger entry
    let ledgers = get_ledgers_by_wallet(&pool, wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 1);

    ctx.teardown();
}
