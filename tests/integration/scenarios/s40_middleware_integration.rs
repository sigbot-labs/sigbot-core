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

//! Scenario Tests: Middleware Integration
//!
//! Direct middleware integration verification tests

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema,
    insert_ledger, get_ledgers_by_wallet,
    init_balance, update_balance, get_balance,
    insert_log, get_logs,
};

/// Test database insert operations
#[tokio::test]
#[ignore = "Requires Docker environment"]
async fn test_middleware_database_insert() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Insert test data
    let wallet_id = 1001;
    init_balance(&pool, wallet_id, "USDT", 100000.0).await.unwrap();
    insert_ledger(&pool, wallet_id, 1, 5000.0, "USDT", "DEBIT").await.unwrap();

    // Verify
    let ledgers = get_ledgers_by_wallet(&pool, wallet_id).await.unwrap();
    assert_eq!(ledgers.len(), 1);

    ctx.teardown();
}

/// Test balance transaction
#[tokio::test]
#[ignore = "Requires Docker environment"]
async fn test_middleware_balance_transaction() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    let wallet_id = 1002;
    let initial = 100000.0;
    init_balance(&pool, wallet_id, "USDT", initial).await.unwrap();

    // Multiple transactions
    update_balance(&pool, wallet_id, "USDT", -1000.0).await.unwrap();
    update_balance(&pool, wallet_id, "USDT", -2000.0).await.unwrap();
    update_balance(&pool, wallet_id, "USDT", 500.0).await.unwrap();

    // Verify
    let (balance, _) = get_balance(&pool, wallet_id, "USDT").await.unwrap().unwrap();
    assert!((balance - (initial - 1000.0 - 2000.0 + 500.0)).abs() < 0.01);

    ctx.teardown();
}

/// Test log archiving
#[tokio::test]
#[ignore = "Requires Docker environment"]
async fn test_middleware_log_archiving() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Insert logs
    for level in &["DEBUG", "INFO", "WARN", "ERROR"] {
        insert_log(&pool, "test-service", level, &format!("{} log", level), None)
            .await
            .unwrap();
    }

    // Verify
    let logs = get_logs(&pool, 10).await.unwrap();
    assert_eq!(logs.len(), 4);

    ctx.teardown();
}

/// Test Kafka produce operations
#[tokio::test]
#[ignore = "Requires Docker environment"]
async fn test_middleware_kafka_produce() {
    let ctx = MiddlewareE2EContext::setup().await;

    let kafka_utils = KafkaTestUtils::from_context(&ctx);

    // Create test topic
    kafka_utils.create_topic("test-topic", 1).await.unwrap();

    // Produce message
    kafka_utils
        .produce_message("test-topic", "key1", "value1")
        .await
        .unwrap();

    // Verify topic exists
    let topics = kafka_utils.list_topics().await.unwrap();
    assert!(topics.contains(&"test-topic".to_string()));

    ctx.teardown();
}

/// Test all containers integration
#[tokio::test]
#[ignore = "Requires Docker environment"]
async fn test_middleware_all_containers_integration() {
    let ctx = MiddlewareE2EContext::setup().await;

    // Verify all connection strings are valid
    assert!(!ctx.postgres_url.is_empty());
    assert!(!ctx.timescaledb_url.is_empty());
    assert!(!ctx.redis_url.is_empty());
    assert!(!ctx.kafka_bootstrap_servers.is_empty());
    assert!(!ctx.minio_endpoint.is_empty());
    assert!(!ctx.mqtt_broker_url.is_empty());

    // Test PostgreSQL connection
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    let result = sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Test TimescaleDB connection
    let ts_pool = sqlx::PgPool::connect(&ctx.timescaledb_url).await.unwrap();
    let result = sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&ts_pool)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Test Redis connection
    let redis_client = redis::Client::open(ctx.redis_url.as_str()).unwrap();
    let mut conn = redis_client.get_connection().unwrap();
    let pong: String = redis::cmd("PING").query(&mut conn).unwrap();
    assert_eq!(pong, "PONG");

    // Test Kafka
    let kafka_utils = KafkaTestUtils::from_context(&ctx);
    let topics = kafka_utils.list_topics().await.unwrap();
    assert!(topics.contains(&ctx.kafka_export_topic.clone()));

    // Test MinIO health
    let client = reqwest::Client::new();
    let health_url = format!("{}minio/health/live", ctx.minio_endpoint);
    let response = client.get(&health_url).send().await.unwrap();
    assert!(response.status().is_success());

    // Test EMQX health
    let emqx_url = format!("http://localhost:{}/status", ctx.manager.emqx_config.host_dashboard_port);
    let response = client.get(&emqx_url).send().await.unwrap();
    assert!(response.status().is_success());

    log::info!("All middleware containers verified successfully");

    ctx.teardown();
}
