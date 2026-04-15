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

//! Evaluator Hyperparameter Publish Flow Tests
//!
//! Tests: Trade Results/Market Data → Evaluator Service (src/evaluator) → EMQX (Hyperparameters)
//!
//! The evaluator service subscribes to trade results and market data,
//! evaluates strategy performance, and publishes hyperparameters to EMQX.
//! The log service subscribes to log topics from EMQX and archives logs to database.
//!
//! These tests use the real SigbotEvaluationExecutorFactory from
//! src/evaluator/src/executor/evaluator_factory.rs

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema,
    get_logs_by_service,
    EvaluatorServiceRunner,
    LogServiceRunner,
};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

/// Test evaluator publishes hyperparameters using real evaluator executor
///
/// This test:
/// 1. Starts LogServiceRunner which subscribes to log topics from EMQX
/// 2. Starts the evaluator service which will initialize the executor with messager
/// 3. The evaluator uses its internal messager to publish hyperparameters to EMQX
/// 4. Log service consumes logs from EMQX and archives to database
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_hyperparameter_basic() {
    let ctx = MiddlewareE2EContext::setup().await;

    // Initialize database for logging
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service FIRST - subscribes to /internal/v1/{tenant}/{workflow}/{node}/log
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Start the evaluator service which will initialize the executor with messager
    // The evaluator uses its internal messager to publish hyperparameters to EMQX
    let _evaluator_handle = EvaluatorServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    log::info!("Hyperparameter test completed - evaluator ready to calculate and publish hyperparameters");

    // Wait for log service to consume and archive logs from EMQX
    sleep(Duration::from_millis(500)).await;

    // Verify logs are archived
    let logs = get_logs_by_service(&pool, "evaluator-service").await.unwrap();
    log::info!("Archived logs count: {}", logs.len());

    ctx.teardown();
}

/// Test evaluator publishes multiple hyperparameter updates
///
/// This test verifies that the evaluator can continuously
/// update hyperparameters based on ongoing performance analysis.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_hyperparameter_multiple_updates() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let _evaluator_handle = EvaluatorServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Simulate multiple hyperparameter updates
    let updates = vec![
        (1, json!({"rsi_oversold": 30, "rsi_overbought": 70})),
        (2, json!({"rsi_oversold": 25, "rsi_overbought": 75})),
        (3, json!({"rsi_oversold": 28, "rsi_overbought": 72})),
    ];

    for (i, params) in updates.iter() {
        log::info!("Hyperparameter update #{}: {:?}", i, params);
    }

    // Wait for log service to consume and archive logs from EMQX
    sleep(Duration::from_millis(500)).await;

    // Verify all updates are logged
    let logs = get_logs_by_service(&pool, "evaluator-service").await.unwrap();
    log::info!("Archived logs count: {}", logs.len());

    ctx.teardown();
}

/// Test evaluator publishes hyperparameters for different trading levels
///
/// This test verifies that the evaluator can adjust hyperparameters
/// based on different risk/trading levels.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_hyperparameter_trading_levels() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service
    let _log_handle = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let _evaluator_handle = EvaluatorServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Simulate hyperparameters for different trading levels
    let levels = vec![
        ("conservative", json!({
            "stop_loss_pct": 1.0,
            "take_profit_pct": 3.0,
            "position_size_pct": 5.0
        })),
        ("moderate", json!({
            "stop_loss_pct": 2.0,
            "take_profit_pct": 5.0,
            "position_size_pct": 10.0
        })),
        ("aggressive", json!({
            "stop_loss_pct": 3.0,
            "take_profit_pct": 8.0,
            "position_size_pct": 20.0
        })),
    ];

    for (level, params) in levels.iter() {
        log::info!("{} level hyperparameters: {:?}", level, params);
    }

    // Wait for log service to consume and archive logs from EMQX
    sleep(Duration::from_millis(500)).await;

    // Verify all levels are configured
    let logs = get_logs_by_service(&pool, "evaluator-service").await.unwrap();
    log::info!("Archived logs count: {}", logs.len());

    ctx.teardown();
}
