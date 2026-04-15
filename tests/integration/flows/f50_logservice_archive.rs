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

//! Log Service Archive Flow Tests
//!
//! Tests: Services → Log Service (src/logservice) → PostgreSQL
//!
//! The log service collects logs from various services and archives
//! them to PostgreSQL for long-term storage and analysis.
//!
//! These tests use the real LogServiceRunner which wraps
//! SigbotLogManagerFactory from src/logservice.

use std::time::Duration;
use tokio::time::sleep;

/// Test log archiving using real LogServiceRunner
///
/// This test:
/// 1. Starts LogServiceRunner which subscribes to log topics from EMQX
/// 2. Publishes log to EMQX via LogServiceRunner's internal messager
/// 3. Verifies logs are archived to PostgreSQL by Log Service
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_log_archive_basic() {
    let ctx = MiddlewareE2EContext::setup().await;

    // Initialize database
    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner - subscribes to /internal/v1/{tenant}/{workflow}/{node}/log
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Publish log via internal messager to EMQX
    log_runner.publish_log("test-service", "INFO", "Test log message", None)
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // Verify
    let logs = get_logs(&pool, 10).await.unwrap();
    assert!(!logs.is_empty());
    assert_eq!(logs[0].service_name, "test-service");
    assert_eq!(logs[0].level, "INFO");
    assert_eq!(logs[0].message, "Test log message");

    ctx.teardown();
}

/// Test log archiving with different log levels using real service
///
/// This test verifies that logs with different severity levels
/// are properly archived by the real log service.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_log_archive_levels() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner - subscribes to log topics from EMQX
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Publish logs with different levels via internal messager to EMQX
    for level in &["DEBUG", "INFO", "WARN", "ERROR"] {
        log_runner.publish_log("test-service", level, &format!("{} level log", level), None)
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // Verify all levels are archived
    let logs = get_logs(&pool, 10).await.unwrap();
    assert_eq!(logs.len(), 4);

    let levels: Vec<_> = logs.iter().map(|l| l.level.as_str()).collect();
    assert!(levels.contains(&"DEBUG"));
    assert!(levels.contains(&"INFO"));
    assert!(levels.contains(&"WARN"));
    assert!(levels.contains(&"ERROR"));

    ctx.teardown();
}

/// Test log archiving from multiple services using real service
///
/// This test verifies that the real log service can handle logs
/// from multiple source services.
#[tokio::test]
#[ignore = "Requires Docker environment"]
pub async fn test_log_archive_multiple_services() {
    let ctx = MiddlewareE2EContext::setup().await;

    let pool = sqlx::PgPool::connect(&ctx.postgres_url).await.unwrap();
    init_database_schema(&pool).await.unwrap();

    // Start log service runner - subscribes to log topics from EMQX
    let log_runner = LogServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
        &ctx.postgres_url,
    ).await.unwrap();

    // Publish logs from different services via internal messager to EMQX
    let services = vec!["datafeed-service", "strategy-service", "order-service", "wallet-service"];
    for service in &services {
        log_runner.publish_log(service, "INFO", &format!("Log from {}", service), None)
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // Verify logs from all services
    let logs = get_logs(&pool, 10).await.unwrap();
    assert_eq!(logs.len(), 4);

    for service in &services {
        let service_logs: Vec<_> = logs.iter().filter(|l| l.service_name == *service).collect();
        assert_eq!(service_logs.len(), 1, "Should have 1 log from {}", service);
    }

    ctx.teardown();
}
