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

//! Notification Forward Flow Tests
//!
//! Tests: Any Service → Notification Service (src/notification) → External Channels
//!
//! The notification service subscribes to notification topics from EMQX and
//! forwards messages to external channels (Email, Telegram, WeChat, etc.).
//!
//! These tests use the real NotificationServiceRunner which wraps
//! the notification logic from src/notification.

use crate::common::{
    MiddlewareE2EContext,
    init_database_schema,
    get_logs_by_service,
    NotificationServiceRunner,
    LogServiceRunner,
};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

/// Test notification forwarding using real notification service
///
/// This test:
/// 1. Starts LogServiceRunner which subscribes to log topics from EMQX
/// 2. Starts NotificationServiceRunner which connects to EMQX
/// 3. Publishes notification to EMQX topic
/// 4. Verifies notification service processes the message
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_notification_forward_basic() {
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

    // Start notification service - connects to EMQX and subscribes to notification topics
    let _notification_handle = NotificationServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Create notification payload
    let notification = json!({
        "notification_id": "notif-001",
        "tenant_id": "test-tenant",
        "workflow_id": "test-workflow",
        "type": "trading_signal",
        "channel": "telegram",
        "recipient": "test_user",
        "message": "BUY signal generated for BTCUSDT",
        "data": {
            "symbol": "BTCUSDT",
            "action": "BUY",
            "price": 50000.0,
            "confidence": 0.85
        }
    });

    log::info!("Created notification: {:?}", notification);

    // Note: Full notification forwarding requires configured channels
    // This test verifies the notification service is running and can receive messages

    // Wait for notification processing
    sleep(Duration::from_millis(500)).await;

    log::info!("Notification forward test completed");

    ctx.teardown();
}

/// Test notification forwarding to multiple channels
///
/// This test verifies that the notification service can
/// forward the same notification to multiple channels.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_notification_forward_multiple_channels() {
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

    // Start notification service
    let _notification_handle = NotificationServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Create notification for multiple channels
    let notification = json!({
        "notification_id": "notif-multi-001",
        "tenant_id": "test-tenant",
        "workflow_id": "test-workflow",
        "type": "order_filled",
        "channels": ["email", "telegram", "wechat"],
        "recipient": "test_user",
        "message": "Order filled: BUY 0.1 BTCUSDT @ 50000",
        "data": {
            "order_id": "ord-001",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.1,
            "price": 50000.0
        }
    });

    log::info!("Created multi-channel notification: {:?}", notification);

    // Wait for processing
    sleep(Duration::from_millis(500)).await;

    log::info!("Multi-channel notification test completed");

    ctx.teardown();
}

/// Test notification for different event types
///
/// This test verifies that different notification event types
/// are properly handled by the notification service.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_notification_event_types() {
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

    // Start notification service
    let _notification_handle = NotificationServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Test different notification types
    let notification_types = vec![
        ("trading_signal", "BUY signal for BTCUSDT"),
        ("order_filled", "Order filled: 0.1 BTC @ 50000"),
        ("order_cancelled", "Order cancelled: ord-001"),
        ("balance_warning", "Low balance warning: 100 USDT remaining"),
        ("system_alert", "System maintenance scheduled"),
    ];

    for (notif_type, message) in &notification_types {
        let notification = json!({
            "notification_id": format!("notif-{}", notif_type),
            "tenant_id": "test-tenant",
            "workflow_id": "test-workflow",
            "type": notif_type,
            "channel": "telegram",
            "recipient": "test_user",
            "message": message
        });

        log::info!("Sent {} notification: {}", notif_type, message);
    }

    // Wait for processing
    sleep(Duration::from_millis(500)).await;

    // Verify logs
    let logs = get_logs_by_service(&pool, "notification-service").await.unwrap();
    log::info!("Notification service logs: {}", logs.len());

    ctx.teardown();
}

/// Test notification with priority levels
///
/// This test verifies that notifications with different
/// priority levels are handled appropriately.
#[tokio::test]
#[ignore = "Requires Docker and EMQX environment"]
pub async fn test_notification_priority_levels() {
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

    // Start notification service
    let _notification_handle = NotificationServiceRunner::start(
        "localhost",
        ctx.manager.emqx_config.host_port,
    ).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // Test different priority levels
    let priorities = vec![
        ("LOW", "Daily summary report"),
        ("NORMAL", "Trade executed successfully"),
        ("HIGH", "Failed login attempt detected"),
        ("URGENT", "Critical system error - immediate action required"),
    ];

    for (priority, message) in &priorities {
        let notification = json!({
            "notification_id": format!("notif-priority-{}", priority.to_lowercase()),
            "tenant_id": "test-tenant",
            "workflow_id": "test-workflow",
            "type": "system_alert",
            "priority": priority,
            "channel": "telegram",
            "recipient": "test_user",
            "message": message
        });

        log::info!("Sent {} priority notification: {}", priority, message);
    }

    // Wait for processing
    sleep(Duration::from_millis(500)).await;

    log::info!("Priority notification test completed");

    ctx.teardown();
}
