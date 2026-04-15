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

//! Real Database Test Utilities
//!
//! Provides database schema initialization and helper functions for E2E tests
//! using real PostgreSQL containers.

use sqlx::{postgres::PgPool, Row};
use anyhow::Result;

/// Initialize database schema for E2E tests
pub async fn init_database_schema(pool: &PgPool) -> Result<()> {
    // Create logs table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS logs (
            id BIGINT PRIMARY KEY,
            service_name VARCHAR(255) NOT NULL,
            log_type VARCHAR(50) NOT NULL,
            level VARCHAR(20) NOT NULL,
            content TEXT NOT NULL,
            source TEXT,
            tags TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create ledgers table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ledgers (
            id BIGINT PRIMARY KEY,
            wallet_id BIGINT NOT NULL,
            trade_id BIGINT NOT NULL,
            amount DECIMAL NOT NULL,
            currency VARCHAR(20) NOT NULL,
            entry_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create balances table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS balances (
            id BIGINT PRIMARY KEY,
            wallet_id BIGINT NOT NULL,
            currency VARCHAR(20) NOT NULL,
            available DECIMAL NOT NULL DEFAULT 0,
            frozen DECIMAL NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(wallet_id, currency)
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create positions table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS positions (
            id BIGINT PRIMARY KEY,
            wallet_id BIGINT NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            side VARCHAR(20) NOT NULL,
            quantity DECIMAL NOT NULL,
            entry_price DECIMAL NOT NULL,
            unrealized_pnl DECIMAL DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(wallet_id, symbol)
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create trades table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS trades (
            id BIGINT PRIMARY KEY,
            wallet_id BIGINT NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            side VARCHAR(20) NOT NULL,
            quantity DECIMAL NOT NULL,
            price DECIMAL NOT NULL,
            fee DECIMAL DEFAULT 0,
            fee_asset VARCHAR(20) DEFAULT 'USDT',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create sequence table for ID generation
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS id_sequences (
            name VARCHAR(100) PRIMARY KEY,
            current_value BIGINT NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Get next ID from sequence (auto-increment simulation)
pub async fn next_id(pool: &PgPool, sequence_name: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO id_sequences (name, current_value)
        VALUES ($1, 1)
        ON CONFLICT (name) DO UPDATE SET current_value = id_sequences.current_value + 1
        RETURNING current_value
        "#,
    )
    .bind(sequence_name)
    .fetch_one(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to get next ID: {}", e))
}

/// Initialize a balance for a wallet
pub async fn init_balance(
    pool: &PgPool,
    wallet_id: i64,
    currency: &str,
    amount: f64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO balances (id, wallet_id, currency, available, frozen)
        VALUES ($1, $2, $3, $4, 0)
        ON CONFLICT (wallet_id, currency) DO UPDATE
        SET available = balances.available + $4
        "#,
    )
    .bind(next_id(pool, "balance").await?)
    .bind(wallet_id)
    .bind(currency)
    .bind(amount)
    .execute(pool)
    .await?;

    Ok(())
}

/// Update balance (atomic operation)
pub async fn update_balance(
    pool: &PgPool,
    wallet_id: i64,
    currency: &str,
    delta: f64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE balances
        SET available = available + $3, updated_at = NOW()
        WHERE wallet_id = $1 AND currency = $2
        "#,
    )
    .bind(wallet_id)
    .bind(currency)
    .bind(delta)
    .execute(pool)
    .await?;

    Ok(())
}

/// Get balance
pub async fn get_balance(
    pool: &PgPool,
    wallet_id: i64,
    currency: &str,
) -> Result<Option<(f64, f64)>> {
    let result = sqlx::query_as::<_, (f64, f64)>(
        "SELECT available, frozen FROM balances WHERE wallet_id = $1 AND currency = $2",
    )
    .bind(wallet_id)
    .bind(currency)
    .fetch_optional(pool)
    .await?;

    Ok(result)
}

/// Insert ledger entry
pub async fn insert_ledger(
    pool: &PgPool,
    wallet_id: i64,
    trade_id: i64,
    amount: f64,
    currency: &str,
    entry_type: &str,
) -> Result<i64> {
    let id = next_id(pool, "ledgers").await?;
    sqlx::query(
        r#"
        INSERT INTO ledgers (id, wallet_id, trade_id, amount, currency, entry_type)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(id)
    .bind(wallet_id)
    .bind(trade_id)
    .bind(amount)
    .bind(currency)
    .bind(entry_type)
    .execute(pool)
    .await?;

    Ok(id)
}

/// Get ledgers by wallet
pub async fn get_ledgers_by_wallet(
    pool: &PgPool,
    wallet_id: i64,
) -> Result<Vec<(i64, i64, i64, f64, String, String)>> {
    let results = sqlx::query_as::<_, (i64, i64, i64, f64, String, String)>(
        "SELECT id, wallet_id, trade_id, amount, currency, entry_type FROM ledgers WHERE wallet_id = $1 ORDER BY created_at",
    )
    .bind(wallet_id)
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Insert log entry
pub async fn insert_log(
    pool: &PgPool,
    service_name: &str,
    level: &str,
    content: &str,
    source: Option<&str>,
) -> Result<i64> {
    let id = next_id(pool, "logs").await?;
    sqlx::query(
        r#"
        INSERT INTO logs (id, service_name, log_type, level, content, source)
        VALUES ($1, $2, 'WORKFLOW', $3, $4, $5)
        "#,
    )
    .bind(id)
    .bind(service_name)
    .bind(level)
    .bind(content)
    .bind(source)
    .execute(pool)
    .await?;

    Ok(id)
}

/// Get logs
pub async fn get_logs(pool: &PgPool, limit: usize) -> Result<Vec<LogRecord>> {
    let results = sqlx::query(
        "SELECT id, service_name, log_type, level, content, source, tags, created_at FROM logs ORDER BY created_at DESC LIMIT $1",
    )
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    let logs = results
        .into_iter()
        .map(|row| {
            let id: i64 = row.get("id");
            let service_name: String = row.get("service_name");
            let log_type: String = row.get("log_type");
            let level: String = row.get("level");
            let content: String = row.get("content");
            let source: Option<String> = row.get("source");
            let tags: Option<String> = row.get("tags");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

            LogRecord {
                id,
                service_name,
                log_type,
                level,
                content,
                source,
                tags,
                created_at,
            }
        })
        .collect();

    Ok(logs)
}

/// Log record structure
#[derive(Debug)]
pub struct LogRecord {
    pub id: i64,
    pub service_name: String,
    pub log_type: String,
    pub level: String,
    pub content: String,
    pub source: Option<String>,
    pub tags: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Get logs by service name
pub async fn get_logs_by_service(pool: &PgPool, service_name: &str) -> Result<Vec<LogRecord>> {
    let results = sqlx::query(
        "SELECT id, service_name, log_type, level, content, source, tags, created_at FROM logs WHERE service_name = $1 ORDER BY created_at DESC",
    )
    .bind(service_name)
    .fetch_all(pool)
    .await?;

    let logs = results
        .into_iter()
        .map(|row| {
            let id: i64 = row.get("id");
            let service_name: String = row.get("service_name");
            let log_type: String = row.get("log_type");
            let level: String = row.get("level");
            let content: String = row.get("content");
            let source: Option<String> = row.get("source");
            let tags: Option<String> = row.get("tags");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

            LogRecord {
                id,
                service_name,
                log_type,
                level,
                content,
                source,
                tags,
                created_at,
            }
        })
        .collect();

    Ok(logs)
}

/// Clear all tables
pub async fn clear_all_tables(pool: &PgPool) -> Result<()> {
    sqlx::query("TRUNCATE TABLE ledgers, balances, positions, logs, trades, id_sequences RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}
