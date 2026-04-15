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

//! TimescaleDB Test Utilities for E2E Tests
//!
//! Provides helper functions for TimescaleDB time-series operations.
//! Used for storing and querying kline/candlestick data in backtest scenarios.

use sqlx::postgres::PgPool;
use anyhow::Result;
use chrono::{DateTime, Utc};

/// TimescaleDB test utilities
pub struct TimescaleDBTestUtils {
    pub pool: PgPool,
}

impl TimescaleDBTestUtils {
    /// Create new TimescaleDB test utilities
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create from MiddlewareE2EContext
    pub async fn from_context(ctx: &crate::common::MiddlewareE2EContext) -> Result<Self> {
        let pool = PgPool::connect(&ctx.timescaledb_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to TimescaleDB: {}", e))?;
        Ok(Self::new(pool))
    }

    /// Initialize klines hypertable schema
    pub async fn init_klines_schema(&self) -> Result<()> {
        // Create klines table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS klines (
                time TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                interval VARCHAR(20) NOT NULL,
                open DECIMAL NOT NULL,
                high DECIMAL NOT NULL,
                low DECIMAL NOT NULL,
                close DECIMAL NOT NULL,
                volume DECIMAL NOT NULL,
                quote_volume DECIMAL NOT NULL,
                trades_count BIGINT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Convert to hypertable (TimescaleDB specific)
        sqlx::query(
            "SELECT create_hypertable('klines', 'time', if_not_exists => TRUE)",
        )
        .execute(&self.pool)
        .await?;

        // Create indexes
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_klines_symbol_interval ON klines (symbol, interval, time DESC)",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert a kline record
    pub async fn insert_kline(
        &self,
        time: DateTime<Utc>,
        symbol: &str,
        interval: &str,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        quote_volume: f64,
        trades_count: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO klines (time, symbol, interval, open, high, low, close, volume, quote_volume, trades_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(time)
        .bind(symbol)
        .bind(interval)
        .bind(open)
        .bind(high)
        .bind(low)
        .bind(close)
        .bind(volume)
        .bind(quote_volume)
        .bind(trades_count)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert multiple kline records (batch)
    pub async fn insert_klines_batch(&self, klines: &[KlineRecord]) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for kline in klines {
            sqlx::query(
                r#"
                INSERT INTO klines (time, symbol, interval, open, high, low, close, volume, quote_volume, trades_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(kline.time)
            .bind(&kline.symbol)
            .bind(&kline.interval)
            .bind(kline.open)
            .bind(kline.high)
            .bind(kline.low)
            .bind(kline.close)
            .bind(kline.volume)
            .bind(kline.quote_volume)
            .bind(kline.trades_count)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Get klines by symbol and interval
    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
    ) -> Result<Vec<KlineRecord>> {
        let results = sqlx::query_as::<_, KlineRecord>(
            r#"
            SELECT time, symbol, interval, open, high, low, close, volume, quote_volume, trades_count, created_at
            FROM klines
            WHERE symbol = $1 AND interval = $2
            ORDER BY time DESC
            LIMIT $3
            "#,
        )
        .bind(symbol)
        .bind(interval)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    /// Get klines in time range
    pub async fn get_klines_in_range(
        &self,
        symbol: &str,
        interval: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<KlineRecord>> {
        let results = sqlx::query_as::<_, KlineRecord>(
            r#"
            SELECT time, symbol, interval, open, high, low, close, volume, quote_volume, trades_count, created_at
            FROM klines
            WHERE symbol = $1 AND interval = $2 AND time >= $3 AND time <= $4
            ORDER BY time ASC
            "#,
        )
        .bind(symbol)
        .bind(interval)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    /// Get latest kline
    pub async fn get_latest_kline(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<Option<KlineRecord>> {
        let result = sqlx::query_as::<_, KlineRecord>(
            r#"
            SELECT time, symbol, interval, open, high, low, close, volume, quote_volume, trades_count, created_at
            FROM klines
            WHERE symbol = $1 AND interval = $2
            ORDER BY time DESC
            LIMIT 1
            "#,
        )
        .bind(symbol)
        .bind(interval)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    /// Get klines count
    pub async fn get_klines_count(&self, symbol: &str, interval: &str) -> Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) FROM klines WHERE symbol = $1 AND interval = $2
            "#,
        )
        .bind(symbol)
        .bind(interval)
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Clear klines table
    pub async fn clear_klines(&self) -> Result<()> {
        sqlx::query("TRUNCATE TABLE klines").execute(&self.pool).await?;
        Ok(())
    }

    /// Drop klines table
    pub async fn drop_klines(&self) -> Result<()> {
        sqlx::query("DROP TABLE IF EXISTS klines").execute(&self.pool).await?;
        Ok(())
    }
}

/// Kline record structure
#[derive(Debug, sqlx::FromRow)]
pub struct KlineRecord {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub interval: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades_count: i64,
    pub created_at: DateTime<Utc>,
}

/// Create TimescaleDB test utilities from MiddlewareE2EContext
pub async fn create_timescaledb_utils(ctx: &crate::common::MiddlewareE2EContext) -> Result<TimescaleDBTestUtils> {
    TimescaleDBTestUtils::from_context(ctx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::MiddlewareE2EContext;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_timescaledb_klines_basic() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = TimescaleDBTestUtils::from_context(&ctx).await.unwrap();

        // Initialize schema
        utils.init_klines_schema().await.unwrap();

        // Insert kline
        let now = Utc::now();
        utils.insert_kline(
            now,
            "BTCUSDT",
            "1m",
            50000.0,
            50100.0,
            49900.0,
            50050.0,
            100.0,
            5005000.0,
            50,
        ).await.unwrap();

        // Get klines
        let klines = utils.get_klines("BTCUSDT", "1m", 10).await.unwrap();
        assert_eq!(klines.len(), 1);
        assert_eq!(klines[0].symbol, "BTCUSDT");
        assert_eq!(klines[0].close, 50050.0);

        // Get count
        let count = utils.get_klines_count("BTCUSDT", "1m").await.unwrap();
        assert_eq!(count, 1);

        ctx.teardown();
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_timescaledb_klines_batch() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = TimescaleDBTestUtils::from_context(&ctx).await.unwrap();

        // Initialize schema
        utils.init_klines_schema().await.unwrap();

        // Insert batch klines
        let mut klines = Vec::new();
        let base_time = Utc::now();
        for i in 0..10 {
            klines.push(KlineRecord {
                time: base_time - chrono::Duration::minutes(i as i64),
                symbol: "ETHUSDT".to_string(),
                interval: "1m".to_string(),
                open: 3000.0 + i as f64,
                high: 3010.0 + i as f64,
                low: 2990.0 + i as f64,
                close: 3005.0 + i as f64,
                volume: 50.0,
                quote_volume: 150000.0,
                trades_count: 25,
                created_at: Utc::now(),
            });
        }
        utils.insert_klines_batch(&klines).await.unwrap();

        // Get klines
        let results = utils.get_klines("ETHUSDT", "1m", 20).await.unwrap();
        assert_eq!(results.len(), 10);

        ctx.teardown();
    }
}
