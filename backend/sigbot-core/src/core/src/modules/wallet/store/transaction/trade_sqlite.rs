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
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::store::sqlite::SQLiteRepository;
use crate::{config::config::SqliteAppDBProperties, modules::wallet::store::transaction::IWalletUpdater};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{error, info};
use sigbot_types::modules::order::events::SigbotTradeEvent;
use sqlx::sqlite::SqlitePool;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SqliteWalletUpdater {
    db_pool: Arc<Mutex<Option<Arc<SqlitePool>>>>,
}

impl SqliteWalletUpdater {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        let repo = SQLiteRepository::<()>::get_or_init(config)
            .await
            .context("Failed to initialize database connection for wallet trade handler")?;
        let pool = repo.get_pool();
        Ok(Self {
            db_pool: Arc::new(Mutex::new(Some(Arc::new(pool.clone())))),
        })
    }

    async fn get_pool(&self) -> Result<Arc<SqlitePool>, Error> {
        let pool_guard = self.db_pool.lock().await;
        pool_guard
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::msg("Database pool not initialized"))
    }
}

#[async_trait]
impl IWalletUpdater for SqliteWalletUpdater {
    async fn upsert(&self, event: &SigbotTradeEvent) -> Result<(), Error> {
        let pool = self.get_pool().await?;

        // Begin transaction
        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        // Insert trade record (append-only, immutable ledger)
        // SQLite uses datetime('now') for timestamp conversion
        sqlx::query(
            "INSERT OR IGNORE INTO s_ledger (wallet_id, order_id, trade_id, symbol, side, price, qty, fee, fee_asset, exchange_order_id, exchange_trade_id, ts)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'))",
        )
        .bind(event.wallet_id)
        .bind(event.order_id)
        .bind(event.trade_id)
        .bind(&event.symbol)
        .bind(&event.side)
        .bind(event.price)
        .bind(event.qty)
        .bind(event.fee)
        .bind(event.fee_asset.as_ref())
        .bind(event.exchange_order_id.as_ref())
        .bind(event.exchange_trade_id.as_ref())
        .bind(event.ts / 1000) // Convert milliseconds to seconds for SQLite
        .execute(&mut *tx)
        .await
        .context("Failed to insert trade record")?;

        // Commit transaction
        tx.commit().await.context("Failed to commit transaction")?;

        info!(
            "Trade record written successfully: wallet_id={}, order_id={}, trade_id={}",
            event.wallet_id, event.order_id, event.trade_id
        );

        // Async trigger derived state update (non-blocking)
        let pool_clone = pool.clone();
        let event_clone = event.clone();
        tokio::spawn(async move {
            if let Err(e) = SqliteWalletUpdater::update_derived_states(&pool_clone, &event_clone).await {
                error!("Failed to update derived states: {}", e);
            }
        });

        Ok(())
    }
}

impl SqliteWalletUpdater {
    /// Update derived states (balances, positions, equity snapshot)
    async fn update_derived_states(pool: &SqlitePool, event: &SigbotTradeEvent) -> Result<(), Error> {
        info!("Updating derived states for trade event: {}", event.idempotency_key());

        // Update balances
        SqliteWalletUpdater::update_balances(pool, event).await?;

        // Update positions
        SqliteWalletUpdater::update_positions(pool, event).await?;

        // Generate equity snapshot
        SqliteWalletUpdater::generate_equity_snapshot(pool, event).await?;

        Ok(())
    }

    /// Update balances (derived from ledger table)
    async fn update_balances(pool: &SqlitePool, event: &SigbotTradeEvent) -> Result<(), Error> {
        let (base_asset, quote_asset) = Self::parse_symbol(&event.symbol)?;

        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        if event.side == "BUY" {
            sqlx::query(
                "INSERT INTO s_balances (wallet_id, asset, available, locked, updated_at)
                 VALUES (?, ?, -(? * ? + CASE WHEN ? = ? THEN ? ELSE 0 END), 0, CURRENT_TIMESTAMP)
                 ON CONFLICT (wallet_id, asset) DO UPDATE SET
                     available = s_balances.available + EXCLUDED.available,
                     updated_at = CURRENT_TIMESTAMP",
            )
            .bind(event.wallet_id)
            .bind(&quote_asset)
            .bind(event.price)
            .bind(event.qty)
            .bind(event.fee_asset.as_ref().unwrap_or(&quote_asset))
            .bind(&quote_asset)
            .bind(event.fee)
            .execute(&mut *tx)
            .await
            .context("Failed to update quote asset balance")?;

            sqlx::query(
                "INSERT INTO s_balances (wallet_id, asset, available, locked, updated_at)
                 VALUES (?, ?, ? - CASE WHEN ? = ? THEN ? ELSE 0 END, 0, CURRENT_TIMESTAMP)
                 ON CONFLICT (wallet_id, asset) DO UPDATE SET
                     available = s_balances.available + EXCLUDED.available,
                     updated_at = CURRENT_TIMESTAMP",
            )
            .bind(event.wallet_id)
            .bind(&base_asset)
            .bind(event.qty)
            .bind(event.fee_asset.as_ref().unwrap_or(&quote_asset))
            .bind(&base_asset)
            .bind(event.fee)
            .execute(&mut *tx)
            .await
            .context("Failed to update base asset balance")?;
        } else {
            sqlx::query(
                "INSERT INTO s_balances (wallet_id, asset, available, locked, updated_at)
                 VALUES (?, ?, -(? + CASE WHEN ? = ? THEN ? ELSE 0 END), 0, CURRENT_TIMESTAMP)
                 ON CONFLICT (wallet_id, asset) DO UPDATE SET
                     available = s_balances.available + EXCLUDED.available,
                     updated_at = CURRENT_TIMESTAMP",
            )
            .bind(event.wallet_id)
            .bind(&base_asset)
            .bind(event.qty)
            .bind(event.fee_asset.as_ref().unwrap_or(&base_asset))
            .bind(&base_asset)
            .bind(event.fee)
            .execute(&mut *tx)
            .await
            .context("Failed to update base asset balance")?;

            sqlx::query(
                "INSERT INTO s_balances (wallet_id, asset, available, locked, updated_at)
                 VALUES (?, ?, ? * ? - CASE WHEN ? = ? THEN ? ELSE 0 END, 0, CURRENT_TIMESTAMP)
                 ON CONFLICT (wallet_id, asset) DO UPDATE SET
                     available = s_balances.available + EXCLUDED.available,
                     updated_at = CURRENT_TIMESTAMP",
            )
            .bind(event.wallet_id)
            .bind(&quote_asset)
            .bind(event.price)
            .bind(event.qty)
            .bind(event.fee_asset.as_ref().unwrap_or(&base_asset))
            .bind(&quote_asset)
            .bind(event.fee)
            .execute(&mut *tx)
            .await
            .context("Failed to update quote asset balance")?;
        }

        tx.commit().await.context("Failed to commit balance update")?;
        Ok(())
    }

    /// Update positions (derived from ledger table)
    async fn update_positions(pool: &SqlitePool, event: &SigbotTradeEvent) -> Result<(), Error> {
        let position_side = if event.side == "BUY" { "LONG" } else { "SHORT" };

        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        sqlx::query(
            "INSERT INTO s_positions (wallet_id, symbol, side, size, entry_price, realized_pnl, updated_at)
             SELECT 
                 ? as wallet_id,
                 ? as symbol,
                 ? as side,
                 SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END) as size,
                 SUM(CASE WHEN side = 'BUY' THEN price * qty ELSE 0 END) / NULLIF(SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END), 0) as entry_price,
                 0 as realized_pnl,
                 CURRENT_TIMESTAMP as updated_at
             FROM s_ledger
             WHERE wallet_id = ? AND symbol = ?
             GROUP BY wallet_id, symbol
             ON CONFLICT (wallet_id, symbol, side) DO UPDATE SET
                 size = EXCLUDED.size,
                 entry_price = EXCLUDED.entry_price,
                 updated_at = CURRENT_TIMESTAMP",
        )
        .bind(event.wallet_id)
        .bind(&event.symbol)
        .bind(position_side)
        .bind(event.wallet_id)
        .bind(&event.symbol)
        .execute(&mut *tx)
        .await
        .context("Failed to update positions")?;

        tx.commit().await.context("Failed to commit position update")?;
        Ok(())
    }

    /// Generate equity snapshot
    async fn generate_equity_snapshot(pool: &SqlitePool, event: &SigbotTradeEvent) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO equity_snapshots (wallet_id, equity, ts)
             SELECT 
                 ? as wallet_id,
                 COALESCE(SUM(available + locked), 0) as equity,
                 CURRENT_TIMESTAMP as ts
             FROM s_balances
             WHERE wallet_id = ?",
        )
        .bind(event.wallet_id)
        .bind(event.wallet_id)
        .execute(pool)
        .await
        .context("Failed to generate equity snapshot")?;

        Ok(())
    }

    /// 解析交易对，返回基础资产和报价资产
    fn parse_symbol(symbol: &str) -> Result<(String, String), Error> {
        let common_quotes = vec!["USDT", "USDC", "BTC", "ETH", "BNB"];
        for quote in common_quotes {
            if symbol.ends_with(quote) {
                let base = symbol
                    .strip_suffix(quote)
                    .ok_or_else(|| Error::msg(format!("Failed to parse symbol: {}", symbol)))?;
                return Ok((base.to_string(), quote.to_string()));
            }
        }
        Err(Error::msg(format!("Failed to parse symbol: {}", symbol)))
    }
}
