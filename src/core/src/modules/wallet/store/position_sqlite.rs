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

use crate::config::config::SqliteAppDBProperties;
use crate::store::sqlite::SQLiteRepository;
use crate::store::AsyncRepository;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::wallet::position::PositionInfo;
use sigbot_types::{PageRequest, PageResponse};
use sqlx::Row;

pub struct PositionInfoSQLiteRepository {
    inner: SQLiteRepository<PositionInfo>,
}

impl PositionInfoSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(PositionInfoSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<PositionInfo> for PositionInfoSQLiteRepository {
    async fn select(
        &self,
        position: PositionInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<PositionInfo>), Error> {
        let mut conditions = Vec::new();
        let mut params: Vec<String> = Vec::new();

        if position.wallet_id != 0 {
            conditions.push("wallet_id = ?".to_string());
            params.push(position.wallet_id.to_string());
        }
        if !position.symbol.is_empty() {
            conditions.push("symbol = ?".to_string());
            params.push(position.symbol.clone());
        }

        let where_clause = if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        };

        // Count total records
        let count_query = format!("SELECT COUNT(1) FROM s_positions WHERE {}", where_clause);
        let mut count_query_builder = sqlx::query(&count_query);
        for param in &params {
            count_query_builder = count_query_builder.bind(param);
        }
        let total_count: i64 = count_query_builder.fetch_one(self.inner.get_pool()).await?.get(0);

        // Query data with pagination
        let query = format!(
            "SELECT wallet_id, symbol, side, size, entry_price, realized_pnl, updated_at FROM s_positions WHERE {} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            where_clause
        );
        let mut query_builder = sqlx::query_as::<_, PositionInfo>(&query);
        for param in &params {
            query_builder = query_builder.bind(param);
        }
        query_builder = query_builder.bind(page.get_limit() as i64);
        query_builder = query_builder.bind(page.get_offset() as i64);

        let results = query_builder.fetch_all(self.inner.get_pool()).await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        info!("query positions: total={}, returned={}", total_count, results.len());
        Ok((page_response, results))
    }

    async fn select_by_id(&self, _id: i64) -> Result<PositionInfo, Error> {
        Err(Error::msg(
            "s_positions uses composite primary key, use select_by_wallet_symbol_side instead",
        ))
    }

    async fn insert(&self, position: PositionInfo) -> Result<i64, Error> {
        // Use ON CONFLICT for idempotency (handles duplicate messages from EMQX)
        sqlx::query(
            "INSERT INTO s_positions (wallet_id, symbol, side, size, entry_price, realized_pnl, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
             ON CONFLICT (wallet_id, symbol, side) DO UPDATE SET size = EXCLUDED.size, entry_price = EXCLUDED.entry_price, realized_pnl = EXCLUDED.realized_pnl, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(position.wallet_id)
        .bind(&position.symbol)
        .bind(position.side.as_string())
        .bind(position.size)
        .bind(position.entry_price)
        .bind(position.realized_pnl)
        .execute(self.inner.get_pool())
        .await
        .context("Failed to insert position")?;
        Ok(position.wallet_id)
    }

    async fn update(&self, position: PositionInfo) -> Result<i64, Error> {
        sqlx::query(
            "UPDATE s_positions SET size = ?, entry_price = ?, realized_pnl = ?, updated_at = CURRENT_TIMESTAMP WHERE wallet_id = ? AND symbol = ? AND side = ?",
        )
        .bind(position.size)
        .bind(position.entry_price)
        .bind(position.realized_pnl)
        .bind(position.wallet_id)
        .bind(&position.symbol)
        .bind(position.side.as_string())
        .execute(self.inner.get_pool())
        .await
        .context("Failed to update position")?;
        Ok(position.wallet_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM s_positions")
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        // s_positions uses composite primary key, delete all records for this wallet_id
        let delete_result = sqlx::query("DELETE FROM s_positions WHERE wallet_id = ?")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
