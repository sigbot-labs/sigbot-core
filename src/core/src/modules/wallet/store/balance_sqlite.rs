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
use sigbot_types::modules::wallet::balance::BalanceInfo;
use sigbot_types::{PageRequest, PageResponse};
use sqlx::Row;

pub struct BalanceInfoSQLiteRepository {
    inner: SQLiteRepository<BalanceInfo>,
}

impl BalanceInfoSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(BalanceInfoSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<BalanceInfo> for BalanceInfoSQLiteRepository {
    async fn select(&self, balance: BalanceInfo, page: PageRequest) -> Result<(PageResponse, Vec<BalanceInfo>), Error> {
        let mut conditions = Vec::new();
        let mut params: Vec<String> = Vec::new();

        if balance.wallet_id != 0 {
            conditions.push("wallet_id = ?".to_string());
            params.push(balance.wallet_id.to_string());
        }
        if !balance.asset.is_empty() {
            conditions.push("asset = ?".to_string());
            params.push(balance.asset.clone());
        }

        let where_clause = if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        };

        // Count total records
        let count_query = format!("SELECT COUNT(1) FROM s_balances WHERE {}", where_clause);
        let mut count_query_builder = sqlx::query(&count_query);
        for param in &params {
            count_query_builder = count_query_builder.bind(param);
        }
        let total_count: i64 = count_query_builder.fetch_one(self.inner.get_pool()).await?.get(0);

        // Query data with pagination
        let query = format!(
            "SELECT wallet_id, asset, available, locked, updated_at FROM s_balances WHERE {} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            where_clause
        );
        let mut query_builder = sqlx::query_as::<_, BalanceInfo>(&query);
        for param in &params {
            query_builder = query_builder.bind(param);
        }
        query_builder = query_builder.bind(page.get_limit() as i64);
        query_builder = query_builder.bind(page.get_offset() as i64);

        let results = query_builder.fetch_all(self.inner.get_pool()).await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        info!("query balances: total={}, returned={}", total_count, results.len());
        Ok((page_response, results))
    }

    async fn select_by_id(&self, _id: i64) -> Result<BalanceInfo, Error> {
        Err(Error::msg(
            "s_balances uses composite primary key, use select_by_wallet_and_asset instead",
        ))
    }

    async fn insert(&self, balance: BalanceInfo) -> Result<i64, Error> {
        // Use ON CONFLICT for idempotency (handles duplicate messages from EMQX)
        sqlx::query(
            "INSERT INTO s_balances (wallet_id, asset, available, locked, updated_at)
             VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
             ON CONFLICT (wallet_id, asset) DO UPDATE SET available = EXCLUDED.available, locked = EXCLUDED.locked, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(balance.wallet_id)
        .bind(&balance.asset)
        .bind(balance.available)
        .bind(balance.locked)
        .execute(self.inner.get_pool())
        .await
        .context("Failed to insert balance")?;
        Ok(balance.wallet_id)
    }

    async fn update(&self, balance: BalanceInfo) -> Result<i64, Error> {
        sqlx::query(
            "UPDATE s_balances SET available = ?, locked = ?, updated_at = CURRENT_TIMESTAMP WHERE wallet_id = ? AND asset = ?",
        )
        .bind(balance.available)
        .bind(balance.locked)
        .bind(balance.wallet_id)
        .bind(&balance.asset)
        .execute(self.inner.get_pool())
        .await
        .context("Failed to update balance")?;
        Ok(balance.wallet_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM s_balances")
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        // s_balances uses composite primary key, delete all records for this wallet_id
        let delete_result = sqlx::query("DELETE FROM s_balances WHERE wallet_id = ?")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
