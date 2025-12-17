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

use crate::config::config::PostgresAppDBProperties;
use crate::store::postgres::PostgresRepository;
use crate::store::AsyncRepository;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::wallet::ledger::LedgerInfo;
use sigbot_types::{PageRequest, PageResponse};
use sqlx::Row;

pub struct LedgerInfoPostgresRepository {
    inner: PostgresRepository<LedgerInfo>,
}

impl LedgerInfoPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(LedgerInfoPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<LedgerInfo> for LedgerInfoPostgresRepository {
    async fn select(&self, ledger: LedgerInfo, page: PageRequest) -> Result<(PageResponse, Vec<LedgerInfo>), Error> {
        let mut conditions = Vec::new();
        let mut param_index = 1;

        if ledger.wallet_id != 0 {
            conditions.push(format!("wallet_id = ${}", param_index));
            param_index += 1;
        }
        if ledger.order_id != 0 {
            conditions.push(format!("order_id = ${}", param_index));
            param_index += 1;
        }
        if !ledger.symbol.is_empty() {
            conditions.push(format!("symbol = ${}", param_index));
            param_index += 1;
        }

        let where_clause = if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        };

        // Count total records
        let count_query = format!("SELECT COUNT(1) FROM s_ledger WHERE {}", where_clause);
        let mut count_query_builder = sqlx::query(&count_query);
        if ledger.wallet_id != 0 {
            count_query_builder = count_query_builder.bind(ledger.wallet_id);
        }
        if ledger.order_id != 0 {
            count_query_builder = count_query_builder.bind(ledger.order_id);
        }
        if !ledger.symbol.is_empty() {
            count_query_builder = count_query_builder.bind(&ledger.symbol);
        }
        let total_count: i64 = count_query_builder.fetch_one(self.inner.get_pool()).await?.get(0);

        // Query data with pagination
        let query = format!(
            "SELECT wallet_id, order_id, trade_id, symbol, side, price, qty, fee, fee_asset, exchange_order_id, exchange_trade_id, ts FROM s_ledger WHERE {} ORDER BY ts DESC LIMIT ${} OFFSET ${}",
            where_clause, param_index, param_index + 1
        );
        let mut query_builder = sqlx::query_as::<_, LedgerInfo>(&query);
        if ledger.wallet_id != 0 {
            query_builder = query_builder.bind(ledger.wallet_id);
        }
        if ledger.order_id != 0 {
            query_builder = query_builder.bind(ledger.order_id);
        }
        if !ledger.symbol.is_empty() {
            query_builder = query_builder.bind(&ledger.symbol);
        }
        query_builder = query_builder.bind(page.get_limit() as i64);
        query_builder = query_builder.bind(page.get_offset() as i64);

        let results = query_builder.fetch_all(self.inner.get_pool()).await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        info!("query ledgers: total={}, returned={}", total_count, results.len());
        Ok((page_response, results))
    }

    async fn select_by_id(&self, _id: i64) -> Result<LedgerInfo, Error> {
        // s_ledger uses composite primary key, use select_by_composite_key instead
        Err(Error::msg(
            "s_ledger uses composite primary key, use select_by_composite_key instead",
        ))
    }

    async fn insert(&self, ledger: LedgerInfo) -> Result<i64, Error> {
        // Use ON CONFLICT DO NOTHING for idempotency (handles duplicate messages from EMQX)
        // s_ledger is immutable ledger (append-only), so we don't update on conflict
        sqlx::query(
            "INSERT INTO s_ledger (wallet_id, order_id, trade_id, symbol, side, price, qty, fee, fee_asset, exchange_order_id, exchange_trade_id, ts)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (wallet_id, order_id, trade_id) DO NOTHING",
        )
        .bind(ledger.wallet_id)
        .bind(ledger.order_id)
        .bind(ledger.trade_id)
        .bind(&ledger.symbol)
        .bind(ledger.side.as_string())
        .bind(ledger.price)
        .bind(ledger.qty)
        .bind(ledger.fee)
        .bind(ledger.fee_asset.as_ref())
        .bind(ledger.exchange_order_id.as_ref())
        .bind(ledger.exchange_trade_id.as_ref())
        .bind(ledger.ts)
        .execute(self.inner.get_pool())
        .await
        .context("Failed to insert ledger")?;

        Ok(ledger.trade_id)
    }

    async fn update(&self, _ledger: LedgerInfo) -> Result<i64, Error> {
        // s_ledger is immutable ledger, updates are not allowed
        Err(Error::msg("s_ledger is immutable, updates are not allowed"))
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        // s_ledger is immutable ledger, deletes are not allowed
        Err(Error::msg("s_ledger is immutable, deletes are not allowed"))
    }

    async fn delete_by_id(&self, _id: i64) -> Result<u64, Error> {
        // s_ledger is immutable ledger, deletes are not allowed
        Err(Error::msg("s_ledger is immutable, deletes are not allowed"))
    }
}
