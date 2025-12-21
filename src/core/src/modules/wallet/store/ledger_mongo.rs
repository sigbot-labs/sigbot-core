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

use crate::config::config::MongoAppDBProperties;
use crate::store::mongo::MongoRepository;
use crate::store::AsyncRepository;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::modules::wallet::ledger::LedgerInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct LedgerInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<LedgerInfo>>,
    collection: Collection<LedgerInfo>,
}

impl LedgerInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("s_ledger");
        Ok(LedgerInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl AsyncRepository<LedgerInfo> for LedgerInfoMongoRepository {
    async fn select(&self, ledger: LedgerInfo, page: PageRequest) -> Result<(PageResponse, Vec<LedgerInfo>), Error> {
        let mut filter = doc! {};

        if ledger.wallet_id != 0 {
            filter.insert("wallet_id", ledger.wallet_id);
        }
        if ledger.order_id != 0 {
            filter.insert("order_id", ledger.order_id);
        }
        if !ledger.symbol.is_empty() {
            filter.insert("symbol", &ledger.symbol);
        }

        // Count total records
        let total_count = self.collection.count_documents(filter.clone()).await? as i64;

        // Query data with pagination
        let cursor = self
            .collection
            .find(filter)
            .skip(page.get_offset() as u64)
            .limit(page.get_limit() as i64)
            .sort(doc! { "ts": -1 })
            .await?;

        let results: Vec<LedgerInfo> = cursor.try_collect().await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        info!("query ledgers: total={}, returned={}", total_count, results.len());
        Ok((page_response, results))
    }

    async fn select_by_id(&self, _id: i64) -> Result<LedgerInfo, Error> {
        Err(Error::msg(
            "s_ledger uses composite primary key, use select_by_composite_key instead",
        ))
    }

    async fn insert(&self, ledger: LedgerInfo) -> Result<i64, Error> {
        // Use upsert with DO NOTHING semantics for idempotency (handles duplicate messages from EMQX)
        // s_ledger is immutable ledger (append-only), so we check existence first
        let filter = doc! {
            "wallet_id": &ledger.wallet_id,
            "order_id": &ledger.order_id,
            "trade_id": &ledger.trade_id
        };
        let existing = self.collection.find_one(filter.clone()).await?;
        if existing.is_none() {
            self.collection.insert_one(&ledger).await?;
        }
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
