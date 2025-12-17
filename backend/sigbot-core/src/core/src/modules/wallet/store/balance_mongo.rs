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
use sigbot_types::modules::wallet::balance::BalanceInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct BalanceInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<BalanceInfo>>,
    collection: Collection<BalanceInfo>,
}

impl BalanceInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("s_balances");
        Ok(BalanceInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl AsyncRepository<BalanceInfo> for BalanceInfoMongoRepository {
    async fn select(&self, balance: BalanceInfo, page: PageRequest) -> Result<(PageResponse, Vec<BalanceInfo>), Error> {
        let mut filter = doc! {};

        if balance.wallet_id != 0 {
            filter.insert("wallet_id", balance.wallet_id);
        }
        if !balance.asset.is_empty() {
            filter.insert("asset", &balance.asset);
        }

        // Count total records
        let total_count = self.collection.count_documents(filter.clone()).await? as i64;

        // Query data with pagination
        let cursor = self
            .collection
            .find(filter)
            .skip(page.get_offset() as u64)
            .limit(page.get_limit() as i64)
            .sort(doc! { "updated_at": -1 })
            .await?;

        let results: Vec<BalanceInfo> = cursor.try_collect().await?;
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
        // Use upsert for idempotency (handles duplicate messages from EMQX)
        let filter = doc! { "wallet_id": balance.wallet_id, "asset": &balance.asset };
        let update = doc! {
            "$set": {
                "available": balance.available,
                "locked": balance.locked,
                "updated_at": mongodb::bson::DateTime::now()
            }
        };
        self.collection.update_one(filter, update).await?;
        Ok(balance.wallet_id)
    }

    async fn update(&self, balance: BalanceInfo) -> Result<i64, Error> {
        let filter = doc! { "wallet_id": balance.wallet_id, "asset": &balance.asset };
        let update = doc! {
            "$set": {
                "available": balance.available,
                "locked": balance.locked,
                "updated_at": mongodb::bson::DateTime::now()
            }
        };
        self.collection.update_one(filter, update).await?;
        Ok(balance.wallet_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let result = self.collection.delete_many(doc! {}).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.deleted_count)
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        // s_balances uses composite primary key, delete all records for this wallet_id
        let result = self.collection.delete_many(doc! { "wallet_id": id }).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.deleted_count)
    }
}
