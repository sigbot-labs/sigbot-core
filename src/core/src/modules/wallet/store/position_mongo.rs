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
use crate::store::IAsyncRepository;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::modules::wallet::position::PositionInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct PositionInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<PositionInfo>>,
    collection: Collection<PositionInfo>,
}

impl PositionInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("s_positions");
        Ok(PositionInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl IAsyncRepository<PositionInfo> for PositionInfoMongoRepository {
    async fn select(
        &self,
        position: PositionInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<PositionInfo>), Error> {
        let mut filter = doc! {};

        if position.wallet_id != 0 {
            filter.insert("wallet_id", position.wallet_id);
        }
        if !position.symbol.is_empty() {
            filter.insert("symbol", &position.symbol);
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

        let results: Vec<PositionInfo> = cursor.try_collect().await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        info!("query positions: total={}, returned={}", total_count, results.len());
        Ok((page_response, results))
    }

    async fn select_by_id(&self, _id: i64) -> Result<PositionInfo, Error> {
        Err(Error::msg(
            "s_positions uses composite primary key, use select_by_wallet_symbol_side instead",
        ))
    }

    async fn upsert(&self, position: PositionInfo) -> Result<i64, Error> {
        // Use upsert for idempotency (handles duplicate messages from EMQX)
        let filter = doc! {
            "wallet_id": position.wallet_id,
            "symbol": &position.symbol,
            "side": position.side.as_string()
        };
        let update = doc! {
            "$set": {
                "size": position.size,
                "entry_price": position.entry_price,
                "realized_pnl": position.realized_pnl,
                "updated_at": mongodb::bson::DateTime::now()
            }
        };
        self.collection.update_one(filter, update).await?;
        Ok(position.wallet_id)
    }

    async fn update(&self, position: PositionInfo) -> Result<i64, Error> {
        let filter = doc! {
            "wallet_id": position.wallet_id,
            "symbol": &position.symbol,
            "side": position.side.as_string()
        };
        let update = doc! {
            "$set": {
                "size": position.size,
                "entry_price": position.entry_price,
                "realized_pnl": position.realized_pnl,
                "updated_at": mongodb::bson::DateTime::now()
            }
        };
        self.collection.update_one(filter, update).await?;
        Ok(position.wallet_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let result = self.collection.delete_many(doc! {}).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.deleted_count)
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        // s_positions uses composite primary key, delete all records for this wallet_id
        let result = self.collection.delete_many(doc! { "wallet_id": id }).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.deleted_count)
    }
}
