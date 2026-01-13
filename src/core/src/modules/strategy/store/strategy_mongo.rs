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
use crate::{dynamic_mongo_insert, dynamic_mongo_query, dynamic_mongo_update};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::modules::strategy::strategy::StrategyInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct StrategyInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<StrategyInfo>>,
    collection: Collection<StrategyInfo>,
}

impl StrategyInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("t_strategy");
        Ok(StrategyInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl IAsyncRepository<StrategyInfo> for StrategyInfoMongoRepository {
    async fn select(
        &self,
        strategy: StrategyInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<StrategyInfo>), Error> {
        //let result = &self.inner.select(strategy, page).await;
        match dynamic_mongo_query!(strategy, self.collection, "updated_at", page, StrategyInfo) {
            Ok(result) => {
                info!("query strategys: {:?}", result);
                Ok((result.0, result.1))
            }
            Err(error) => Err(error),
        }
    }

    async fn select_by_id(&self, id: i64) -> Result<StrategyInfo, Error> {
        let filter = doc! { "id": id };
        let strategy = self
            .collection
            .find_one(filter)
            .await?
            .ok_or_else(|| Error::msg("StrategyInfo not found"))?;
        Ok(strategy)
    }

    async fn upsert(&self, mut strategy: StrategyInfo) -> Result<i64, Error> {
        dynamic_mongo_insert!(strategy, self.collection)
    }

    async fn update(&self, mut strategy: StrategyInfo) -> Result<i64, Error> {
        dynamic_mongo_update!(strategy, self.collection)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let result = self.collection.delete_many(doc! {}).await?;
        Ok(result.deleted_count)
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let filter = doc! { "id": id };
        let result = self.collection.delete_one(filter).await?;
        Ok(result.deleted_count)
    }
}
