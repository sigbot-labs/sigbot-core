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
use crate::{dynamic_mongo_query, dynamic_mongo_update, dynamic_mongo_insert};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::modules::wallet::wallet::WalletInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct WalletInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<WalletInfo>>,
    collection: Collection<WalletInfo>,
}

impl WalletInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("s_wallets");
        Ok(WalletInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl IAsyncRepository<WalletInfo> for WalletInfoMongoRepository {
    async fn select(&self, wallet: WalletInfo, page: PageRequest) -> Result<(PageResponse, Vec<WalletInfo>), Error> {
        let result = dynamic_mongo_query!(wallet, self.collection.clone(), "updated_at", page, WalletInfo)
            .map_err(|e: mongodb::error::Error| Error::from(e))?;
        info!("query wallets: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<WalletInfo, Error> {
        let filter = doc! { "id": id, "del_flag": false };
        let wallet = self
            .collection
            .find_one(filter)
            .await?
            .ok_or_else(|| Error::msg("Wallet not found"))?;
        info!("query wallet: {:?}", wallet);
        Ok(wallet)
    }

    async fn upsert(&self, mut wallet: WalletInfo) -> Result<i64, Error> {
        let upserted_id = dynamic_mongo_insert!(wallet, self.collection.clone())
            .map_err(|e: mongodb::error::Error| Error::from(e))?;
        info!("Inserted wallet.id: {:?}", upserted_id);
        Ok(upserted_id)
    }

    async fn update(&self, mut wallet: WalletInfo) -> Result<i64, Error> {
        let updated_id = dynamic_mongo_update!(wallet, self.collection.clone())
            .map_err(|e: mongodb::error::Error| Error::from(e))?;
        info!("Updated wallet.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let filter = doc! {};
        let update = doc! { "$set": { "del_flag": true } };
        let result = self.collection.update_many(filter, update).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.modified_count)
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let filter = doc! { "id": id, "del_flag": false };
        let update = doc! { "$set": { "del_flag": true } };
        let result = self.collection.update_one(filter, update).await?;
        info!("Deleted result: {:?}", result);
        Ok(result.modified_count)
    }
}
