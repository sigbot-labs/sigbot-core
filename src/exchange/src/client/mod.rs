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

pub mod cex;
pub mod dex;
pub mod exchange_factory;
pub mod predict;
pub mod stock;

use crate::client::exchange_factory::ISigbotExchangeClient;
use anyhow::Error;
use async_trait::async_trait;
use sigbot_core::cache::ICache;
use sigbot_types::modules::exchange::models::trade_market::{KlineModel, PriceModel};
use std::{collections::HashMap, sync::Arc};

#[async_trait]
pub trait ISigbotOrderBookExchangeClient: ISigbotExchangeClient {
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error>;
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineModel>, Error>;
}

// Wrapper to convert ICache<String> to ICache<Vec<KlineModel>>
struct KlineModelCacheWrapper {
    inner: Arc<dyn ICache<String>>,
}

#[async_trait::async_trait]
impl ICache<Vec<KlineModel>> for KlineModelCacheWrapper {
    async fn get(&self, key: String) -> Result<Option<Vec<KlineModel>>, anyhow::Error> {
        match self.inner.get(key).await? {
            Some(s) => Ok(serde_json::from_str(&s).ok()),
            None => Ok(None),
        }
    }

    async fn set(&self, key: String, value: Vec<KlineModel>, seconds: Option<i32>) -> Result<bool, anyhow::Error> {
        let s = serde_json::to_string(&value)?;
        self.inner.set(key, s, seconds).await
    }

    async fn set_nx(&self, key: String, value: Option<String>) -> Result<bool, anyhow::Error> {
        self.inner.set_nx(key, value).await
    }

    async fn keys(&self, pattern: String) -> Result<Vec<String>, anyhow::Error> {
        self.inner.keys(pattern).await
    }

    async fn hget(&self, key: String, field: Option<String>) -> Result<Option<String>, anyhow::Error> {
        self.inner.hget(key, field).await
    }

    async fn hget_all(&self, name: String) -> Result<Option<HashMap<String, String>>, anyhow::Error> {
        self.inner.hget_all(name).await
    }

    async fn hkeys(&self, key: String) -> Result<Vec<String>, anyhow::Error> {
        self.inner.hkeys(key).await
    }

    async fn hset(&self, key: String, field_values: Option<Vec<(String, String)>>) -> Result<bool, anyhow::Error> {
        self.inner.hset(key, field_values).await
    }

    async fn hset_nx(&self, key: String, field: String, value: String) -> Result<bool, anyhow::Error> {
        self.inner.hset_nx(key, field, value).await
    }

    async fn hdel(&self, key: String, field: String) -> Result<bool, anyhow::Error> {
        self.inner.hdel(key, field).await
    }

    async fn expire(&self, key: String, milliseconds: i64) -> Result<bool, anyhow::Error> {
        self.inner.expire(key, milliseconds).await
    }

    async fn get_bit(&self, key: String, offset: u64) -> Result<bool, anyhow::Error> {
        self.inner.get_bit(key, offset).await
    }

    async fn set_bit(&self, key: String, offset: u64, value: bool) -> Result<bool, anyhow::Error> {
        self.inner.set_bit(key, offset, value).await
    }

    async fn del(&self, key: String) -> Result<bool, anyhow::Error> {
        self.inner.del(key).await
    }
}
