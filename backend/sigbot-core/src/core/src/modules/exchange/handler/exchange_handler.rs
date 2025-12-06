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

use crate::context::state::SigbotState;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_audit_log::audit_log;
use sigbot_types::modules::exchange::exchange::{
    DeleteExchangeRequest, ExchangeInfo, QueryExchangeRequest, SaveExchangeRequest,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IExchangeInfoHandler: Send {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<ExchangeInfo>>, Error>;

    async fn find(
        &self,
        param: QueryExchangeRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<ExchangeInfo>), Error>;

    async fn save(&self, param: SaveExchangeRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteExchangeRequest) -> Result<u64, Error>;
}

pub struct ExchangeInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> ExchangeInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IExchangeInfoHandler for ExchangeInfoHandler<'a> {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<ExchangeInfo>>, Error> {
        let param = ExchangeInfo {
            base: EntityBase::new_with_id(id),
            name: None,
            provider: None,
            configuration: None,
            secrets: None,
            description: None,
        };

        let repo = self.state.exchange_repo.lock().await;
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .unwrap()
            .1;

        if res.len() > 0 {
            let exchange = Arc::new(res.get(0).unwrap().clone());
            return Ok(Some(exchange));
        } else {
            Ok(None)
        }
    }

    #[audit_log("[EXCHANGE][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryExchangeRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<ExchangeInfo>), Error> {
        let repo = self.state.exchange_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity(), page).await
    }

    #[audit_log("[EXCHANGE][ADD] param: {param.name.clone()}")]
    async fn save(&self, param: SaveExchangeRequest) -> Result<i64, Error> {
        let repo = self.state.exchange_repo.lock().await;
        if param.id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()).await
        } else {
            repo.get(&self.state.config).insert(param.to_entity()).await
        }
    }

    #[audit_log("[EXCHANGE][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteExchangeRequest) -> Result<u64, Error> {
        let repo = self.state.exchange_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }
}
