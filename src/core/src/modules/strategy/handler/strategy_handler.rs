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
use sigbot_types::modules::strategy::strategy::{
    DeleteStrategyRequest, QueryStrategyRequest, SaveStrategyRequest, StrategyInfo,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IStrategyInfoHandler: Send + Sync {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<StrategyInfo>>, Error>;

    async fn find(
        &self,
        param: QueryStrategyRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<StrategyInfo>), Error>;

    async fn save(&self, param: SaveStrategyRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteStrategyRequest) -> Result<u64, Error>;
}

pub struct StrategyInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> StrategyInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IStrategyInfoHandler for StrategyInfoHandler<'a> {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<StrategyInfo>>, Error> {
        let param = StrategyInfo {
            base: EntityBase::new_with_id(id),
            name: None,
            provider: None,
            parameters: None,
            description: None,
        };

        let repo = self.state.strategy_repo.lock().await;
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .unwrap()
            .1;

        if res.len() > 0 {
            let strategy = Arc::new(res.get(0).unwrap().clone());
            return Ok(Some(strategy));
        } else {
            Ok(None)
        }
    }

    #[audit_log("[EXCHANGE][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryStrategyRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<StrategyInfo>), Error> {
        let repo = self.state.strategy_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity(), page).await
    }

    #[audit_log("[EXCHANGE][ADD] param: {param.name.clone()}")]
    async fn save(&self, param: SaveStrategyRequest) -> Result<i64, Error> {
        let repo = self.state.strategy_repo.lock().await;
        if param.id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()).await
        } else {
            repo.get(&self.state.config).upsert(param.to_entity()).await
        }
    }

    #[audit_log("[EXCHANGE][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteStrategyRequest) -> Result<u64, Error> {
        let repo = self.state.strategy_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }
}
