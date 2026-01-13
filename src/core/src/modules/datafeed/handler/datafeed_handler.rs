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
use sigbot_types::modules::datafeed::datafeed::{
    DatafeedInfo, DeleteDatafeedRequest, QueryDatafeedRequest, SaveDatafeedRequest,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IDatafeedInfoHandler: Send + Sync {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<DatafeedInfo>>, Error>;

    async fn find(
        &self,
        param: QueryDatafeedRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<DatafeedInfo>), Error>;

    async fn save(&self, param: SaveDatafeedRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteDatafeedRequest) -> Result<u64, Error>;
}

pub struct DatafeedInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> DatafeedInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IDatafeedInfoHandler for DatafeedInfoHandler<'a> {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<DatafeedInfo>>, Error> {
        let param = DatafeedInfo {
            base: EntityBase::new_with_id(id),
            name: None,
            provider: None,
            properties: None,
            secrets: None,
            description: None,
        };

        let repo = self.state.datafeed_repo.lock().await;
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .unwrap()
            .1;

        if res.len() > 0 {
            let datafeed = Arc::new(res.get(0).unwrap().clone());
            return Ok(Some(datafeed));
        } else {
            Ok(None)
        }
    }

    #[audit_log("[DATAFEED][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryDatafeedRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<DatafeedInfo>), Error> {
        let repo = self.state.datafeed_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity()?, page).await
    }

    #[audit_log("[DATAFEED][ADD] param: {param.name.clone()}")]
    async fn save(&self, param: SaveDatafeedRequest) -> Result<i64, Error> {
        let repo = self.state.datafeed_repo.lock().await;
        if param.id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()?).await
        } else {
            repo.get(&self.state.config).upsert(param.to_entity()?).await
        }
    }

    #[audit_log("[DATAFEED][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteDatafeedRequest) -> Result<u64, Error> {
        let repo = self.state.datafeed_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }
}
