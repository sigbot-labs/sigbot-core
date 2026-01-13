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
use sigbot_types::modules::backtest::backtest_case::{
    BacktestCaseInfo, DeleteBacktestCaseRequest, QueryBacktestCaseRequest, SaveBacktestCaseRequest,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IBacktestCaseInfoHandler: Send + Sync {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<BacktestCaseInfo>>, Error>;

    async fn find(
        &self,
        param: QueryBacktestCaseRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<BacktestCaseInfo>), Error>;

    async fn save(&self, param: SaveBacktestCaseRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteBacktestCaseRequest) -> Result<u64, Error>;
}

pub struct BacktestCaseInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> BacktestCaseInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IBacktestCaseInfoHandler for BacktestCaseInfoHandler<'a> {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<BacktestCaseInfo>>, Error> {
        let param = BacktestCaseInfo {
            base: EntityBase::new_with_id(id),
            tenant_id: String::new(),
            provider: None,
        };

        let repo = self.state.backtest_case_repo.lock().await;
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

    #[audit_log("[BACKTEST_CASE][FIND] tenant_id: {param.tenant_id.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryBacktestCaseRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<BacktestCaseInfo>), Error> {
        let repo = self.state.backtest_case_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity(), page).await
    }

    #[audit_log("[BACKTEST_CASE][ADD] backtest_case_id: {param.backtest_case_id.unwrap_or_default()}")]
    async fn save(&self, param: SaveBacktestCaseRequest) -> Result<i64, Error> {
        let repo = self.state.backtest_case_repo.lock().await;
        if param.backtest_case_id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()).await
        } else {
            repo.get(&self.state.config).upsert(param.to_entity()).await
        }
    }

    #[audit_log("[BACKTEST_CASE][DELETE] backtest_case_id: {param.backtest_case_id}")]
    async fn delete(&self, param: DeleteBacktestCaseRequest) -> Result<u64, Error> {
        let repo = self.state.backtest_case_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.backtest_case_id).await
    }
}
