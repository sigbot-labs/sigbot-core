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
use anyhow::Error;
use async_trait::async_trait;
use common_audit_log::audit_log;
use sigbot_types::modules::wallet::wallet::{
    DeleteWalletRequest, QueryWalletRequest, SaveWalletRequest, WalletInfo, WalletMode,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IWalletInfoHandler: Send {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WalletInfo>>, Error>;

    async fn find(
        &self,
        param: QueryWalletRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WalletInfo>), Error>;

    async fn save(&self, param: SaveWalletRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteWalletRequest) -> Result<u64, Error>;
}

pub struct WalletInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> WalletInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IWalletInfoHandler for WalletInfoHandler<'a> {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WalletInfo>>, Error> {
        let param = WalletInfo {
            base: EntityBase::new_with_id(id),
            tenant_id: String::new(),
            exchange: String::new(),
            mode: WalletMode::LIVE,
            account_type: None,
        };

        let repo = self.state.wallet_repo.lock().await;
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .unwrap()
            .1;

        if res.len() > 0 {
            let wallet = Arc::new(res.get(0).unwrap().clone());
            return Ok(Some(wallet));
        } else {
            Ok(None)
        }
    }

    #[audit_log("[WALLET][FIND] tenant_id: {param.tenant_id.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryWalletRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WalletInfo>), Error> {
        let repo = self.state.wallet_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity(), page).await
    }

    #[audit_log("[WALLET][ADD] tenant_id: {param.tenant_id}")]
    async fn save(&self, param: SaveWalletRequest) -> Result<i64, Error> {
        let repo = self.state.wallet_repo.lock().await;
        if param.wallet_id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()).await
        } else {
            repo.get(&self.state.config).insert(param.to_entity()).await
        }
    }

    #[audit_log("[WALLET][DELETE] wallet_id: {param.wallet_id}")]
    async fn delete(&self, param: DeleteWalletRequest) -> Result<u64, Error> {
        let repo = self.state.wallet_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.wallet_id).await
    }
}
