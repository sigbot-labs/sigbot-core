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
use sigbot_types::modules::wallet::balance::BalanceInfo;
use sigbot_types::modules::wallet::balance::QueryBalanceRequest;
use sigbot_types::{PageRequest, PageResponse};

#[async_trait]
pub trait IBalanceInfoHandler: Send {
    async fn find(
        &self,
        param: QueryBalanceRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<BalanceInfo>), Error>;
}

pub struct BalanceInfoHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> BalanceInfoHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> IBalanceInfoHandler for BalanceInfoHandler<'a> {
    async fn find(
        &self,
        param: QueryBalanceRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<BalanceInfo>), Error> {
        let balance = BalanceInfo {
            wallet_id: param.wallet_id.unwrap_or(0),
            asset: param.asset.clone().unwrap_or_default(),
            available: 0.0,
            locked: 0.0,
            updated_at: None,
        };

        let repo = self.state.balance_repo.lock().await;
        repo.get(&self.state.config).select(balance, page).await
    }
}
