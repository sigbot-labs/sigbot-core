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
use sigbot_types::sys::tenant::{DeleteTenantRequest, QueryTenantRequest, SaveTenantRequest, Tenant};
use sigbot_types::{PageRequest, PageResponse};

#[async_trait]
pub trait ITenantHandler: Send {
    async fn find(&self, param: QueryTenantRequest, page: PageRequest) -> Result<(PageResponse, Vec<Tenant>), Error>;

    async fn save(&self, param: SaveTenantRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteTenantRequest) -> Result<u64, Error>;
}

pub struct TenantHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> TenantHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> ITenantHandler for TenantHandler<'a> {
    #[audit_log("[TENANT][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(&self, param: QueryTenantRequest, page: PageRequest) -> Result<(PageResponse, Vec<Tenant>), Error> {
        let repo = self.state.tenant_repo.lock().await;
        repo.get(&self.state.config).select(param.to_tenant(), page).await
    }

    #[audit_log("[TENANT][ADD] name: {param.name.clone().unwrap_or_default()}")]
    async fn save(&self, param: SaveTenantRequest) -> Result<i64, Error> {
        let repo = self.state.tenant_repo.lock().await;
        if param.id.is_some() {
            repo.get(&self.state.config).update(param.to_tenant()).await
        } else {
            repo.get(&self.state.config).insert(param.to_tenant()).await
        }
    }

    #[audit_log("[TENANT][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteTenantRequest) -> Result<u64, Error> {
        let repo = self.state.tenant_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }
}
