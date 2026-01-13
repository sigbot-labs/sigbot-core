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

use crate::config::config::PostgresAppDBProperties;
use crate::dynamic_postgres_upsert;
use crate::dynamic_postgres_query;
use crate::dynamic_postgres_update;
use crate::store::postgres::PostgresRepository;
use crate::store::IAsyncRepository;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::sys::tenant::TenantInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct TenantPostgresRepository {
    inner: PostgresRepository<TenantInfo>,
}

impl TenantPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(TenantPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl IAsyncRepository<TenantInfo> for TenantPostgresRepository {
    async fn select(&self, tenant: TenantInfo, page: PageRequest) -> Result<(PageResponse, Vec<TenantInfo>), Error> {
        let result = dynamic_postgres_query!(tenant, "sys_tenant", self.inner.get_pool(), "updated_at", page, TenantInfo)?;
        info!("query tenants: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<TenantInfo, Error> {
        let tenant = sqlx::query_as::<_, TenantInfo>("SELECT * FROM sys_tenant WHERE id = $1 and del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        info!("query tenant: {:?}", tenant);
        Ok(tenant)
    }

    async fn upsert(&self, mut tenant: TenantInfo) -> Result<i64, Error> {
        let upserted_id = dynamic_postgres_upsert!(tenant, "tenants", self.inner.get_pool())?;
        info!("Inserted tenant.id: {:?}", upserted_id);
        Ok(upserted_id)
    }

    async fn update(&self, mut tenant: TenantInfo) -> Result<i64, Error> {
        let updated_id = dynamic_postgres_update!(tenant, "sys_tenant", self.inner.get_pool())?;
        info!("Updated tenant.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM sys_tenant")
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM sys_tenant WHERE id = $1 and del_flag = 0")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
