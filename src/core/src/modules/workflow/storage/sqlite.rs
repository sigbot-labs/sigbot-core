// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

use crate::config::config::SqliteStorageProperties;
use crate::dynamic_sqlite_query;
use crate::storage::sqlite::SQLiteRepository;
use crate::storage::IGenericRepository;
use anyhow::Error;
use anyhow::Ok;
use async_trait::async_trait;
use sigbot_types::workflow::workflow::WorkflowInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use sigbot_utils::types::GenericValue;
use std::collections::HashMap;
use uuid::Uuid;

pub struct WorkflowSQLiteRepository {
    inner: SQLiteRepository<WorkflowInfo>,
}

impl WorkflowSQLiteRepository {
    pub async fn new(config: &SqliteStorageProperties) -> Result<Self, Error> {
        Ok(WorkflowSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl IGenericRepository<WorkflowInfo> for WorkflowSQLiteRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        anyhow::ensure!(workflow.tenant_id.is_some(), "explicit workflow tenant scope required");
        let result = dynamic_sqlite_query!(
            workflow,
            "workflow_info",
            self.inner.get_pool(),
            "updated_at",
            page,
            WorkflowInfo
        )?;

        Ok((result.0, result.1))
    }

    async fn select_by(&self, _id: Uuid) -> Result<WorkflowInfo, Error> {
        Err(Error::msg("use select with explicit workflow tenant scope"))
    }

    async fn upsert(&self, mut workflow: WorkflowInfo) -> Result<Uuid, Error> {
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.env.as_deref().is_some_and(|value| !value.is_empty()),
            "bound workflow tenant and environment required"
        );
        let supplied_id = workflow.base.id;
        let actor = workflow.base.created_by.clone();
        if supplied_id.is_none() {
            workflow.base.pre_insert(actor).await;
        }
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_string).transpose()?;
        let result = sqlx::query_scalar::<_, String>(
            "INSERT INTO workflow_info (id, name, status, dag_config, properties, secrets, description, created_at, updated_at, created_by, updated_by, del_flag, tenant_id, env)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, status = excluded.status,
             dag_config = excluded.dag_config, properties = excluded.properties, secrets = excluded.secrets,
             description = excluded.description, updated_at = excluded.updated_at, updated_by = excluded.updated_by
             WHERE workflow_info.del_flag = 0 AND workflow_info.tenant_id = excluded.tenant_id AND workflow_info.env = excluded.env RETURNING id")
            .bind(id.to_string()).bind(&workflow.name).bind(workflow.status.as_ref().map(|value| value.as_str()))
            .bind(flow).bind(workflow.properties.as_ref().map(serde_json::to_string).transpose()?)
            .bind(workflow.secrets.as_ref().map(serde_json::to_string).transpose()?)
            .bind(&workflow.description).bind(workflow.base.created_at).bind(workflow.base.updated_at)
            .bind(&workflow.base.created_by).bind(&workflow.base.updated_by).bind(0_i16)
            .bind(workflow.tenant_id.map(|value| value.to_string())).bind(&workflow.env)
            .fetch_one(self.inner.get_pool()).await?;
        Ok(Uuid::parse_str(&result)?)
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<Uuid, Error> {
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.env.is_some(),
            "explicit workflow scope required"
        );
        let deleted = workflow.base.del_flag.unwrap_or(0);
        let actor = workflow.base.updated_by.clone();
        workflow.base.pre_update(actor).await;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_string).transpose()?;
        let rows = sqlx::query(
            "UPDATE workflow_info SET name = ?, status = ?, dag_config = ?,
             properties = ?, secrets = ?, description = ?, updated_at = ?, updated_by = ?, del_flag = ?
             WHERE id = ? AND tenant_id = ? AND env = ? AND del_flag = 0 AND (status = ? OR ? NOT IN ('STOPPED','SUCCESS','FAILED'))",
        )
        .bind(&workflow.name)
        .bind(workflow.status.as_ref().map(|value| value.as_str()))
        .bind(flow)
        .bind(workflow.properties.as_ref().map(serde_json::to_string).transpose()?)
        .bind(workflow.secrets.as_ref().map(serde_json::to_string).transpose()?)
        .bind(&workflow.description)
        .bind(workflow.base.updated_at)
        .bind(&workflow.base.updated_by)
        .bind(i16::from(deleted))
        .bind(id.to_string())
        .bind(workflow.tenant_id.map(|value| value.to_string()))
        .bind(&workflow.env)
        .bind(workflow.status.as_ref().map(|status|status.as_str()))
        .bind(workflow.status.as_ref().map(|status|status.as_str()))
        .execute(self.inner.get_pool())
        .await?
        .rows_affected();
        anyhow::ensure!(rows == 1, "workflow missing or deleted");
        Ok(id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        Err(Error::msg("unscoped workflow deletion is disabled"))
    }

    async fn delete_by_id(&self, _id: Uuid) -> Result<u64, Error> {
        Err(Error::msg("use a scoped workflow update to mark deletion"))
    }

    async fn select_by_sql(
        &self,
        _sql_template: &str,
        _params: &HashMap<String, GenericValue>,
    ) -> Result<Vec<WorkflowInfo>, Error> {
        Err(Error::msg("unscoped workflow SQL is disabled"))
    }
}
