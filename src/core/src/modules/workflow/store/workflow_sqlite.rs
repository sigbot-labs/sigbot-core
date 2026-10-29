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

use crate::config::config::SqliteAppDBProperties;
use crate::dynamic_sqlite_query;
use crate::store::sqlite::SQLiteRepository;
use crate::store::IAsyncRepository;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use sigbot_types::modules::workflow::workflow::WorkflowInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use sigbot_utils::types::GenericValue;
use std::collections::HashMap;

pub struct WorkflowInfoSQLiteRepository {
    inner: SQLiteRepository<WorkflowInfo>,
}

impl WorkflowInfoSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(WorkflowInfoSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl IAsyncRepository<WorkflowInfo> for WorkflowInfoSQLiteRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        anyhow::ensure!(workflow.tenant_id.is_some(), "explicit workflow tenant scope required");
        let result = dynamic_sqlite_query!(
            workflow,
            "s_workflow",
            self.inner.get_pool(),
            "updated_at",
            page,
            WorkflowInfo
        )?;

        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, _id: i64) -> Result<WorkflowInfo, Error> {
        Err(Error::msg("use select with explicit workflow tenant scope"))
    }

    async fn upsert(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        anyhow::ensure!(
            workflow.tenant_id.is_some()
                && workflow.space_id.is_some()
                && workflow
                    .env
                    .as_deref()
                    .is_some_and(|value| !value.is_empty() && value != "legacy"),
            "bound workflow tenant, space and environment required"
        );
        let supplied_id = workflow.base.id;
        let actor = workflow.base.created_by.clone();
        if supplied_id.is_none() {
            workflow.base.pre_insert(actor).await;
        }
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_string).transpose()?;
        let result = sqlx::query_scalar::<_, i64>(
            "INSERT INTO s_workflow (id, name, status, flow_json, properties, secrets, description, created_at, updated_at, created_by, updated_by, del_flag, tenant_id, space_id, env)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, status = excluded.status,
             flow_json = excluded.flow_json, properties = excluded.properties, secrets = excluded.secrets,
             description = excluded.description, updated_at = excluded.updated_at, updated_by = excluded.updated_by
             WHERE s_workflow.del_flag = 0 AND s_workflow.tenant_id = excluded.tenant_id AND s_workflow.space_id = excluded.space_id AND s_workflow.env = excluded.env RETURNING id")
            .bind(id).bind(&workflow.name).bind(workflow.status.as_ref().map(|value| value.as_str()))
            .bind(flow).bind(workflow.properties.as_ref().map(serde_json::to_string).transpose()?)
            .bind(workflow.secrets.as_ref().map(serde_json::to_string).transpose()?)
            .bind(&workflow.description).bind(workflow.base.created_at).bind(workflow.base.updated_at)
            .bind(&workflow.base.created_by).bind(&workflow.base.updated_by).bind(0_i16)
            .bind(workflow.tenant_id).bind(workflow.space_id).bind(&workflow.env)
            .fetch_one(self.inner.get_pool()).await?;
        Ok(result)
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.space_id.is_some() && workflow.env.is_some(),
            "explicit workflow scope required"
        );
        let deleted = workflow.base.del_flag.unwrap_or(0);
        let actor = workflow.base.updated_by.clone();
        workflow.base.pre_update(actor).await;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_string).transpose()?;
        let rows = sqlx::query(
            "UPDATE s_workflow SET name = ?, status = ?, flow_json = ?,
             properties = ?, secrets = ?, description = ?, updated_at = ?, updated_by = ?, del_flag = ?
             WHERE id = ? AND tenant_id = ? AND space_id = ? AND env = ? AND del_flag = 0 AND (status = ? OR ? NOT IN ('STOPPED','SUCCESS','FAILED'))",
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
        .bind(id)
        .bind(workflow.tenant_id)
        .bind(workflow.space_id)
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

    async fn delete_by_id(&self, _id: i64) -> Result<u64, Error> {
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
