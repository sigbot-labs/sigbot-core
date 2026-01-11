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
use crate::dynamic_postgres_query;
use crate::store::postgres::PostgresRepository;
use crate::store::{select_by_sql_postgres, AsyncRepository};
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::debug;
use sigbot_types::modules::workflow::workflow::WorkflowInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use sigbot_utils::types::GenericValue;
use std::collections::HashMap;

pub struct WorkflowInfoPostgresRepository {
    inner: PostgresRepository<WorkflowInfo>,
}

impl WorkflowInfoPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(WorkflowInfoPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<WorkflowInfo> for WorkflowInfoPostgresRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        let result = dynamic_postgres_query!(
            workflow,
            "s_workflow",
            self.inner.get_pool(),
            "updated_at",
            page,
            WorkflowInfo
        )?;
        debug!("query workflows: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<WorkflowInfo, Error> {
        let workflow = sqlx::query_as::<_, WorkflowInfo>("SELECT * FROM s_workflow WHERE id = $1 AND del_flag = FALSE")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        debug!("query workflow: {:?}", workflow);
        Ok(workflow)
    }

    async fn insert(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        // Serialize flow_info to JSON string for database storage
        let flow_json_str = workflow
            .flow_info
            .as_ref()
            .map(|fi| serde_json::to_string(fi))
            .transpose()?;

        let insert_by = crate::util::auths::SecurityContext::get_instance()
            .get_current_uname_for_store()
            .await;
        workflow.base.pre_insert(insert_by).await;

        let inserted_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO s_workflow (name, status, flow_json, description, created_at, updated_at, created_by, updated_by, del_flag) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
             ON CONFLICT (id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP 
             RETURNING id"
        )
        .bind(&workflow.name)
        .bind(workflow.status.as_ref().map(|s| s.as_str()))
        .bind(flow_json_str.as_deref())
        .bind(&workflow.description)
        .bind(&workflow.base.created_at)
        .bind(&workflow.base.updated_at)
        .bind(&workflow.base.created_by)
        .bind(&workflow.base.updated_by)
        .bind(workflow.base.del_flag)
        .fetch_one(self.inner.get_pool())
        .await?;

        debug!("Inserted workflow.id: {:?}", inserted_id);
        Ok(inserted_id)
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        // Serialize flow_info to JSON string for database storage
        let flow_json_str = workflow
            .flow_info
            .as_ref()
            .map(|fi| serde_json::to_string(fi))
            .transpose()?;

        let update_by = crate::util::auths::SecurityContext::get_instance()
            .get_current_uname_for_store()
            .await;
        workflow.base.pre_update(update_by).await;

        let workflow_id = workflow.base.id.unwrap_or(0);

        sqlx::query(
            "UPDATE s_workflow 
             SET name = $1, status = $2, flow_json = $3, description = $4, updated_at = $5, updated_by = $6 
             WHERE id = $7 AND del_flag = FALSE",
        )
        .bind(&workflow.name)
        .bind(workflow.status.as_ref().map(|s| s.as_str()))
        .bind(flow_json_str.as_deref())
        .bind(&workflow.description)
        .bind(&workflow.base.updated_at)
        .bind(&workflow.base.updated_by)
        .bind(workflow_id)
        .execute(self.inner.get_pool())
        .await?;

        debug!("Updated workflow.id: {:?}", workflow_id);
        Ok(workflow_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_workflow SET del_flag = TRUE")
            .execute(self.inner.get_pool())
            .await?;

        debug!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_workflow SET del_flag = TRUE WHERE id = $1 AND del_flag = FALSE")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        debug!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn select_by_sql(
        &self,
        sql_template: &str,
        params: &HashMap<String, GenericValue>,
    ) -> Result<Vec<WorkflowInfo>, Error> {
        let workflows = select_by_sql_postgres(self.inner.get_pool(), sql_template, params).await?;
        debug!("query workflows by sql: {:?}", workflows);
        Ok(workflows)
    }
}
