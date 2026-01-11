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
use crate::dynamic_sqlite_insert;
use crate::dynamic_sqlite_query;
use crate::dynamic_sqlite_update;
use crate::store::sqlite::SQLiteRepository;
use crate::store::{select_by_sql_sqlite, AsyncRepository};
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::debug;
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
impl AsyncRepository<WorkflowInfo> for WorkflowInfoSQLiteRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        let result = dynamic_sqlite_query!(
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
        let workflow = sqlx::query_as::<_, WorkflowInfo>("SELECT * FROM s_workflow WHERE id = ? AND del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        debug!("query workflow: {:?}", workflow);
        Ok(workflow)
    }

    async fn insert(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        // Convert flow_info to flow_json for database storage (database column is still named flow_json)
        if let Some(ref flow_info) = workflow.flow_info {
            // Serialize flow_info to JSON string for database storage
            let flow_json_str = serde_json::to_string(flow_info)?;
            let workflow_id = workflow.base.id;
            let insert_by = crate::util::auths::SecurityContext::get_instance()
                .get_current_uname_for_store()
                .await;
            workflow.base.pre_insert(insert_by).await;

            let query = "INSERT OR IGNORE INTO s_workflow (id, name, provider, status, flow_json, description, created_at, updated_at, created_by, updated_by, del_flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            sqlx::query(query)
                .bind(workflow_id)
                .bind(&workflow.name)
                .bind(workflow.provider.as_ref().map(|p| p.as_str()))
                .bind(workflow.status.as_ref().map(|s| s.as_str()))
                .bind(&flow_json_str)
                .bind(&workflow.description)
                .bind(&workflow.base.created_at)
                .bind(&workflow.base.updated_at)
                .bind(&workflow.base.created_by)
                .bind(&workflow.base.updated_by)
                .bind(workflow.base.del_flag)
                .execute(self.inner.get_pool())
                .await?;

            let inserted_id = if let Some(id) = workflow_id {
                id
            } else {
                sqlx::query_scalar::<_, i64>("SELECT last_insert_rowid()")
                    .fetch_one(self.inner.get_pool())
                    .await?
            };
            debug!("Inserted workflow.id: {:?}", inserted_id);
            Ok(inserted_id)
        } else {
            let inserted_id = dynamic_sqlite_insert!(workflow, "s_workflow", self.inner.get_pool())?;
            debug!("Inserted workflow.id: {:?}", inserted_id);
            Ok(inserted_id)
        }
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        // Convert flow_info to flow_json for database storage (database column is still named flow_json)
        if let Some(ref flow_info) = workflow.flow_info {
            let flow_json_value = serde_json::to_value(flow_info)?;
            let flow_json_str = serde_json::to_string(&flow_json_value)?;
            let workflow_id = workflow.base.id.unwrap_or(0);
            let update_by = crate::util::auths::SecurityContext::get_instance()
                .get_current_uname_for_store()
                .await;
            workflow.base.pre_update(update_by).await;

            let query = "UPDATE s_workflow SET name = ?, provider = ?, status = ?, flow_json = ?, description = ?, updated_at = ?, updated_by = ? WHERE id = ? AND del_flag = 0";
            sqlx::query(query)
                .bind(&workflow.name)
                .bind(workflow.provider.as_ref().map(|p| p.as_str()))
                .bind(workflow.status.as_ref().map(|s| s.as_str()))
                .bind(&flow_json_str)
                .bind(&workflow.description)
                .bind(&workflow.base.updated_at)
                .bind(&workflow.base.updated_by)
                .bind(workflow_id)
                .execute(self.inner.get_pool())
                .await?;

            debug!("Updated workflow.id: {:?}", workflow_id);
            Ok(workflow_id)
        } else {
            let updated_id = dynamic_sqlite_update!(workflow, "s_workflow", self.inner.get_pool())?;
            debug!("Updated workflow.id: {:?}", updated_id);
            Ok(updated_id)
        }
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_workflow SET del_flag = 1")
            .execute(self.inner.get_pool())
            .await?;

        debug!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_workflow SET del_flag = 1 WHERE id = ? AND del_flag = 0")
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
        let workflows = select_by_sql_sqlite(self.inner.get_pool(), sql_template, params).await?;
        debug!("query workflows by sql: {:?}", workflows);
        Ok(workflows)
    }
}
