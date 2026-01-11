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
use sigbot_types::modules::workflow::workflow::{
    DeleteWorkflowRequest, JobStatus, QueryWorkflowRequest, SaveWorkflowRequest, WorkflowInfo,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use sigbot_utils::types::GenericValue;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait IWorkflowInfoHandler: Send + Sync {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WorkflowInfo>>, Error>;

    async fn find(
        &self,
        param: QueryWorkflowRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error>;

    async fn save(&self, param: SaveWorkflowRequest) -> Result<i64, Error>;

    async fn delete(&self, param: DeleteWorkflowRequest) -> Result<u64, Error>;

    async fn find_start_jobs(&self) -> Result<Vec<WorkflowInfo>, Error>;

    async fn find_stop_jobs(&self) -> Result<Vec<WorkflowInfo>, Error>;

    async fn update_status(&self, workflow_id: i64, status: JobStatus) -> Result<u64, Error>;

    async fn update_node_status(&self, workflow_id: i64, node_id: &str, status: &str) -> Result<u64, Error>;
}

pub struct WorkflowInfoHandler {
    state: Arc<SigbotState>,
}

impl WorkflowInfoHandler {
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl IWorkflowInfoHandler for WorkflowInfoHandler {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WorkflowInfo>>, Error> {
        let param = WorkflowInfo {
            base: EntityBase::new_with_id(id),
            name: None,
            provider: None,
            status: None,
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        };

        let repo = self.state.workflow_repo.lock().await;
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .expect("Failed to get workflow")
            .1;

        if res.len() > 0 {
            let workflow = Arc::new(res.get(0).unwrap().clone());
            return Ok(Some(workflow));
        } else {
            Ok(None)
        }
    }

    #[audit_log("[WORKFLOW][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(
        &self,
        param: QueryWorkflowRequest,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        let repo = self.state.workflow_repo.lock().await;
        repo.get(&self.state.config).select(param.to_entity(), page).await
    }

    #[audit_log("[WORKFLOW][ADD] param: {param.name.clone()}")]
    async fn save(&self, param: SaveWorkflowRequest) -> Result<i64, Error> {
        let repo = self.state.workflow_repo.lock().await;
        if param.id.is_some() {
            repo.get(&self.state.config).update(param.to_entity()).await
        } else {
            repo.get(&self.state.config).insert(param.to_entity()).await
        }
    }

    #[audit_log("[WORKFLOW][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteWorkflowRequest) -> Result<u64, Error> {
        let repo = self.state.workflow_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }

    #[audit_log("[WORKFLOW][FIND_START_JOBS]")]
    async fn find_start_jobs(&self) -> Result<Vec<WorkflowInfo>, Error> {
        let repo = self.state.workflow_repo.lock().await;
        let sql = "SELECT * FROM s_workflow WHERE del_flag = FALSE AND (status = :PENDING OR (status = :RUNNING AND updated_at < NOW() - INTERVAL :TIMEOUT seconds))";
        let params = {
            let mut p = HashMap::new();
            p.insert(
                "PENDING".to_string(),
                GenericValue::String(JobStatus::PENDING.as_str().to_string()),
            );
            p.insert(
                "RUNNING".to_string(),
                GenericValue::String(JobStatus::RUNNING.as_str().to_string()),
            );
            p.insert("TIMEOUT".to_string(), GenericValue::String("180".to_string()));
            p
        };
        repo.get(&self.state.config).select_by_sql(sql, &params).await
    }

    #[audit_log("[WORKFLOW][FIND_STOP_JOBS]")]
    async fn find_stop_jobs(&self) -> Result<Vec<WorkflowInfo>, Error> {
        let repo = self.state.workflow_repo.lock().await;
        let sql = "SELECT * FROM s_workflow WHERE del_flag = 0 AND status = :STOPPING";
        let params = {
            let mut p = HashMap::new();
            p.insert(
                "STOPPING".to_string(),
                GenericValue::String(JobStatus::STOPPING.as_str().to_string()),
            );
            p
        };
        repo.get(&self.state.config).select_by_sql(sql, &params).await
    }

    // TODO: using dynamic_postgres_query! macro to update status with transaction.
    #[audit_log("[WORKFLOW][UPDATE_STATUS] id: {workflow_id}, status: {status.as_str()}")]
    async fn update_status(&self, workflow_id: i64, status: JobStatus) -> Result<u64, Error> {
        let repo = self.state.workflow_repo.lock().await;
        let param = WorkflowInfo {
            base: EntityBase::new_with_id(Some(workflow_id)),
            name: None,
            provider: None,
            status: None,
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        };
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .expect("Failed to get workflow")
            .1;

        if res.is_empty() {
            return Err(Error::msg(format!("Workflow with id {} not found", workflow_id)));
        }

        let mut updated = res.get(0).unwrap().clone();
        updated.status = Some(status);

        repo.get(&self.state.config).update(updated).await?;
        Ok(1)
    }

    // TODO: using dynamic_postgres_query! macro to update jsonb node status with transaction.
    #[audit_log("[WORKFLOW][UPDATE_NODE_STATUS] id: {workflow_id}, node_id: {node_id}, status: {status}")]
    async fn update_node_status(&self, workflow_id: i64, node_id: &str, status: &str) -> Result<u64, Error> {
        let repo = self.state.workflow_repo.lock().await;
        let param = WorkflowInfo {
            base: EntityBase::new_with_id(Some(workflow_id)),
            name: None,
            provider: None,
            status: None,
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        };
        let res = repo
            .get(&self.state.config)
            .select(param, PageRequest::default())
            .await
            .expect("Failed to get workflow")
            .1;

        if res.is_empty() {
            return Err(Error::msg(format!("Workflow with id {} not found", workflow_id)));
        }

        let mut updated = res.get(0).unwrap().clone();
        if let Some(ref mut flow_info) = updated.flow_info {
            for node in flow_info.nodes.iter_mut() {
                if node.id == node_id {
                    node.status = Some(status.to_string());
                    break;
                }
            }
        }

        repo.get(&self.state.config).update(updated).await?;
        Ok(1)
    }
}
