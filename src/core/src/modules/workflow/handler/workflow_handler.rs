//! Tenant-scoped workflow lifecycle. Identity is explicit, never ambient.
use crate::context::{identity::RequestIdentity, state::SigbotState};
use anyhow::{ensure, Error, Result};
use async_trait::async_trait;
use sigbot_types::modules::workflow::workflow::{
    DeleteWorkflowRequest, JobStatus, QueryWorkflowRequest, SaveWorkflowRequest, WorkflowInfo,
};
use sigbot_types::{EntityBase, PageRequest, PageResponse};
use std::sync::Arc;

#[async_trait]
pub trait IWorkflowInfoHandler: Send + Sync {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WorkflowInfo>>>;
    async fn find(&self, param: QueryWorkflowRequest, page: PageRequest) -> Result<(PageResponse, Vec<WorkflowInfo>)>;
    async fn save(&self, param: SaveWorkflowRequest) -> Result<i64>;
    async fn delete(&self, param: DeleteWorkflowRequest) -> Result<u64>;
    async fn find_start_jobs(&self) -> Result<Vec<WorkflowInfo>>;
    async fn find_stop_jobs(&self) -> Result<Vec<WorkflowInfo>>;
    async fn update_status(&self, workflow_id: i64, status: JobStatus) -> Result<u64>;
    async fn update_node_status(&self, workflow_id: i64, node_id: i64, status: JobStatus) -> Result<u64>;
}

pub struct WorkflowInfoHandler {
    state: Arc<SigbotState>,
    tenant_id: Option<i64>,
    principal_id: Option<String>,
    identity: Option<RequestIdentity>,
    service_space_id: Option<i64>,
    service_env: Option<String>,
}

impl WorkflowInfoHandler {
    /// Worker processes must configure their tenant and service principal explicitly.
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self {
            tenant_id: state.config.authguard.service_tenant_id,
            service_space_id: state.config.authguard.service_space_id,
            service_env: state.config.authguard.service_env.clone(),
            identity: None,
            principal_id: state.config.authguard.service_principal_id.clone(),
            state,
        }
    }

    pub fn for_identity(state: Arc<SigbotState>, identity: &RequestIdentity) -> Result<Self> {
        Ok(Self {
            state,
            tenant_id: Some(identity.tenant_id().parse()?),
            principal_id: Some(identity.principal_id().into()),
            identity: Some(identity.clone()),
            service_space_id: None,
            service_env: None,
        })
    }

    fn scoped(&self, mut workflow: WorkflowInfo) -> Result<WorkflowInfo> {
        let tenant_id = self
            .tenant_id
            .ok_or_else(|| Error::msg("explicit workflow tenant scope required"))?;
        let actor = self
            .principal_id
            .as_ref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::msg("explicit workflow principal required"))?;
        ensure!(
            workflow.tenant_id.is_none_or(|value| value == tenant_id),
            "workflow tenant mismatch"
        );
        workflow.tenant_id = Some(tenant_id);
        if self.identity.is_none() {
            let env = self
                .service_env
                .as_ref()
                .ok_or_else(|| Error::msg("explicit service environment required"))?;
            let space_id = self
                .service_space_id
                .ok_or_else(|| Error::msg("explicit service space required"))?;
            ensure!(
                workflow.env.as_ref().is_none_or(|value| value == env)
                    && workflow.space_id.is_none_or(|value| value == space_id),
                "workflow service isolation mismatch"
            );
            workflow.env = Some(env.clone());
            workflow.space_id = Some(space_id);
        }
        workflow.base.created_by.get_or_insert_with(|| actor.clone());
        workflow.base.updated_by = Some(actor.clone());
        Ok(workflow)
    }

    async fn query(&self, workflow: WorkflowInfo, page: PageRequest) -> Result<(PageResponse, Vec<WorkflowInfo>)> {
        let mut workflow = self.scoped(workflow)?;
        // Actor audit fields are not search predicates.
        workflow.base.created_by = None;
        workflow.base.updated_by = None;
        let repository = self.state.workflow_repo.lock().await;
        let (mut page, mut workflows) = repository.get(&self.state.config).select(workflow, page).await?;
        if let Some(identity) = &self.identity {
            workflows.retain(|workflow| match (&workflow.env, workflow.space_id) {
                (Some(env), Some(space_id)) => identity.require_workflow(env, space_id, workflow.base.id).is_ok(),
                _ => false,
            });
            page.total = None; // A total must not reveal resources denied by row-level grants.
        }
        Ok((page, workflows))
    }

    async fn store(&self, workflow: WorkflowInfo) -> Result<i64> {
        let workflow = self.scoped(workflow)?;
        if let Some(identity) = &self.identity {
            identity.require_workflow(
                workflow
                    .env
                    .as_deref()
                    .ok_or_else(|| Error::msg("workflow environment required"))?,
                workflow.space_id.ok_or_else(|| Error::msg("workflow space required"))?,
                workflow.base.id,
            )?;
        }
        let repository = self.state.workflow_repo.lock().await;
        repository.get(&self.state.config).update(workflow).await
    }
}

#[async_trait]
impl IWorkflowInfoHandler for WorkflowInfoHandler {
    async fn get(&self, id: Option<i64>) -> Result<Option<Arc<WorkflowInfo>>> {
        ensure!(id.is_some(), "workflow id required");
        let workflow = WorkflowInfo {
            base: EntityBase::new_empty(),
            status: None,
            ..WorkflowInfo::default()
        };
        let mut workflow = workflow;
        workflow.base.id = id;
        let workflow = self.query(workflow, PageRequest::new(1, 1)).await?.1.into_iter().next();
        if let (Some(identity), Some(workflow)) = (&self.identity, &workflow) {
            identity.require_workflow(
                workflow
                    .env
                    .as_deref()
                    .ok_or_else(|| Error::msg("unbound environment"))?,
                workflow.space_id.ok_or_else(|| Error::msg("unbound space"))?,
                workflow.base.id,
            )?;
        }
        Ok(workflow.map(Arc::new))
    }

    async fn find(&self, param: QueryWorkflowRequest, page: PageRequest) -> Result<(PageResponse, Vec<WorkflowInfo>)> {
        if let Some(identity) = &self.identity {
            identity.require_workflow(
                param
                    .env
                    .as_deref()
                    .ok_or_else(|| Error::msg("environment query required"))?,
                param.space_id.ok_or_else(|| Error::msg("space query required"))?,
                None,
            )?;
        }
        self.query(param.to_entity(), page).await
    }

    async fn save(&self, param: SaveWorkflowRequest) -> Result<i64> {
        if let Some(flow) = &param.flow_info {
            sigbot_types::modules::workflow::deployment::WorkflowDeployment::validate(flow, false)?;
        }
        ensure!(
            param
                .secret_configuration
                .as_ref()
                .is_none_or(|secrets| secrets.is_empty()),
            "workflow credentials must use host Secret references"
        );
        ensure!(
            matches!(param.env.as_str(), "live" | "paper" | "simulated" | "backtest"),
            "invalid workflow environment"
        );
        ensure!(param.space_id > 0, "space_id must be positive");
        if let Some(identity) = &self.identity {
            identity.require_workflow(&param.env, param.space_id, param.id)?;
        }
        if let Some(ref status) = param.status {
            JobStatus::of(status).map_err(Error::msg)?;
        }
        if let Some(id) = param.id {
            let existing = self
                .get(Some(id))
                .await?
                .ok_or_else(|| Error::msg("workflow not found"))?;
            ensure!(
                existing.env.as_deref() == Some(param.env.as_str()) && existing.space_id == Some(param.space_id),
                "workflow isolation cannot be changed"
            );
            ensure!(
                matches!(
                    existing.status,
                    Some(JobStatus::STOPPED | JobStatus::SUCCESS | JobStatus::FAILED)
                ),
                "stop workflow before editing"
            );
            let mut updated = param.to_entity();
            updated.status = existing.status.clone();
            updated.base.created_at = existing.base.created_at;
            updated.base.created_by = existing.base.created_by.clone();
            if updated.secrets.is_none() {
                updated.secrets = existing.secrets.clone();
            }
            self.store(updated).await
        } else {
            let mut workflow = self.scoped(param.to_entity())?;
            workflow.status = Some(JobStatus::STOPPED);
            let repository = self.state.workflow_repo.lock().await;
            repository.get(&self.state.config).upsert(workflow).await
        }
    }

    async fn delete(&self, param: DeleteWorkflowRequest) -> Result<u64> {
        let workflow = self
            .get(Some(param.id))
            .await?
            .ok_or_else(|| Error::msg("workflow not found"))?;
        ensure!(
            matches!(
                workflow.status,
                Some(JobStatus::STOPPED | JobStatus::SUCCESS | JobStatus::FAILED)
            ),
            "stop workflow before deletion"
        );
        let mut workflow = (*workflow).clone();
        workflow.base.del_flag = Some(1);
        self.store(workflow).await?;
        Ok(1)
    }

    async fn find_start_jobs(&self) -> Result<Vec<WorkflowInfo>> {
        let mut jobs = self
            .query(
                WorkflowInfo {
                    status: Some(JobStatus::PENDING),
                    ..WorkflowInfo::default()
                },
                PageRequest::new(1, 1000),
            )
            .await?
            .1;
        let running = self
            .query(
                WorkflowInfo {
                    status: Some(JobStatus::RUNNING),
                    ..WorkflowInfo::default()
                },
                PageRequest::new(1, 1000),
            )
            .await?
            .1;
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(180);
        jobs.extend(
            running
                .into_iter()
                .filter(|job| job.base.updated_at.is_some_and(|at| at < cutoff)),
        );
        jobs.retain(|job| job.env.as_deref().is_some_and(|env| env != "legacy") && job.space_id.is_some());
        Ok(jobs)
    }

    async fn find_stop_jobs(&self) -> Result<Vec<WorkflowInfo>> {
        Ok(self
            .query(
                WorkflowInfo {
                    status: Some(JobStatus::STOPPING),
                    ..WorkflowInfo::default()
                },
                PageRequest::new(1, 1000),
            )
            .await?
            .1)
    }

    async fn update_status(&self, workflow_id: i64, status: JobStatus) -> Result<u64> {
        let workflow = self
            .get(Some(workflow_id))
            .await?
            .ok_or_else(|| Error::msg("workflow not found"))?;
        let mut workflow = (*workflow).clone();
        workflow.status = Some(status);
        self.store(workflow).await?;
        Ok(1)
    }

    async fn update_node_status(&self, workflow_id: i64, node_id: i64, status: JobStatus) -> Result<u64> {
        let workflow = self
            .get(Some(workflow_id))
            .await?
            .ok_or_else(|| Error::msg("workflow not found"))?;
        let mut workflow = (*workflow).clone();
        let node = workflow
            .flow_info
            .as_mut()
            .and_then(|flow| flow.nodes.iter_mut().find(|node| node.id == node_id))
            .ok_or_else(|| Error::msg("workflow node not found"))?;
        node.status = Some(status);
        self.store(workflow).await?;
        Ok(1)
    }
}
