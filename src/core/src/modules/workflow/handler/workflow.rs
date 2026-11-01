//! Tenant-scoped workflow lifecycle. Identity is explicit, never ambient.
use crate::context::identity::RequestIdentity;
use crate::context::state::SigbotState;
use crate::modules::workflow::storage::postgres::WorkflowPostgresRepository;
use anyhow::ensure;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sigbot_types::workflow::workflow::DeleteWorkflowRequest;
use sigbot_types::workflow::workflow::JobStatus;
use sigbot_types::workflow::workflow::QueryWorkflowRequest;
use sigbot_types::workflow::workflow::SaveWorkflowRequest;
use sigbot_types::workflow::workflow::WorkflowInfo;
use sigbot_types::EntityBase;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use std::sync::Arc;
use uuid::Uuid;

#[async_trait]
pub trait IWorkflowHandler: Send + Sync {
    async fn get(&self, id: Option<Uuid>) -> Result<Option<Arc<WorkflowInfo>>>;
    async fn find(&self, param: QueryWorkflowRequest, page: PageRequest) -> Result<(PageResponse, Vec<WorkflowInfo>)>;
    async fn save(&self, param: SaveWorkflowRequest) -> Result<Uuid>;
    async fn delete(&self, param: DeleteWorkflowRequest) -> Result<u64>;
    async fn find_start_jobs(&self) -> Result<Vec<WorkflowInfo>>;
    async fn find_stop_jobs(&self) -> Result<Vec<WorkflowInfo>>;
    async fn update_status(&self, workflow_id: Uuid, status: JobStatus) -> Result<u64>;
    async fn update_node_status(&self, workflow_id: Uuid, node_id: i64, status: JobStatus) -> Result<u64>;
    async fn overview(&self, env: &str) -> Result<Value>;
    async fn events(&self, env: &str, workflow_id: Uuid) -> Result<Vec<Value>>;
    async fn request_run(&self, env: &str, workflow_id: Uuid, start: bool) -> Result<Uuid>;
}

pub struct WorkflowHandler {
    state: Arc<SigbotState>,
    tenant_id: Option<Uuid>,
    principal_id: Option<String>,
    identity: Option<RequestIdentity>,
    service_env: Option<String>,
}

impl WorkflowHandler {
    /// Worker processes must configure their tenant and service principal explicitly.
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self {
            tenant_id: state.config.authguard.service_tenant_id,
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
            service_env: None,
        })
    }

    pub fn for_scope(
        state: Arc<SigbotState>,
        tenant_id: Uuid,
        principal_id: impl Into<String>,
        env: impl Into<String>,
    ) -> Result<Self> {
        let principal_id = principal_id.into();
        let env = env.into();
        ensure!(tenant_id.get_version_num() == 7, "workflow tenant scope required");
        ensure!(!principal_id.is_empty(), "workflow principal required");
        ensure!(!env.is_empty(), "workflow environment required");
        Ok(Self {
            state,
            tenant_id: Some(tenant_id),
            principal_id: Some(principal_id),
            identity: None,
            service_env: Some(env),
        })
    }

    fn authorize_scope(&self, env: &str, workflow_id: Option<Uuid>) -> Result<Uuid> {
        let tenant_id = self
            .tenant_id
            .ok_or_else(|| Error::msg("explicit workflow tenant scope required"))?;
        if let Some(identity) = &self.identity {
            identity.require_workflow(env, workflow_id)?;
        } else {
            ensure!(
                self.service_env.as_deref() == Some(env),
                "workflow service isolation mismatch"
            );
        }
        Ok(tenant_id)
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
            ensure!(
                workflow.env.as_ref().is_none_or(|value| value == env),
                "workflow service isolation mismatch"
            );
            workflow.env = Some(env.clone());
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
            workflows.retain(|workflow| {
                workflow
                    .env
                    .as_deref()
                    .is_some_and(|env| identity.require_workflow(env, workflow.base.id).is_ok())
            });
            page.total = None; // A total must not reveal resources denied by row-level grants.
        }
        Ok((page, workflows))
    }

    async fn store(&self, workflow: WorkflowInfo) -> Result<Uuid> {
        let workflow = self.scoped(workflow)?;
        if let Some(identity) = &self.identity {
            identity.require_workflow(
                workflow
                    .env
                    .as_deref()
                    .ok_or_else(|| Error::msg("workflow environment required"))?,
                workflow.base.id,
            )?;
        }
        let repository = self.state.workflow_repo.lock().await;
        repository.get(&self.state.config).update(workflow).await
    }
}

#[async_trait]
impl IWorkflowHandler for WorkflowHandler {
    async fn get(&self, id: Option<Uuid>) -> Result<Option<Arc<WorkflowInfo>>> {
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
                None,
            )?;
        }
        self.query(param.to_entity(), page).await
    }

    async fn save(&self, param: SaveWorkflowRequest) -> Result<Uuid> {
        if let Some(flow) = &param.flow_info {
            sigbot_types::workflow::deployment::WorkflowDeployment::validate(flow, false)?;
            let requested = param
                .plain_configuration
                .as_ref()
                .and_then(|value| value.get("mode"))
                .map(|value| serde_json::json!(value));
            sigbot_types::workflow::deployment::WorkflowDeployment::execution_mode(flow, requested.as_ref())?;
        }
        ensure!(
            param
                .secret_configuration
                .as_ref()
                .is_none_or(|secrets| secrets.is_empty()),
            "workflow credentials must use host Secret references"
        );
        sigbot_types::messager::topic::MessageScope {
            env: param.env.clone(),
            tenant_id: self
                .tenant_id
                .ok_or_else(|| Error::msg("workflow tenant scope required"))?,
        }
        .validate()?;
        if let Some(identity) = &self.identity {
            identity.require_workflow(&param.env, param.id)?;
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
                existing.env.as_deref() == Some(param.env.as_str()),
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
        jobs.retain(|job| job.env.as_deref().is_some_and(|env| !env.is_empty()));
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

    async fn update_status(&self, workflow_id: Uuid, status: JobStatus) -> Result<u64> {
        let workflow = self
            .get(Some(workflow_id))
            .await?
            .ok_or_else(|| Error::msg("workflow not found"))?;
        let mut workflow = (*workflow).clone();
        workflow.status = Some(status);
        self.store(workflow).await?;
        Ok(1)
    }

    async fn update_node_status(&self, workflow_id: Uuid, node_id: i64, status: JobStatus) -> Result<u64> {
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

    async fn overview(&self, env: &str) -> Result<Value> {
        let tenant_id = self.authorize_scope(env, None)?;
        let pool = self.state.postgres_pool().await?;
        let mut result = WorkflowPostgresRepository::overview(&pool, tenant_id, env).await?;
        if let (Some(identity), Some(runs)) = (&self.identity, result["recent_runs"].as_array_mut()) {
            runs.retain(|run| {
                run["workflow_id"]
                    .as_str()
                    .and_then(|id| id.parse().ok())
                    .is_some_and(|id| identity.require_workflow(env, Some(id)).is_ok())
            });
        }
        Ok(result)
    }

    async fn events(&self, env: &str, workflow_id: Uuid) -> Result<Vec<Value>> {
        let tenant_id = self.authorize_scope(env, Some(workflow_id))?;
        let pool = self.state.postgres_pool().await?;
        WorkflowPostgresRepository::events(&pool, tenant_id, env, workflow_id).await
    }

    async fn request_run(&self, env: &str, workflow_id: Uuid, start: bool) -> Result<Uuid> {
        let tenant_id = self.authorize_scope(env, Some(workflow_id))?;
        let principal = self
            .principal_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::msg("explicit workflow principal required"))?;
        let pool = self.state.postgres_pool().await?;
        WorkflowPostgresRepository::request_run(&pool, tenant_id, env, workflow_id, principal, start).await
    }
}
