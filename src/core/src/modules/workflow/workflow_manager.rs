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
use crate::modules::workflow::handler::workflow_handler::{IWorkflowInfoHandler, WorkflowInfoHandler};
use crate::sys::handler::dlock_handler::{DLockHandler, IDLockHandler};
use anyhow::Error;
use common_telemetry::{debug, info, warn};
use lazy_static::lazy_static;
use sigbot_types::modules::workflow::workflow::{JobStatus, WorkflowInfo, WorkflowNodeInfo, WorkflowStageType};
use sigbot_utils::dash_maps::ConcurrentMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};

lazy_static! {
    static ref SINGLETON_INSTANCE: Arc<RwLock<Option<Arc<SigbotWorkflowManager>>>> = Arc::new(RwLock::new(None));
}

pub type SigbotWorkflowStartHandler =
    Arc<dyn Fn(Arc<SigbotWorkflowNodeJob>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>;

pub type SigbotWorkflowStopHandler =
    Arc<dyn Fn(Arc<SigbotWorkflowNodeJob>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>;

pub type SigbotWorkflowHandlerWrapper = (SigbotWorkflowStartHandler, SigbotWorkflowStopHandler);

/// WorkflowManager manages workflow lifecycle by scanning s_workflow table to start and stop flow node jobs.
#[derive(Clone)]
pub struct SigbotWorkflowManager {
    #[allow(unused)]
    state: Arc<SigbotState>,
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    call_handlers: Option<SigbotWorkflowHandlerWrapper>,
    /// Job registry: workflow_id => map{node_id -> InternalWorkflowNodeJob}, Supports multiple nodes per workflow (e.g., multiple evaluators)
    job_registry: Arc<ConcurrentMap<i64, Arc<ConcurrentMap<i64, Arc<SigbotWorkflowNodeJob>>>>>,
    dlock_handler: Arc<dyn IDLockHandler>,
    workflow_handler: Arc<dyn IWorkflowInfoHandler>,
    start_lock_timeout: Duration,
    /// Supported stage type. e.g. EVALUATION for strategy runner, INPUT for datafeed ingestor.
    supported_stage_type: WorkflowStageType,
}

impl SigbotWorkflowManager {
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_PROCESS_TIMEOUT: Duration = Duration::from_millis(3);
    pub const DEFAULT_WF_NODE_START_TIMEOUT: Duration = Duration::from_secs(15);

    pub fn get() -> &'static Arc<RwLock<Option<Arc<SigbotWorkflowManager>>>> {
        &SINGLETON_INSTANCE
    }

    /// Create a new WorkflowManager with start and stop handlers as a tuple
    /// The handlers must be provided together as a tuple
    ///
    /// # Arguments
    /// * `state` - SigbotState instance
    /// * `handlers` - Tuple of start and stop handlers
    /// * `supported_stage_type` - The stage type this microservice supports
    ///   - `WorkflowStageType::EVALUATION(vec![])` for strategy runner
    ///   - `WorkflowStageType::INPUT(vec![])` for datafeed ingestor
    ///   - `WorkflowStageType::POST(vec![])` for notification forwarder
    ///   - etc.
    /// * `schedule_cron` - Optional cron expression for scheduling
    /// * `schedule_channels` - Optional number of channels for the scheduler
    pub fn new(
        state: Arc<SigbotState>,
        handlers: SigbotWorkflowHandlerWrapper,
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
        supported_stage_type: WorkflowStageType,
    ) -> Arc<Self> {
        let dlock_handler = Arc::new(DLockHandler::new(state.to_owned()));
        let workflow_handler = Arc::new(WorkflowInfoHandler::new(state.to_owned()));
        Arc::new(Self {
            state,
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            call_handlers: Some(handlers),
            job_registry: Arc::new(ConcurrentMap::new()),
            dlock_handler,
            workflow_handler,
            start_lock_timeout: Self::DEFAULT_WF_NODE_START_TIMEOUT,
            supported_stage_type,
        })
    }

    /// Initialize, start cron job, and set the global singleton instance
    pub async fn startup(
        self: Arc<Self>,
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
    ) -> Result<Arc<Self>, Error> {
        // Update schedule config if provided
        let cron_expr = schedule_cron
            .as_deref()
            .or(self.schedule_cron.as_deref())
            .unwrap_or(Self::DEFAULT_CRON_EXPRESSION);
        let channel_size = schedule_channels
            .or(self.schedule_channels)
            .unwrap_or(Self::DEFAULT_CHANNELS);

        // Validate the cron expression
        let cron = match Job::new_async(cron_expr, |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => cron_expr,
            Err(e) => {
                warn!(
                    "Invalid cron expression '{}': {}. Using default '{}'",
                    cron_expr,
                    e,
                    Self::DEFAULT_CRON_EXPRESSION
                );
                Self::DEFAULT_CRON_EXPRESSION
            }
        };

        debug!("Starting workflow manager with cron '{}'", cron);
        let this = self.to_owned();
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.to_owned();
            Box::pin(async move {
                debug!("Executing workflow manager scan job ...");
                that.execute().await;
                debug!("Executed workflow manager scan job ...");
            })
        })
        .map_err(|e| Error::msg(format!("Failed to create workflow manager job: {}", e)))?;

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .map_err(|e| Error::msg(format!("Failed to create scheduler: {}", e)))?;
        scheduler
            .add(job)
            .await
            .map_err(|e| Error::msg(format!("Failed to add workflow manager job: {}", e)))?;
        scheduler
            .start()
            .await
            .map_err(|e| Error::msg(format!("Failed to start workflow manager scheduler: {}", e)))?;

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started workflow manager with cron '{}', channels '{}'",
            cron, channel_size
        );

        // Set the global singleton instance
        {
            let mut guard = SINGLETON_INSTANCE.write().await;
            *guard = Some(self.to_owned());
        }

        Ok(self)
    }

    /// Execute the workflow scan job with distributed lock
    pub(super) async fn execute(&self) {
        // Acquire distributed lock
        let dlock_name = "WORKFLOW_MANAGER";

        let acquired = &self
            .dlock_handler
            .acquire(dlock_name.to_string(), Self::DEFAULT_PROCESS_TIMEOUT)
            .await;
        match acquired {
            Ok(true) => {
                self.process().await;
            }
            Ok(false) => {
                warn!("Unable to acquire dlock with {}", dlock_name);
            }
            Err(e) => {
                warn!("Failed to acquire dlock: {}", e);
            }
        }

        info!("Executed workflow manager process ...");
    }

    /// Process scans s_workflow table and calls registered handlers
    pub(super) async fn process(&self) {
        info!("Processing workflow manager scan ...");

        let (start_handler, stop_handler) = match &self.call_handlers {
            Some(handlers) => handlers,
            None => {
                warn!("Workflow handlers are not registered");
                return;
            }
        };

        // Query workflows that need to be stopped
        // Find workflows with status=CANCELLING OR status=STOPPING
        let stop_workflows = {
            match self.workflow_handler.find_stop_jobs().await {
                Ok(workflows) => workflows,
                Err(e) => {
                    warn!("Failed to query to-stop workflows: {}", e);
                    Vec::new()
                }
            }
        };

        // Query workflows that need to be started
        // Find workflows with status=PENDING OR (status=RUNNING AND updated_at < NOW() - '180 seconds')
        let start_workflows = {
            match self.workflow_handler.find_start_jobs().await {
                Ok(workflows) => workflows,
                Err(e) => {
                    warn!("Failed to query to-start workflows: {}", e);
                    Vec::new()
                }
            }
        };

        // Stop workflows that need to be stopped (process stop first)
        if !stop_workflows.is_empty() {
            info!("Found {} workflows to stop", stop_workflows.len());
            for workflow in stop_workflows {
                let workflow_id = workflow.base.id.unwrap_or(0);

                // Get all node jobs for this workflow before stopping
                let node_jobs = self.get_node_jobs(workflow_id).await;
                if !node_jobs.is_empty() {
                    info!("Stopping workflow {} with {} nodes", workflow_id, node_jobs.len());
                    // Set all node jobs to STOPPING
                    for job in &node_jobs {
                        let node_id = job.node_id();
                        job.set_status(JobStatus::STOPPING).await;
                        if let Err(e) = self
                            .workflow_handler
                            .update_node_status(workflow_id, node_id, JobStatus::STOPPING)
                            .await
                        {
                            warn!("Failed to update node {} status to Stopping: {}", node_id, e);
                        }
                    }

                    if let Err(err) = self
                        .workflow_handler
                        .update_status(workflow_id, JobStatus::STOPPING)
                        .await
                    {
                        warn!("Failed to update workflow {} status to Stopping: {}", workflow_id, err);
                    }

                    let manager0 = Arc::new(self.to_owned());

                    // Spawn async task to handle the actual async stop operation
                    tokio::spawn(async move {
                        for job in &node_jobs {
                            let node_id = job.node_id();
                            match job.stop().await {
                                Ok(_) => {
                                    // Update job status to STOPPED immediately
                                    job.set_status(JobStatus::STOPPED).await;

                                    // Update node status to STOPPED immediately
                                    if let Err(e) = manager0
                                        .workflow_handler
                                        .update_node_status(workflow_id, node_id, JobStatus::STOPPED)
                                        .await
                                    {
                                        warn!("Failed to update node {} status to stopped: {}", node_id, e);
                                    } else {
                                        info!("Successfully stopped workflow {} node {}", workflow_id, node_id);
                                    }
                                }
                                Err(e) => {
                                    // Update job status to FAILED immediately
                                    job.set_status(JobStatus::FAILED).await;

                                    // Update node status to FAILED immediately
                                    if let Err(update_err) = manager0
                                        .workflow_handler
                                        .update_node_status(workflow_id, node_id, JobStatus::FAILED)
                                        .await
                                    {
                                        warn!("Failed to update node {} status to failed: {}", node_id, update_err);
                                    }
                                    warn!("Failed to stop workflow {} node {}: {}", workflow_id, node_id, e);
                                }
                            }
                        }
                    });
                }
            }
        }

        // Start all workflows that need to be started and are not already running
        // Use distributed lock to prevent multiple pod instances from starting the same workflow
        if !start_workflows.is_empty() {
            info!("Found {} workflows to start", start_workflows.len());

            for workflow in start_workflows {
                let workflow0 = Arc::new(workflow);
                let workflow_id = workflow0.base.id.unwrap_or(0);

                // Check if workflow already has registered jobs
                if self.has_node_jobs(workflow_id).await {
                    debug!("Workflow {} is already running, skipping", workflow_id);
                    continue;
                }

                // Acquire distributed lock for this workflow to prevent duplicate starts
                let dlock_name = format!("WORKFLOW_START_{}", workflow_id);
                let lock_acquired = match &self
                    .dlock_handler
                    .acquire(dlock_name.to_owned(), self.start_lock_timeout)
                    .await
                {
                    Ok(true) => true,
                    Ok(false) => {
                        debug!(
                            "Unable to acquire distributed lock for workflow {}, another instance may be starting it",
                            workflow_id
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!("Failed to acquire distributed lock for workflow {}: {}", workflow_id, e);
                        continue;
                    }
                };
                if !lock_acquired {
                    continue;
                }

                // Find all nodes matching the supported stage type
                let node_ids_to_start = workflow0.get_stage_nodes(self.supported_stage_type.clone());

                if let Err(err) = self
                    .workflow_handler
                    .update_status(workflow_id, JobStatus::STARTING)
                    .await
                {
                    warn!(
                        "Failed to update workflow {} status to Starting before start: {}",
                        workflow_id, err
                    );
                }

                // Register jobs for all nodes before calling handler
                let mut registered_jobs = Vec::new();
                let manager0 = Arc::new(self.to_owned());
                for node_id in &node_ids_to_start {
                    // Get node info from workflow
                    let node_info = match workflow0
                        .flow_info
                        .as_ref()
                        .and_then(|flow_info| flow_info.nodes.iter().find(|n| n.id == *node_id))
                    {
                        Some(node) => Arc::new(node.clone()),
                        None => {
                            warn!("Node {} not found in workflow {}", node_id, workflow_id);
                            continue;
                        }
                    };

                    let job = Arc::new(SigbotWorkflowNodeJob::new(
                        workflow0.to_owned(),
                        node_info.clone(),
                        Some(start_handler.to_owned()),
                        Some(stop_handler.to_owned()),
                    ));
                    // Set node status to STARTING before starting
                    job.set_status(JobStatus::STARTING).await;
                    if let Err(e) = manager0
                        .workflow_handler
                        .update_node_status(workflow_id, *node_id, JobStatus::STARTING)
                        .await
                    {
                        warn!(
                            "Failed to update node {} status to Starting before start: {}",
                            node_id, e
                        );
                    }
                    self.register_job(job.to_owned()).await;
                    registered_jobs.push((*node_id, job));
                }

                // Release the distributed lock after jobs are registered
                // The lock is only needed to prevent concurrent starts, once the jobs are registered,
                // the job registry will prevent duplicate starts
                if let Err(release_err) = self.dlock_handler.release(dlock_name.to_owned()).await {
                    warn!(
                        "Failed to release distributed lock for workflow {}: {}",
                        workflow_id, release_err
                    );
                }

                // Spawn async task to handle the actual async start operation
                // Call start handler for each node job and handle results
                tokio::spawn(async move {
                    // Call start handler asynchronously for each node job
                    // Each node job (e.g., LLM, PYCODE) needs to be started separately
                    // Update status immediately after each node job starts
                    let mut success_count = 0;
                    let mut failure_count = 0;
                    let mut first_error: Option<Error> = None;
                    let mut successful_jobs = Vec::new();

                    for (node_id, job) in &registered_jobs {
                        match job.start().await {
                            Ok(_) => {
                                // Update job status to RUNNING immediately
                                job.set_status(JobStatus::RUNNING).await;

                                // Update node status to RUNNING immediately
                                if let Err(e) = manager0
                                    .workflow_handler
                                    .update_node_status(workflow_id, *node_id, JobStatus::RUNNING)
                                    .await
                                {
                                    warn!("Failed to update node {} status to running: {}", node_id, e);
                                } else {
                                    info!("Successfully started workflow {} node {}", workflow_id, node_id);
                                }

                                success_count += 1;
                                successful_jobs.push((*node_id, job.clone()));
                            }
                            Err(e) => {
                                // Update job status to FAILED immediately
                                job.set_status(JobStatus::FAILED).await;

                                // Update node status to FAILED immediately
                                if let Err(update_err) = manager0
                                    .workflow_handler
                                    .update_node_status(workflow_id, *node_id, JobStatus::FAILED)
                                    .await
                                {
                                    warn!("Failed to update node {} status to failed: {}", node_id, update_err);
                                }

                                warn!("Failed to start workflow {} node {}: {}", workflow_id, node_id, e);
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                                failure_count += 1;
                            }
                        }
                    }

                    // If any node job failed to start, stop all successfully started node jobs
                    if failure_count > 0 && !successful_jobs.is_empty() {
                        warn!(
                            "Some nodes failed to start for workflow {}, stopping {} successfully started nodes",
                            workflow_id,
                            successful_jobs.len()
                        );
                        for (node_id, job) in &successful_jobs {
                            // Set status to STOPPING before stopping
                            job.set_status(JobStatus::STOPPING).await;
                            if let Err(e) = manager0
                                .workflow_handler
                                .update_node_status(workflow_id, *node_id, JobStatus::STOPPING)
                                .await
                            {
                                warn!("Failed to update node {} status to Stopping: {}", node_id, e);
                            }

                            match job.stop().await {
                                Ok(_) => {
                                    job.set_status(JobStatus::STOPPED).await;
                                    if let Err(e) = manager0
                                        .workflow_handler
                                        .update_node_status(workflow_id, *node_id, JobStatus::STOPPED)
                                        .await
                                    {
                                        warn!("Failed to update node {} status to stopped: {}", node_id, e);
                                    } else {
                                        info!(
                                            "Successfully stopped workflow {} node {} after start failure",
                                            workflow_id, node_id
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to stop workflow {} node {} after start failure: {}",
                                        workflow_id, node_id, e
                                    );
                                    job.set_status(JobStatus::FAILED).await;
                                    if let Err(update_err) = manager0
                                        .workflow_handler
                                        .update_node_status(workflow_id, *node_id, JobStatus::FAILED)
                                        .await
                                    {
                                        warn!("Failed to update node {} status to failed: {}", node_id, update_err);
                                    }
                                }
                            }
                        }
                    }

                    // Update workflow status based on overall results
                    if failure_count == 0 {
                        // All nodes started successfully
                        if let Err(e) = manager0
                            .workflow_handler
                            .update_status(workflow_id, JobStatus::RUNNING)
                            .await
                        {
                            warn!("Failed to update workflow {} status to Running: {}", workflow_id, e);
                        } else {
                            info!(
                                "Successfully started workflow: {} with {} nodes",
                                workflow_id, success_count
                            );
                        }
                    } else {
                        // Some nodes failed to start
                        if let Err(update_err) = manager0
                            .workflow_handler
                            .update_status(workflow_id, JobStatus::FAILED)
                            .await
                        {
                            warn!(
                                "Failed to update workflow {} status to Failed: {}",
                                workflow_id, update_err
                            );
                        }

                        // Unregister all failed jobs
                        let unregistered_jobs = manager0.unregister_workflow_jobs(workflow_id).await;
                        for job in unregistered_jobs {
                            job.set_status(JobStatus::FAILED).await;
                        }

                        if let Some(err) = first_error {
                            warn!(
                                "Failed to start workflow {}: {} nodes succeeded, {} nodes failed. First error: {}",
                                workflow_id, success_count, failure_count, err
                            );
                        }
                    }
                });
            }
        }
    }

    /// Register a workflow node job in the registry
    pub async fn register_job(&self, job: Arc<SigbotWorkflowNodeJob>) {
        let workflow_id = job.workflow_id();
        let node_id = job.node_id();

        // Get or create the inner map for this workflow
        let node_map = if let Some(existing_map) = self.job_registry.get(&workflow_id) {
            existing_map
        } else {
            let new_map = Arc::new(ConcurrentMap::new());
            self.job_registry.insert(workflow_id, new_map.clone());
            new_map
        };

        if node_map.contains_key(&node_id) {
            debug!("Workflow {} node {} is already registered", workflow_id, node_id);
            return;
        }

        node_map.insert(node_id, job);
        debug!("Registered workflow {} node {} job", workflow_id, node_id);
    }

    /// Unregister all node jobs for a workflow
    pub async fn unregister_workflow_jobs(&self, workflow_id: i64) -> Vec<Arc<SigbotWorkflowNodeJob>> {
        if let Some(node_map) = self.job_registry.remove(&workflow_id) {
            let mut jobs = Vec::new();
            for job in node_map.iter() {
                jobs.push(job);
            }
            jobs
        } else {
            Vec::new()
        }
    }

    /// Unregister a specific workflow node job from the registry
    pub async fn unregister_node_job(&self, workflow_id: i64, node_id: i64) -> Option<Arc<SigbotWorkflowNodeJob>> {
        if let Some(node_map) = self.job_registry.get(&workflow_id) {
            if let Some(job) = node_map.remove(&node_id) {
                // If the node map is empty, remove the workflow entry
                if node_map.is_empty() {
                    self.job_registry.remove(&workflow_id);
                }
                Some(job)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Check if a workflow has any registered jobs
    pub async fn has_node_jobs(&self, workflow_id: i64) -> bool {
        self.job_registry.contains_key(&workflow_id)
    }

    /// Get a specific workflow node job by workflow ID and node ID
    pub async fn get_node_job(&self, workflow_id: i64, node_id: i64) -> Option<Arc<SigbotWorkflowNodeJob>> {
        self.job_registry
            .get(&workflow_id)
            .and_then(|node_map| node_map.get(&node_id))
    }

    /// Get all node jobs for a workflow
    pub async fn get_node_jobs(&self, workflow_id: i64) -> Vec<Arc<SigbotWorkflowNodeJob>> {
        if let Some(node_map) = self.job_registry.get(&workflow_id) {
            let mut jobs = Vec::new();
            for job in node_map.iter() {
                jobs.push(job);
            }
            jobs
        } else {
            Vec::new()
        }
    }

    /// Get all node jobs by status across all workflows
    pub async fn get_node_jobs_by_status(&self, status: JobStatus) -> Vec<Arc<SigbotWorkflowNodeJob>> {
        let mut result = Vec::new();
        for node_map in self.job_registry.iter() {
            for job in node_map.iter() {
                if job.get_status().await == status {
                    result.push(job);
                }
            }
        }
        result
    }

    /// Shutdown the global singleton instance
    pub async fn shutdown() {
        if let Some(manager) = SINGLETON_INSTANCE.write().await.take() {
            manager.shutdown0().await;
            info!("Shutdown Workflow Manager.");
        }
    }

    /// Shutdown the workflow manager: stop cron job first, then stop all workflow jobs
    async fn shutdown0(&self) {
        info!(
            "Shutting down workflow manager with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );

        // First, stop the cron scheduler
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown workflow manager scheduler");
        }

        // Then, stop all running workflow node jobs
        for node_map in self.job_registry.iter() {
            for job in node_map.iter() {
                let _ = job.stop().await;
            }
        }
        self.job_registry.clear();
        info!("Stopped all workflow jobs.");

        info!(
            "Shutdown workflow manager with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
    }
}

/// Workflow node job manages the lifecycle of a workflow node
#[derive(Clone)]
pub struct SigbotWorkflowNodeJob {
    workflow_info: Arc<WorkflowInfo>,
    node_info: Arc<WorkflowNodeInfo>,
    status: Arc<RwLock<JobStatus>>,
    start_handler: Option<SigbotWorkflowStartHandler>,
    stop_handler: Option<SigbotWorkflowStopHandler>,
}

impl SigbotWorkflowNodeJob {
    pub fn new(
        workflow_info: Arc<WorkflowInfo>,
        node_info: Arc<WorkflowNodeInfo>,
        start_handler: Option<SigbotWorkflowStartHandler>,
        stop_handler: Option<SigbotWorkflowStopHandler>,
    ) -> Self {
        Self {
            workflow_info,
            node_info,
            status: Arc::new(RwLock::new(JobStatus::PENDING)),
            start_handler,
            stop_handler,
        }
    }

    pub fn node_id(&self) -> i64 {
        self.node_info.id
    }

    pub fn node_info(&self) -> &Arc<WorkflowNodeInfo> {
        &self.node_info
    }

    pub fn workflow_id(&self) -> i64 {
        self.workflow_info.base.id.unwrap_or(0)
    }

    pub fn workflow_info(&self) -> &Arc<WorkflowInfo> {
        &self.workflow_info
    }

    pub async fn set_status(&self, status: JobStatus) {
        *self.status.write().await = status;
    }

    pub async fn get_status(&self) -> JobStatus {
        self.status.read().await.to_owned()
    }

    pub async fn start(&self) -> Result<(), Error> {
        if let Some(start_handler) = &self.start_handler {
            let job = Arc::new(self.clone());
            start_handler(job).await?;
        }
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), Error> {
        if let Some(stop_handler) = &self.stop_handler {
            let job = Arc::new(self.clone());
            stop_handler(job).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
