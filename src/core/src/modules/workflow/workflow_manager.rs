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
use sigbot_types::modules::workflow::workflow::{JobStatus, WorkflowInfo};
use sigbot_utils::dash_maps::ConcurrentHashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};

lazy_static! {
    static ref SINGLETON_INSTANCE: Arc<RwLock<Option<Arc<WorkflowManager>>>> = Arc::new(RwLock::new(None));
}

/// Callback function type for starting a workflow
/// The handler receives the workflow info and WorkflowManager, and should perform the actual start operation
/// The async task spawning and status updates are handled by WorkflowManager
pub type WorkflowStartHandler = Arc<dyn Fn(Arc<WorkflowInfo>, Arc<WorkflowManager>) -> Result<(), Error> + Send + Sync>;

/// Callback function type for stopping a workflow
/// The handler receives the workflow id and WorkflowManager, and should perform the actual stop operation
/// The async task spawning and status updates are handled by WorkflowManager
pub type WorkflowStopHandler = Arc<dyn Fn(String, Arc<WorkflowManager>) -> Result<(), Error> + Send + Sync>;

/// Tuple of start and stop handlers, must be provided together
pub type WorkflowCallHandlers = (WorkflowStartHandler, WorkflowStopHandler);

/// WorkflowManager manages workflow lifecycle by scanning s_workflow table
/// Similar to SigbotKlineBacktestManager in backtest_kline.rs
#[derive(Clone)]
pub struct WorkflowManager {
    #[allow(unused)]
    state: Arc<SigbotState>,
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    call_handlers: Option<WorkflowCallHandlers>,
    job_registry: Arc<ConcurrentHashMap<String, Arc<WorkflowNodeJob>>>,
    dlock_handler: Arc<dyn IDLockHandler>,
    workflow_handler: Arc<dyn IWorkflowInfoHandler>,
    start_lock_timeout: Duration,
}

impl WorkflowManager {
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_PROCESS_TIMEOUT: Duration = Duration::from_millis(3);
    pub const DEFAULT_WF_NODE_START_TIMEOUT: Duration = Duration::from_secs(15);

    /// Get the global singleton instance of WorkflowManager
    pub fn get() -> &'static Arc<RwLock<Option<Arc<WorkflowManager>>>> {
        &SINGLETON_INSTANCE
    }

    /// Create a new WorkflowManager with start and stop handlers as a tuple
    /// The handlers must be provided together as a tuple
    pub fn new(
        state: Arc<SigbotState>,
        handlers: WorkflowCallHandlers,
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
    ) -> Arc<Self> {
        let dlock_handler = Arc::new(DLockHandler::new(state.to_owned()));
        let workflow_handler = Arc::new(WorkflowInfoHandler::new(state.to_owned()));
        Arc::new(Self {
            state,
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            call_handlers: Some(handlers),
            job_registry: Arc::new(ConcurrentHashMap::new()),
            dlock_handler,
            workflow_handler,
            start_lock_timeout: Self::DEFAULT_WF_NODE_START_TIMEOUT,
        })
    }

    /// Initialize, start cron job, and set the global singleton instance
    pub async fn startup(
        self: Arc<Self>,
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
    ) -> Result<Arc<Self>, Error> {
        // Update schedule config if provided
        let cron_expression = schedule_cron
            .as_deref()
            .or(self.schedule_cron.as_deref())
            .unwrap_or(Self::DEFAULT_CRON_EXPRESSION);
        let channel_size = schedule_channels
            .or(self.schedule_channels)
            .unwrap_or(Self::DEFAULT_CHANNELS);

        // Validate the cron expression
        let cron = match Job::new_async(cron_expression, |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => cron_expression,
            Err(e) => {
                warn!(
                    "Invalid cron expression '{}': {}. Using default '{}'",
                    cron_expression,
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
                let workflow_id_str = workflow_id.to_string();

                // Get job before stopping
                let job = self.get_node_job(&workflow_id_str).await;
                if let Some(job) = job {
                    info!("Stopping workflow {}", workflow_id_str);
                    job.set_status(JobStatus::STOPPING).await;

                    if let Err(err) = self
                        .workflow_handler
                        .update_status(workflow_id, JobStatus::STOPPING)
                        .await
                    {
                        warn!(
                            "Failed to update workflow {} status to Stopping: {}",
                            workflow_id_str, err
                        );
                    }

                    let manager0 = Arc::new(self.to_owned());
                    let stop_result = stop_handler(workflow_id_str.to_owned(), manager0.to_owned());
                    let workflow_id0 = workflow_id_str.to_owned();

                    // Find nodes that need to be stopped (AI_EVALUATOR or PY_EVALUATOR)
                    let node_ids_to_stop: Vec<String> = if let Some(ref flow_info) = workflow.flow_info {
                        flow_info
                            .nodes
                            .iter()
                            .filter_map(|node| {
                                if node.r#type == "AI_EVALUATOR" || node.r#type == "PY_EVALUATOR" {
                                    Some(node.id.clone())
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };

                    match stop_result {
                        Ok(_) => {
                            let manager1 = manager0.to_owned();
                            let node_ids = node_ids_to_stop.clone();
                            tokio::spawn(async move {
                                info!("Successfully stopped workflow: {}", workflow_id0);
                                if let Err(e) = &manager1
                                    .workflow_handler
                                    .update_status(workflow_id, JobStatus::STOPPED)
                                    .await
                                {
                                    warn!("Failed to update workflow {} status to Stopped: {}", workflow_id0, e);
                                }
                                // Update node statuses to STOPPED
                                for node_id in node_ids {
                                    if let Err(e) = manager1
                                        .workflow_handler
                                        .update_node_status(workflow_id, &node_id, "stopped")
                                        .await
                                    {
                                        warn!("Failed to update node {} status to stopped: {}", node_id, e);
                                    }
                                }
                                if let Some(unregistered_job) = manager1.unregister_job(&workflow_id0).await {
                                    unregistered_job.set_status(JobStatus::STOPPED).await;
                                }
                            });
                        }
                        Err(e) => {
                            warn!("Failed to stop workflow {}: {}", workflow_id_str, e);
                            if let Err(update_err) = self
                                .workflow_handler
                                .update_status(workflow_id, JobStatus::FAILED)
                                .await
                            {
                                warn!(
                                    "Failed to update workflow {} status to Failed: {}",
                                    workflow_id_str, update_err
                                );
                            }
                            if let Some(unregistered_job) = manager0.unregister_job(&workflow_id_str).await {
                                unregistered_job.set_status(JobStatus::FAILED).await;
                            }
                        }
                    }
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
                let workflow_id_str = workflow_id.to_string();

                // Check if job already exists
                if self.get_node_job(&workflow_id_str).await.is_some() {
                    debug!("Workflow {} is already running, skipping", workflow_id_str);
                    continue;
                }

                // Acquire distributed lock for this workflow to prevent duplicate starts
                let dlock_name = format!("WORKFLOW_START_{}", workflow_id_str);
                let lock_acquired = match &self
                    .dlock_handler
                    .acquire(dlock_name.to_owned(), self.start_lock_timeout)
                    .await
                {
                    Ok(true) => true,
                    Ok(false) => {
                        debug!(
                            "Unable to acquire distributed lock for workflow {}, another instance may be starting it",
                            workflow_id_str
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to acquire distributed lock for workflow {}: {}",
                            workflow_id_str, e
                        );
                        continue;
                    }
                };

                if !lock_acquired {
                    continue;
                }

                // Create and register job before calling handler
                let job = Arc::new(WorkflowNodeJob::new(workflow0.to_owned()));
                let job_id = job.workflow_id();
                job.set_status(JobStatus::STARTING).await;
                self.register_job(job.to_owned()).await;

                // Call start handler synchronously (handler should return immediately)
                let manager0 = Arc::new(self.to_owned());
                let start_result = start_handler(workflow0.to_owned(), manager0.to_owned());

                // Release the distributed lock after job is registered and handler is called
                // The lock is only needed to prevent concurrent starts, once the job is registered,
                // the job registry will prevent duplicate starts
                if let Err(release_err) = self.dlock_handler.release(dlock_name.to_owned()).await {
                    warn!(
                        "Failed to release distributed lock for workflow {}: {}",
                        workflow_id_str, release_err
                    );
                }

                // Spawn async task to handle the actual async start operation
                let workflow_id0 = workflow_id_str.to_owned();
                let job0 = job.to_owned();

                // Find nodes that need to be started (AI_EVALUATOR or PY_EVALUATOR)
                let node_ids_to_start: Vec<String> = if let Some(ref flow_info) = workflow0.flow_info {
                    flow_info
                        .nodes
                        .iter()
                        .filter_map(|node| {
                            if node.r#type == "AI_EVALUATOR" || node.r#type == "PY_EVALUATOR" {
                                Some(node.id.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                match start_result {
                    Ok(_) => {
                        let node_ids = node_ids_to_start.clone();
                        tokio::spawn(async move {
                            job0.set_status(JobStatus::RUNNING).await;
                            info!("Successfully started workflow: {}", workflow_id0);
                            if let Err(e) = &manager0
                                .workflow_handler
                                .update_status(workflow_id, JobStatus::RUNNING)
                                .await
                            {
                                warn!("Failed to update workflow {} status to Running: {}", workflow_id0, e);
                            }
                            // Update node statuses to RUNNING
                            for node_id in node_ids {
                                if let Err(e) = manager0
                                    .workflow_handler
                                    .update_node_status(workflow_id, &node_id, "running")
                                    .await
                                {
                                    warn!("Failed to update node {} status to running: {}", node_id, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to start workflow {}: {}", workflow_id_str, e);
                        if let Err(update_err) = self
                            .workflow_handler
                            .update_status(workflow_id, JobStatus::FAILED)
                            .await
                        {
                            warn!(
                                "Failed to update workflow {} status to Error: {}",
                                workflow_id_str, update_err
                            );
                        }
                        if let Some(failed_job) = manager0.unregister_job(&job_id).await {
                            failed_job.set_status(JobStatus::FAILED).await;
                        }
                    }
                }
            }
        }
    }

    /// Register a workflow job in the registry
    pub async fn register_job(&self, job: Arc<WorkflowNodeJob>) {
        let workflow_id = job.workflow_id();
        if self.job_registry.contains_key(&workflow_id) {
            debug!("Workflow {} is already registered", workflow_id);
            return;
        }
        self.job_registry.insert(workflow_id.clone(), job);
        debug!("Registered workflow {} job", workflow_id);
    }

    /// Unregister a workflow job from the registry
    pub async fn unregister_job(&self, workflow_id: &str) -> Option<Arc<WorkflowNodeJob>> {
        self.job_registry.remove(workflow_id)
    }

    /// Get a workflow job by ID
    pub async fn get_node_job(&self, workflow_id: &str) -> Option<Arc<WorkflowNodeJob>> {
        self.job_registry.get(workflow_id)
    }

    /// Get workflow jobs by status
    pub async fn get_node_jobs(&self, status: JobStatus) -> Vec<Arc<WorkflowNodeJob>> {
        let mut result = Vec::new();
        for job in self.job_registry.iter() {
            if job.get_status().await == status {
                result.push(job);
            }
        }
        result
    }

    /// Shutdown the workflow manager: stop cron job first, then stop all workflow jobs
    pub async fn shutdown(&self) {
        info!(
            "Closing workflow manager with cron '{}', channels '{}'",
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

        // Then, stop all running workflow jobs
        for job in self.job_registry.iter() {
            job.stop().await;
        }
        self.job_registry.clear();
        info!("Stopped all workflow jobs.");

        info!(
            "Closed workflow manager with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
    }

    /// Shutdown the global singleton instance
    pub async fn shutdown_global() {
        if let Some(manager) = SINGLETON_INSTANCE.write().await.take() {
            manager.shutdown().await;
            info!("Shutdown Workflow Manager.");
        }
    }
}

pub struct WorkflowNodeJob {
    workflow_info: Arc<WorkflowInfo>,
    cancel: Option<Arc<dyn Fn() + Send + Sync>>,
    status: Arc<RwLock<JobStatus>>,
}

impl WorkflowNodeJob {
    pub fn new(workflow_info: Arc<WorkflowInfo>) -> Self {
        Self {
            workflow_info,
            cancel: None,
            status: Arc::new(RwLock::new(JobStatus::PENDING)),
        }
    }

    pub fn workflow_id(&self) -> String {
        self.workflow_info
            .base
            .id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub fn workflow_info(&self) -> &Arc<WorkflowInfo> {
        &self.workflow_info
    }

    pub fn set_cancel(&mut self, cancel: Arc<dyn Fn() + Send + Sync>) {
        self.cancel = Some(cancel);
    }

    pub async fn set_status(&self, status: JobStatus) {
        *self.status.write().await = status;
    }

    pub async fn get_status(&self) -> JobStatus {
        self.status.read().await.to_owned()
    }

    pub async fn start(&self) -> Result<(), Error> {
        let workflow_id = self.workflow_id();
        info!("Starting workflow job: {}", workflow_id);
        self.set_status(JobStatus::STARTING).await;
        // The actual execution logic is handled by the callback in WorkflowManager
        self.set_status(JobStatus::RUNNING).await;
        Ok(())
    }

    pub async fn stop(&self) {
        let workflow_id = self.workflow_id();
        info!("Stopping workflow job: {}", workflow_id);
        self.set_status(JobStatus::STOPPING).await;
        if let Some(cancel) = &self.cancel {
            cancel();
        }
        self.set_status(JobStatus::STOPPED).await;
    }
}

#[cfg(test)]
mod tests {}
