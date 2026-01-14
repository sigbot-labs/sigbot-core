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
// This includes derived works.

use crate::manager::logmanager_factory::ISigbotLogManager;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, error, info, warn};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::messager::TOPIC_WF_LOG;
use sigbot_types::sys::log::{AppendLogRequest, LogManagerArgument, LogMgrProvider, WorkflowLogEntry};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SigbotDefaultLogManager {
    state: Arc<Mutex<Option<Arc<SigbotState>>>>,
    messager: Arc<Mutex<Option<Arc<dyn ISigbotMessagerClient + Send + Sync>>>>,
}

impl SigbotDefaultLogManager {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(None)),
            messager: Arc::new(Mutex::new(None)),
        })
    }

    /// Start log archiving subscription using shared subscription ($share/group_name/topic)
    /// This ensures no log loss even if the service restarts
    async fn start_archiving0(&self) -> Result<(), Error> {
        info!("Starting log archiving subscription.");

        let state_guard = self.state.lock().await;
        let state = state_guard
            .as_ref()
            .context("SigbotState not initialized. Please call init() first.")?
            .clone();
        drop(state_guard);

        let messager_guard = self.messager.lock().await;
        let messager = messager_guard
            .as_ref()
            .context("Messager client not initialized. Please call init() first.")?
            .clone();
        drop(messager_guard);

        // Create handler for log archiving
        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let state = state.clone();
            Box::pin(async move {
                // Parse log entry from message
                let log_entry: WorkflowLogEntry = match serde_json::from_slice(&data) {
                    Ok(entry) => entry,
                    Err(e) => {
                        warn!("Failed to deserialize WorkflowLogEntry: {}", e);
                        return Err(Error::msg(format!("Failed to deserialize WorkflowLogEntry: {}", e)));
                    }
                };

                debug!(
                    "Received log entry for archiving: workflow_id={}, node_id={:?}, log_count={}",
                    log_entry.workflow_id,
                    log_entry.node_id,
                    log_entry.content.len()
                );

                // Process each log line
                for log_line in &log_entry.content {
                    if log_line.trim().is_empty() {
                        continue;
                    }

                    // Convert log line to AppendLogRequest
                    let append_request =
                        Self::create_append_request(&log_entry.workflow_id, log_entry.node_id.as_deref(), log_line);

                    // Append to database using log_repo directly (same as LogHandler does)
                    let repo = state.log_repo.lock().await;
                    match repo.get(&state.config.appdb).append(append_request.to_log()).await {
                        Ok(id) => {
                            debug!(
                                "Archived log entry: workflow_id={}, node_id={:?}, log_id={}",
                                log_entry.workflow_id, log_entry.node_id, id
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to archive log entry: workflow_id={}, node_id={:?}, error={}",
                                log_entry.workflow_id, log_entry.node_id, e
                            );
                            // Continue processing other log lines even if one fails
                        }
                    }
                }

                Ok("OK".to_string())
            })
        });

        // Use shared subscription format: $share/group_name/topic
        // This ensures load balancing and no message loss
        // Replace placeholders with MQTT wildcards: + for single level, # for multi-level
        let base_topic = TOPIC_WF_LOG
            .replace("{TENANT_ID}", "+")
            .replace("{WORKFLOW_ID}", "+")
            .replace("{NODE_ID}", "+");
        let shared_topic = format!("$share/log-archiving/{}", base_topic);

        messager
            .subscribe(&shared_topic, handler)
            .await
            .context("Failed to subscribe to log archiving topic.")?;

        info!("Subscribed to log archiving topic: {}", shared_topic);
        Ok(())
    }

    /// Create AppendLogRequest from log entry data
    fn create_append_request(workflow_id: &str, node_id: Option<&str>, log_content: &str) -> AppendLogRequest {
        // Extract log level from content (if present)
        let level = Self::extract_log_level(log_content);

        // Build tags: workflow_id and optionally node_id
        let mut tags = vec![format!("workflow_id:{}", workflow_id)];
        if let Some(nid) = node_id {
            tags.push(format!("node_id:{}", nid));
        }
        let tags_str = Some(tags.join(","));

        AppendLogRequest {
            service_name: Some("workflow".to_string()),
            log_type: Some("WORKFLOW".to_string()),
            level,
            content: log_content.to_string(),
            source: node_id.map(|n| format!("workflow:{}/node:{}", workflow_id, n)),
            tags: tags_str,
        }
    }

    /// Extract log level from log content
    /// Looks for common log level patterns: DEBUG, INFO, WARN, ERROR, FATAL
    fn extract_log_level(content: &str) -> Option<String> {
        let content_upper = content.to_uppercase();

        // Check for log level keywords (case-insensitive)
        if content_upper.contains(" FATAL ") || content_upper.starts_with("FATAL") {
            Some("FATAL".to_string())
        } else if content_upper.contains(" ERROR ") || content_upper.starts_with("ERROR") {
            Some("ERROR".to_string())
        } else if content_upper.contains(" WARN ") || content_upper.starts_with("WARN") {
            Some("WARN".to_string())
        } else if content_upper.contains(" INFO ") || content_upper.starts_with("INFO") {
            Some("INFO".to_string())
        } else if content_upper.contains(" DEBUG ") || content_upper.starts_with("DEBUG") {
            Some("DEBUG".to_string())
        } else {
            // Default to INFO if no level found
            Some("INFO".to_string())
        }
    }
}

#[async_trait]
impl ISigbotLogManager for SigbotDefaultLogManager {
    fn provider(&self) -> LogMgrProvider {
        LogMgrProvider::DEFAULT
    }

    async fn init(&self, _argument: Arc<LogManagerArgument>) {
        info!("Initializing Default Log Manager.");

        // Initialize SigbotState
        let config = get_config();
        let app_config = Arc::new(config.clone());
        let state = Arc::new(SigbotState::new(&app_config).await);
        *self.state.lock().await = Some(state);

        // Initialize Messager client from argument
        // Note: Messager client should be initialized separately via SigbotMessagerClientFactory
        // We'll store it when start_archiving is called
        info!("Default Log Manager initialized.");
    }

    async fn close(&self) {
        info!("Closing Default Log Manager.");
        *self.state.lock().await = None;
        *self.messager.lock().await = None;
        info!("Default Log Manager closed.");
    }

    async fn start_archiving(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) -> Result<(), Error> {
        // Store messager client
        *self.messager.lock().await = Some(messager);
        // Start archiving
        self.start_archiving0().await
    }
}
