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

use crate::manager::logmanager_factory::SigbotLogManagerFactory;
use anyhow::Error;
use axum::{
    extract::{ws::WebSocketUpgrade, Query, State},
    response::Response,
    routing::get,
    Router,
};
use clap;
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use sigbot_core::config::config::get_config;
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::messager::TOPIC_WF_LOG;
use sigbot_types::sys::log::{LogStreamMessage, LogSubscribeMessage, WorkflowLogEntry};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Shared state for WebSocket connections
#[derive(Clone)]
pub struct LogServiceState {
    /// Map from workflow_id to broadcast channel sender
    pub subscribers: Arc<DashMap<String, broadcast::Sender<LogStreamMessage>>>,
}

impl LogServiceState {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(DashMap::new()),
        }
    }

    /// Subscribe to a workflow's logs
    pub fn subscribe(&self, workflow_id: String) -> broadcast::Receiver<LogStreamMessage> {
        let entry = self
            .subscribers
            .entry(workflow_id.clone())
            .or_insert_with(|| broadcast::channel(1000).0);
        entry.subscribe()
    }

    /// Unsubscribe from a workflow's logs
    pub fn unsubscribe(&self, workflow_id: &str) {
        self.subscribers.remove(workflow_id);
    }

    /// Broadcast log message to all subscribers of a workflow
    pub fn broadcast(&self, workflow_id: &str, message: LogStreamMessage) {
        if let Some(sender) = self.subscribers.get(workflow_id) {
            let _ = sender.send(message);
        }
    }
}

pub struct SigbotLogServer {}

impl SigbotLogServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    #[allow(unused_variables)]
    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        debug!("Initializing Log Service.");

        // Initialize Log Manager via factory
        debug!("Initializing Log Manager.");
        let (log_manager, argument) = SigbotLogManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Log Manager.");
        info!("Initialized Log Manager. {}", log_manager.provider().as_str());

        // Initialize Messager client
        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        // 1. Start log archiving to database.
        let messager0 = messager.clone();
        let log_manager0 = log_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = log_manager0.start_archiving(messager0).await {
                error!("Failed to start log archiving: {}", e);
            }
        });

        // 2. Create shared state for real-time WebSocket push.
        let log_state = LogServiceState::new();
        let log_state0 = log_state.clone();
        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let state = log_state0.clone();
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
                    "Received log entry for real-time push: workflow_id={}, node_id={:?}, log_count={}",
                    log_entry.workflow_id,
                    log_entry.node_id,
                    log_entry.content.len()
                );

                // Broadcast to WebSocket subscribers
                let log_message = LogStreamMessage {
                    workflow_id: log_entry.workflow_id.clone(),
                    node_id: log_entry.node_id.clone(),
                    content: log_entry.content.clone(),
                };
                state.broadcast(&log_entry.workflow_id, log_message);

                Ok("OK".to_string())
            })
        });

        // Subscribe to log topics for real-time push (non-shared subscription)
        let topic = TOPIC_WF_LOG
            .replace("{TENANT_ID}", "+")
            .replace("{WORKFLOW_ID}", "+")
            .replace("{NODE_ID}", "+");
        messager
            .subscribe(&topic, handler)
            .await
            .expect("Failed to subscribe to log topic for real-time push.");
        info!("Subscribed to log topic for real-time push: {}", topic);

        // Start HTTP server with WebSocket support
        let config = get_config();
        let bind_addr = format!("{}:{}", config.server.host, config.server.port);
        info!("Starting Log Service HTTP server on {}", &bind_addr);

        let app = Router::new()
            .route("/ws/logs", get(websocket_handler))
            .with_state(log_state);

        let bind_addr0 = bind_addr.clone();
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(bind_addr0.as_str())
                .await
                .expect("Failed to bind address");
            axum::serve(listener, app)
                .await
                .unwrap_or_else(|e| panic!("Error starting Log Service HTTP server: {}", e));
        });

        info!("Log Service HTTP server started on {}", &bind_addr);
    }

    pub async fn shutdown() {
        info!("Shutting down Log Manager.");
        SigbotLogManagerFactory::close().await;
        info!("Shutdown Log Manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutdown Messager client.");
    }
}

/// Query parameters for WebSocket connection
#[derive(Deserialize)]
struct WsQuery {
    workflow_id: Option<String>,
}

/// WebSocket handler
async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsQuery>,
    State(state): State<LogServiceState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, params.workflow_id, state))
}

/// Handle WebSocket connection
async fn handle_socket(socket: axum::extract::ws::WebSocket, workflow_id: Option<String>, state: LogServiceState) {
    let (mut sender, mut receiver) = socket.split();

    // Channel to communicate subscription changes between tasks
    let (tx_subscribe, mut rx_subscribe) =
        tokio::sync::mpsc::unbounded_channel::<Option<broadcast::Receiver<LogStreamMessage>>>();

    // If workflow_id is provided in query, subscribe immediately
    if let Some(wf_id) = workflow_id.clone() {
        info!("WebSocket client connected with workflow_id: {}", wf_id);
        let rx = state.subscribe(wf_id);
        let _ = tx_subscribe.send(Some(rx));
    } else {
        info!("WebSocket client connected without workflow_id");
        let _ = tx_subscribe.send(None);
    }

    let state_clone = state.clone();
    let mut current_workflow_id = workflow_id;

    // Handle incoming messages from client
    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(axum::extract::ws::Message::Text(text)) => {
                    debug!("Received WebSocket message: {}", text);
                    // Parse subscribe/unsubscribe message
                    if let Ok(sub_msg) = serde_json::from_str::<LogSubscribeMessage>(&text) {
                        match sub_msg.action.as_str() {
                            "subscribe" => {
                                info!("Client subscribing to workflow_id: {}", sub_msg.workflow_id);
                                let rx = state_clone.subscribe(sub_msg.workflow_id.clone());
                                let _ = tx_subscribe.send(Some(rx));
                                current_workflow_id = Some(sub_msg.workflow_id);
                            }
                            "unsubscribe" => {
                                info!("Client unsubscribing from workflow_id: {}", sub_msg.workflow_id);
                                state_clone.unsubscribe(&sub_msg.workflow_id);
                                let _ = tx_subscribe.send(None);
                                current_workflow_id = None;
                            }
                            _ => {
                                warn!("Unknown action: {}", sub_msg.action);
                            }
                        }
                    }
                }
                Ok(axum::extract::ws::Message::Close(_)) => {
                    info!("WebSocket client disconnected");
                    if let Some(wf_id) = current_workflow_id {
                        state_clone.unsubscribe(&wf_id);
                    }
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Handle outgoing messages to client
    let mut send_task = tokio::spawn(async move {
        let mut rx: Option<broadcast::Receiver<LogStreamMessage>> = None;

        loop {
            tokio::select! {
                // Check for new subscription
                new_rx = rx_subscribe.recv() => {
                    match new_rx {
                        Some(Some(new_rx)) => {
                            rx = Some(new_rx);
                        }
                        Some(None) => {
                            rx = None;
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }
                // Receive log messages from current subscription
                result = async {
                    if let Some(ref mut rx) = rx {
                        rx.recv().await.ok()
                    } else {
                        // Wait a bit if no subscription
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        None
                    }
                } => {
                    if let Some(log_msg) = result {
                        let json = match serde_json::to_string(&log_msg) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("Failed to serialize log message: {}", e);
                                continue;
                            }
                        };
                        if sender.send(axum::extract::ws::Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = &mut recv_task => {
            send_task.abort();
        }
        _ = &mut send_task => {
            recv_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {}
