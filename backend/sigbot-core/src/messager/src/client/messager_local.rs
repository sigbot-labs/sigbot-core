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

use crate::client::messager_factory::ISigbotMessagerClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_types::modules::messager::messager::{MessagerConfiguration, MessagerProvider};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::{sync::Mutex, task};

#[derive(Clone)]
pub struct SigbotLocalMessagerClientConfig {
    pub queue_size: usize,
}

impl std::fmt::Display for SigbotLocalMessagerClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(queue_size={:?})", self.queue_size)
    }
}

impl SigbotLocalMessagerClientConfig {
    pub fn from_config(configuration: Arc<MessagerConfiguration>) -> Self {
        Self {
            queue_size: configuration
                .properties
                .as_ref()
                .expect("Queue size is required")
                .get("queue_size")
                .expect("Queue size is required")
                .parse::<usize>()
                .expect("Queue size must be a valid number"),
        }
    }
}

pub struct SigbotLocalMessagerClient {
    config: Arc<SigbotLocalMessagerClientConfig>,
    // Per topic blocking queue (similar to Java BlockingQueue)
    // Store (sender, receiver) pairs
    topic_queues: Arc<
        Mutex<
            HashMap<
                String,
                (
                    tokio::sync::mpsc::Sender<Vec<u8>>,
                    Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>>,
                ),
            >,
        >,
    >,
    // Store subscribed topics with their handlers and task handles
    subscription_registrations: Arc<
        Mutex<
            HashMap<
                String,
                (
                    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>,
                    task::JoinHandle<()>,
                ),
            >,
        >,
    >,
}

impl SigbotLocalMessagerClient {
    pub async fn new(config: Arc<SigbotLocalMessagerClientConfig>) -> Arc<Self> {
        Arc::new(Self {
            config,
            topic_queues: Arc::new(Mutex::new(HashMap::new())),
            subscription_registrations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // Get or create a queue for a given topic, return sender
    async fn get_or_create_queue_sender(&self, topic: &str) -> tokio::sync::mpsc::Sender<Vec<u8>> {
        let mut queues = self.topic_queues.lock().await;
        if let Some((sender, _)) = queues.get(topic) {
            return sender.clone();
        }

        // Create a new queue, with capacity configured in queue_size
        let (sender, receiver) = tokio::sync::mpsc::channel(self.config.queue_size);
        queues.insert(
            topic.to_string(),
            (sender.clone(), Arc::new(tokio::sync::Mutex::new(receiver))),
        );
        sender
    }

    // Get receiver for a given topic
    async fn get_queue_receiver(
        &self,
        topic: &str,
    ) -> Option<Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>>> {
        let queues = self.topic_queues.lock().await;
        queues.get(topic).map(|(_, receiver)| receiver.clone())
    }
}

#[async_trait]
impl ISigbotMessagerClient for SigbotLocalMessagerClient {
    fn provider(&self) -> MessagerProvider {
        MessagerProvider::LOCAL
    }

    async fn init(&self) {
        info!("Initializing Local Queue messager with config={}", self.config);
        // Local queue does not require additional initialization, queues will be created on first use
        info!("Initialized Local Queue messager with config={}", self.config);
    }

    async fn close(&self) {
        info!("Closing Local Queue messager with {}", self.config);

        // Cancel all subscription tasks and clear registrations
        let mut registrations = self.subscription_registrations.lock().await;
        for (topic, (_, task_handle)) in registrations.drain() {
            debug!("Cancelling subscription task for topic={}", topic);
            task_handle.abort();
        }
        drop(registrations);

        // Clear all queues
        let mut queues = self.topic_queues.lock().await;
        queues.clear();
        drop(queues);

        info!("Closed Local Queue messager with {}", self.config);
    }

    async fn publish(&self, topic: &str, message: &str) -> Result<String, Error> {
        debug!(
            "Publishing Local Queue message to topic={} with message={}",
            topic, message
        );

        // Get or create queue for the given topic
        let sender = self.get_or_create_queue_sender(topic).await;

        // Send message to queue (blocking until space is available)
        sender.send(message.as_bytes().to_vec()).await.context(format!(
            "Failed to publish Local Queue message to topic={} (queue may be closed)",
            topic
        ))?;

        info!("Published Local Queue message to topic={}", topic);
        Ok(message.to_string())
    }

    async fn subscribe(
        &self,
        topic: &str,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error> {
        let store_key = topic.to_string();

        // Check if already subscribed to the given topic
        let registrations = self.subscription_registrations.lock().await;
        if registrations.contains_key(&store_key) {
            debug!("Topic {} already subscribed, skipping", topic);
            return Ok(());
        }

        // Get or create queue for the given topic
        let _sender = self.get_or_create_queue_sender(topic).await;
        drop(registrations);

        // Get receiver
        let receiver = self
            .get_queue_receiver(topic)
            .await
            .expect("Queue should exist after get_or_create_queue_sender");

        // Start message processing task
        let handler0 = handler.to_owned();
        let topic0 = topic.to_owned();
        let task = task::spawn(async move {
            let mut guard = receiver.lock().await;
            loop {
                match guard.recv().await {
                    Some(data) => {
                        debug!("Received Local Queue message for topic={}, size={}", topic0, data.len());
                        let handler_fn = handler0.to_owned();
                        tokio::spawn(async move {
                            if let Err(e) = handler_fn(data).await {
                                warn!("Error handling Local Queue message: {:?}", e);
                            }
                        });
                    }
                    None => {
                        // Sender closed, exit loop
                        debug!("Receiver closed for topic={}, stopping subscription task", topic0);
                        break;
                    }
                }
            }
        });

        // Save subscription registration and task handle
        let mut registrations = self.subscription_registrations.lock().await;
        registrations.insert(store_key, (handler, task));

        info!("Subscribed to Local Queue topic={}", topic);
        Ok(())
    }
}
