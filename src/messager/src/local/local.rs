// SPDX-License-Identifier: GPL-3.0-only

use crate::messager::IMessager;
use anyhow::Context;
use anyhow::Error;
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::debug;
use common_telemetry::info;
use common_telemetry::warn;
use sigbot_types::messager::messager::MessagerConfig;
use sigbot_types::messager::messager::MessagerProvider;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;

type LocalMessage = Vec<u8>;
type LocalQueue = (
    tokio::sync::mpsc::Sender<LocalMessage>,
    Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<LocalMessage>>>,
);
type LocalHandler =
    Arc<dyn Fn(LocalMessage) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>;
type LocalSubscription = (Arc<Mutex<HashMap<String, LocalHandler>>>, task::JoinHandle<()>);

// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

#[derive(Clone)]
pub struct LocalMessagerConfig {
    pub queue_size: usize,
}

impl std::fmt::Display for LocalMessagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(queue_size={:?})", self.queue_size)
    }
}

impl LocalMessagerConfig {
    pub fn from_config(configuration: Arc<MessagerConfig>) -> Self {
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

pub struct LocalMessager {
    config: Arc<LocalMessagerConfig>,
    topic_queues: Arc<Mutex<HashMap<String, LocalQueue>>>,
    subscription_registrations: Arc<Mutex<HashMap<String, LocalSubscription>>>,
}

impl LocalMessager {
    pub async fn new(config: Arc<LocalMessagerConfig>) -> Arc<Self> {
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
impl IMessager for LocalMessager {
    fn provider(&self) -> MessagerProvider {
        MessagerProvider::Local
    }

    async fn publish(&self, topic: &str, message: &str) -> Result<String, Error> {
        debug!(
            "Publishing Local Queue message to topic={}, bytes={}",
            topic,
            message.len()
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
        static NEXT_CONSUMER: AtomicU64 = AtomicU64::new(1);
        let consumer = format!("anonymous:{}", NEXT_CONSUMER.fetch_add(1, Ordering::Relaxed));
        self.subscribe_scoped(topic, &consumer, handler).await
    }

    async fn subscribe_scoped(&self, topic: &str, consumer: &str, handler: LocalHandler) -> Result<(), Error> {
        let store_key = topic.to_string();
        anyhow::ensure!(
            !consumer.is_empty() && consumer.len() <= 256,
            "invalid consumer identity"
        );

        // Check if already subscribed to the given topic
        let registrations = self.subscription_registrations.lock().await;
        if let Some((handlers, _)) = registrations.get(&store_key) {
            let mut handlers = handlers.lock().await;
            anyhow::ensure!(!handlers.contains_key(consumer), "consumer already subscribed to topic");
            handlers.insert(consumer.to_owned(), handler);
            debug!("Added Local Queue consumer for topic={}", topic);
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
        let handlers = Arc::new(Mutex::new(HashMap::from([(consumer.to_owned(), handler)])));
        let task_handlers = handlers.clone();
        let topic0 = topic.to_owned();
        let task = task::spawn(async move {
            let mut guard = receiver.lock().await;
            loop {
                match guard.recv().await {
                    Some(data) => {
                        debug!("Received Local Queue message for topic={}, size={}", topic0, data.len());
                        let consumers = task_handlers.lock().await.values().cloned().collect::<Vec<_>>();
                        for consumer in consumers {
                            let mut delay = std::time::Duration::from_millis(100);
                            loop {
                                match consumer(data.clone()).await {
                                    Ok(_) => break,
                                    Err(error) => {
                                        warn!("Local consumer failed; applying serial retry/backpressure: {}", error);
                                        tokio::time::sleep(delay).await;
                                        delay = (delay * 2).min(std::time::Duration::from_secs(30));
                                    }
                                }
                            }
                        }
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
        registrations.insert(store_key, (handlers, task));

        info!("Subscribed to Local Queue topic={}", topic);
        Ok(())
    }

    async fn unsubscribe_scoped(&self, topic: &str, consumer: &str) -> Result<(), Error> {
        let registrations = self.subscription_registrations.lock().await;
        let (handlers, _) = registrations.get(topic).context("topic is not subscribed")?;
        anyhow::ensure!(
            handlers.lock().await.remove(consumer).is_some(),
            "consumer is not subscribed to topic"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl IProvider for LocalMessager {
    async fn close(&self) -> anyhow::Result<()> {
        info!("Closing Local messager with {}", self.config);

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

        info!("Closed Local messager with {}", self.config);

        Ok(())
    }
    async fn init(&self) -> anyhow::Result<()> {
        debug!("Initializing Local messager with config={}", self.config);
        // Local queue does not require additional initialization, queues will be created on first use
        info!("Initialized Local messager with config={}", self.config);

        Ok(())
    }
}
