// SPDX-License-Identifier: GPL-3.0-only
use super::messager_factory::ISigbotMessagerClient;
use crate::storage::postgres::PostgresMessageStore;
use common_telemetry::tracing_context::MessageTrace;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

/// A scoped host task, never an independently deployed service.
pub struct OutboxRelay {
    task: JoinHandle<()>,
}

impl OutboxRelay {
    pub fn start(store: PostgresMessageStore, producer: String, client: Arc<dyn ISigbotMessagerClient>) -> Self {
        let readiness = common_telemetry::readiness::RuntimeReadiness::task(format!("outbox:{producer}"));
        let task = tokio::spawn(async move {
            readiness.set(true);
            loop {
                let result = store
                    .dispatch_pending(&producer, client.as_ref(), 32, &|parent| {
                        Ok(MessageTrace::child("messager.retry", parent).traceparent())
                    })
                    .await;
                if let Err(error) = result {
                    common_telemetry::warn!("outbox relay will retry after durable failure: {}", error);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        Self { task }
    }

    pub fn close(&self) {
        self.task.abort();
    }
}

impl Drop for OutboxRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
