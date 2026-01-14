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

use crate::manager::exporter_factory::ISigbotExporterManager;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    exporter::{ExporterMgrProvider, SigbotExporterManagerArgument},
    messager::TOPIC_WF_TRADING_PLACED,
    order::events::SigbotTradeEvent,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Kafka writer configuration
#[derive(Clone, Debug)]
pub struct KafkaWriterConfig {
    pub brokers: Vec<String>,
    pub topic: Option<String>,
    pub properties: Option<HashMap<String, String>>,
}

/// Kafka writer (placeholder for future implementation)
pub struct KafkaWriter {
    config: KafkaWriterConfig,
}

impl KafkaWriter {
    pub fn new(config: KafkaWriterConfig) -> Self {
        Self { config }
    }

    async fn write_to_kafka(&self, data: Vec<u8>) -> Result<(), Error> {
        // TODO: Implement Kafka producer integration
        // For now, just log the data
        let text = String::from_utf8_lossy(&data);
        info!(
            "[KafkaWriter] Would write to brokers={:?}, topic={:?}: {}",
            self.config.brokers, self.config.topic, text
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct SigbotKafkaExporterManager {
    writer: Arc<Mutex<Option<KafkaWriter>>>,
    messager_client: Arc<Mutex<Option<Arc<dyn ISigbotMessagerClient>>>>,
}

impl SigbotKafkaExporterManager {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            writer: Arc::new(Mutex::new(None)),
            messager_client: Arc::new(Mutex::new(None)),
        })
    }

    async fn export_data_internal(&self, data: Vec<u8>) -> Result<(), Error> {
        // Parse and validate SigbotTradeEvent
        let trade_event: SigbotTradeEvent = serde_json::from_slice(&data)
            .map_err(|e| Error::msg(format!("Failed to deserialize SigbotTradeEvent: {}", e)))?;

        debug!(
            "Kafka exporter processing trade_event: trade_id={}, tenant_id={}",
            trade_event.trade_id, trade_event.tenant_id
        );

        // Write to Kafka (stream mode)
        let writer_guard = self.writer.lock().await;
        if let Some(ref writer) = *writer_guard {
            writer.write_to_kafka(data).await?;
        } else {
            warn!("Kafka writer not configured, skipping export");
        }

        Ok(())
    }
}

#[async_trait]
impl ISigbotExporterManager for SigbotKafkaExporterManager {
    fn provider(&self) -> ExporterMgrProvider {
        ExporterMgrProvider::KAFKA
    }

    async fn init(&self, argument: Arc<SigbotExporterManagerArgument>) {
        debug!("Initializing Kafka Exporter manager");

        // Parse Kafka configuration from argument
        let brokers_str = argument
            .properties
            .as_ref()
            .and_then(|p| p.get("kafka_brokers").cloned())
            .unwrap_or_else(|| "localhost:9092".to_string());
        let brokers: Vec<String> = brokers_str.split(',').map(|s| s.trim().to_string()).collect();

        let config = KafkaWriterConfig {
            brokers,
            topic: argument.properties.as_ref().and_then(|p| p.get("kafka_topic").cloned()),
            properties: argument.properties.clone(),
        };

        let writer = KafkaWriter::new(config);
        *self.writer.lock().await = Some(writer);

        info!("Kafka Exporter manager initialized");
    }

    async fn close(&self) {
        info!("Shutting down Kafka Exporter manager");
    }

    async fn subscribe(&self, messager: Arc<dyn ISigbotMessagerClient>) -> Result<(), Error> {
        // Store messager client
        *self.messager_client.lock().await = Some(messager.clone());

        // Subscribe to trading placed events (orders) - this exporter handles order data
        let topic = TOPIC_WF_TRADING_PLACED
            .replace("{TENANT_ID}", "+")
            .replace("{WORKFLOW_ID}", "+");

        let manager = self.clone();
        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let manager0 = manager.clone();
            Box::pin(async move {
                // Export to Kafka
                if let Err(e) = manager0.export_data_internal(data.clone()).await {
                    warn!("Failed to export to Kafka: {}", e);
                }
                Ok(String::from_utf8_lossy(&data).to_string())
            })
        });

        messager.subscribe(&topic, handler).await?;
        info!("Kafka Exporter subscribed to topic: {}", topic);

        Ok(())
    }

    async fn export_batch(&self, data: Vec<u8>) -> Result<(), Error> {
        self.export_data_internal(data).await
    }

    async fn export_stream(&self, data: Vec<u8>) -> Result<(), Error> {
        self.export_data_internal(data).await
    }
}

#[cfg(test)]
mod tests {}
