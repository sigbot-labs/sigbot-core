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
use common_telemetry::{debug, info};
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

/// Kafka producer with broker delivery acknowledgement and bounded buffering.
pub struct KafkaWriter {
    sink: Arc<dyn super::sink::IExportSink>,
}

impl KafkaWriter {
    pub fn new(config: KafkaWriterConfig) -> Result<Self, Error> {
        let values = config.properties.unwrap_or_default();
        let secret_env = values
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("kafka_secret_env.")
                    .map(|key| (key.to_owned(), value.clone()))
            })
            .collect();
        let properties = values
            .into_iter()
            .filter_map(|(key, value)| key.strip_prefix("kafka_property.").map(|key| (key.to_owned(), value)))
            .collect();
        Ok(Self {
            sink: super::sink::ExportSinkFactory::create(&super::sink::ExportSinkConfig::Kafka {
                brokers: config.brokers.join(","),
                topic: config.topic.ok_or_else(|| Error::msg("kafka_topic required"))?,
                properties,
                secret_env,
            })?,
        })
    }

    async fn write_to_kafka(&self, data: Vec<u8>) -> Result<(), Error> {
        let event: SigbotTradeEvent = serde_json::from_slice(&data)?;
        self.sink
            .write(&super::sink::ExportBatch {
                key: event.idempotency_key(),
                range: String::new(),
                rows: vec![vec![serde_json::to_value(event)?]],
            })
            .await
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
            return Err(Error::msg("Kafka writer not configured"));
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

        let writer = KafkaWriter::new(config).expect("invalid Kafka exporter configuration");
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
                manager0.export_data_internal(data.clone()).await?;
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
