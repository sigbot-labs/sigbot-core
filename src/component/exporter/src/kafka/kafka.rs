// SPDX-License-Identifier: GPL-3.0-only

use crate::exporter::ExportBatch;
use crate::exporter::IExporter;
use anyhow::ensure;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::producer::Producer;
use sigbot_core::config::config::get_config;
use sigbot_types::exporter::ExporterProvider;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub struct KafkaExporter {
    producer: FutureProducer,
    topic: String,
}
impl KafkaExporter {
    pub fn new(
        brokers: &str,
        topic: &str,
        properties: &BTreeMap<String, String>,
        secret_keys: &BTreeMap<String, String>,
    ) -> Result<Arc<Self>> {
        ensure!(!brokers.is_empty() && !topic.is_empty(), "Kafka brokers/topic required");
        let mut config = ClientConfig::new();
        for (key, value) in properties {
            ensure!(
                !key.contains("password") && !key.ends_with(".pem") && key != "sasl.oauthbearer.config",
                "Kafka credentials must use configured secret keys"
            );
            config.set(key, value);
        }
        ensure!(secret_keys.len() <= 16, "Kafka secret references exceed bounds");
        let app_config = get_config();
        for (key, name) in secret_keys {
            config.set(key, app_config.secrets.require(name)?);
        }
        config
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "30000")
            .set("queue.buffering.max.messages", "1000")
            .set("queue.buffering.max.kbytes", "4096")
            .set("max.in.flight.requests.per.connection", "5");
        Ok(Arc::new(Self {
            producer: config
                .create()
                .map_err(|_| anyhow::anyhow!("invalid Kafka producer configuration"))?,
            topic: topic.to_owned(),
        }))
    }
}
#[async_trait]
impl IExporter for KafkaExporter {
    fn provider(&self) -> ExporterProvider {
        ExporterProvider::Kafka
    }
    async fn write(&self, batch: &ExportBatch) -> Result<()> {
        batch.validate()?;
        let bytes = serde_json::to_vec(batch)?;
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(&batch.key).payload(&bytes),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(error, _)| anyhow::anyhow!("Kafka delivery failed: {error}"))?;
        Ok(())
    }
}
#[async_trait]
impl IProvider for KafkaExporter {
    async fn close(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || producer.flush(Duration::from_secs(5))).await??;
        Ok(())
    }
}
