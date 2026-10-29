// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use google_sheets4::api::ValueRange;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(Clone, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExportSinkConfig {
    Kafka {
        brokers: String,
        topic: String,
        properties: BTreeMap<String, String>,
        #[serde(default)]
        secret_env: BTreeMap<String, String>,
    },
    GoogleSheets {
        spreadsheet_id: String,
        token_env: String,
    },
}

pub use sigbot_types::modules::exporter::request::ExportBatch;

#[async_trait]
pub trait IExportSink: Send + Sync {
    async fn write(&self, batch: &ExportBatch) -> Result<()>;
}

pub struct ExportSinkFactory;
impl ExportSinkFactory {
    pub fn create(config: &ExportSinkConfig) -> Result<Arc<dyn IExportSink>> {
        match config {
            ExportSinkConfig::Kafka {
                brokers,
                topic,
                properties,
                secret_env,
            } => {
                ensure!(!brokers.is_empty() && !topic.is_empty(), "Kafka brokers/topic required");
                let mut config = ClientConfig::new();
                for (key, value) in properties {
                    ensure!(
                        !key.contains("password") && !key.ends_with(".pem") && key != "sasl.oauthbearer.config",
                        "Kafka credentials must use secret_env references"
                    );
                    config.set(key, value);
                }
                ensure!(secret_env.len() <= 16, "Kafka secret references exceed bounds");
                for (key, name) in secret_env {
                    ensure!(
                        !name.is_empty()
                            && name.len() <= 128
                            && name
                                .bytes()
                                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_'),
                        "invalid Kafka secret environment reference"
                    );
                    config.set(key, std::env::var(name).context("Kafka host credential missing")?);
                }
                config
                    .set("bootstrap.servers", brokers)
                    .set("enable.idempotence", "true")
                    .set("acks", "all")
                    .set("message.timeout.ms", "30000")
                    .set("queue.buffering.max.messages", "1000")
                    .set("queue.buffering.max.kbytes", "4096")
                    .set("max.in.flight.requests.per.connection", "5");
                Ok(Arc::new(KafkaSink {
                    producer: config
                        .create()
                        .map_err(|_| anyhow::anyhow!("invalid Kafka producer configuration"))?,
                    topic: topic.clone(),
                }))
            }
            ExportSinkConfig::GoogleSheets {
                spreadsheet_id,
                token_env,
            } => {
                ensure!(
                    !spreadsheet_id.is_empty()
                        && spreadsheet_id
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
                    "invalid spreadsheet ID"
                );
                Ok(Arc::new(GoogleSheetsSink {
                    spreadsheet_id: spreadsheet_id.clone(),
                    token_env: token_env.clone(),
                    client: reqwest::Client::builder()
                        .timeout(Duration::from_secs(30))
                        .redirect(reqwest::redirect::Policy::none())
                        .build()?,
                }))
            }
        }
    }
}

struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}
#[async_trait]
impl IExportSink for KafkaSink {
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

struct GoogleSheetsSink {
    spreadsheet_id: String,
    token_env: String,
    client: reqwest::Client,
}
#[async_trait]
impl IExportSink for GoogleSheetsSink {
    async fn write(&self, batch: &ExportBatch) -> Result<()> {
        batch.validate()?;
        ensure!(
            !batch.range.is_empty() && batch.range.len() <= 256,
            "deterministic Google Sheets range required"
        );
        let body = ValueRange {
            range: Some(batch.range.clone()),
            major_dimension: Some("ROWS".into()),
            values: Some(batch.rows.clone()),
        };
        let mut endpoint = url::Url::parse("https://sheets.googleapis.com/v4/spreadsheets/")?;
        endpoint
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Sheets base URL"))?
            .pop_if_empty()
            .push(&self.spreadsheet_id)
            .push("values")
            .push(&batch.range);
        let response = self
            .client
            .put(endpoint)
            .bearer_auth(std::env::var(&self.token_env).context("Google Sheets OAuth token missing")?)
            .query(&[("valueInputOption", "RAW")])
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        let receipt: Value = response.json().await?;
        ensure!(
            receipt.get("updatedRows").and_then(Value::as_u64) == Some(batch.rows.len() as u64),
            "Sheets returned an incomplete write"
        );
        Ok(())
    }
}
