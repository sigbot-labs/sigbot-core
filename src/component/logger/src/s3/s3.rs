// SPDX-License-Identifier: GPL-3.0-only

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use common_context::IProvider;
use object_store::aws::AmazonS3Builder;
use object_store::aws::S3ConditionalPut;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::PutMode;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::logger::handler::logger::LogHandler;
use sigbot_types::logger::models::{ArchiveRecord, LogChunkIndex};
use sigbot_types::messager::envelope::DurableMessage;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogArchiveConfig {
    pub spool_path: PathBuf,
    pub max_buffer_bytes: i64,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_secret: String,
    pub secret_key_secret: String,
    pub allow_http: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogLine {
    pub level: String,
    pub source: String,
    pub content: String,
}

/// Generated logs retain their assigned sequence across an uncertain append retry.
pub struct LogAppend {
    message: DurableMessage<LogLine>,
    allocated: bool,
}
impl LogAppend {
    pub fn new(mut message: DurableMessage<LogLine>) -> Self {
        message.seq = 0;
        Self {
            message,
            allocated: false,
        }
    }
    pub fn message(&self) -> &DurableMessage<LogLine> {
        &self.message
    }
    /// Resume an unacknowledged local append after its COMMIT outcome was uncertain.
    pub fn from_tentative(message: DurableMessage<LogLine>) -> Self {
        Self {
            message,
            allocated: true,
        }
    }
    pub fn retry(&mut self, traceparent: String, now: chrono::DateTime<chrono::Utc>) -> Result<()> {
        self.message.retry(traceparent, now)
    }
}

pub struct S3Logger {
    handler: LogHandler,
    store: Arc<dyn ObjectStore>,
}

impl S3Logger {
    pub async fn new(config: &LogArchiveConfig, handler: LogHandler) -> Result<S3Logger> {
        ensure!(
            config.max_buffer_bytes >= 262_144,
            "log buffer must hold at least one maximum-size record"
        );
        ensure!(
            config.spool_path.is_absolute(),
            "log buffer must have an absolute persistent-volume path"
        );
        let app_config = get_config();
        let store = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_endpoint(&config.endpoint)
            .with_allow_http(config.allow_http)
            .with_access_key_id(app_config.secrets.require(&config.access_key_secret)?)
            .with_secret_access_key(app_config.secrets.require(&config.secret_key_secret)?)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        Ok(S3Logger {
            handler,
            store: Arc::new(store),
        })
    }
}

impl S3Logger {
    pub async fn append(&self, message: &DurableMessage<LogLine>) -> Result<bool> {
        self.validate(message)?;
        self.handler
            .append(
                Self::record(message)?,
                message.seq.checked_add(1).context("log sequence exhausted")?,
                &Self::stream_key(message),
            )
            .await
    }

    /// Local sequence and full record commit in the same WAL transaction; neither PG
    /// nor S3 participates in this durability boundary. Existing explicit append remains valid.
    pub async fn append_generated(&self, draft: &mut LogAppend) -> Result<bool> {
        let stream = Self::stream_key(&draft.message);
        let tentative_key = draft.allocated.then(|| Self::object_key(&draft.message));
        let tentative_hash = draft.allocated.then(|| Self::fact_hash(&draft.message)).transpose()?;
        let tentative = tentative_key
            .as_deref()
            .zip(tentative_hash.as_deref())
            .map(|(key, hash)| (draft.message.seq, key, hash));
        let base = draft.message.clone();
        let outcome = self
            .handler
            .append_generated(&stream, tentative, move |sequence| {
                let mut message = base;
                message.seq = sequence;
                Self::record(&message)
            })
            .await?;
        draft.message.seq = outcome.sequence;
        draft.allocated = true;
        Ok(outcome.inserted)
    }

    fn validate(&self, message: &DurableMessage<LogLine>) -> Result<()> {
        message.validate()?;
        ensure!(
            message.workflow_id.is_some() && !message.payload.content.is_empty(),
            "workflow log content required"
        );
        ensure!(
            ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"].contains(&message.payload.level.as_str()),
            "invalid log level"
        );
        ensure!(
            serde_json::to_vec(message)?.len() <= 262_144,
            "log record exceeds 256 KiB"
        );
        Ok(())
    }

    fn record(message: &DurableMessage<LogLine>) -> Result<ArchiveRecord> {
        let payload = serde_json::to_vec(message)?;
        Ok(ArchiveRecord {
            object_key: Self::object_key(message),
            fact_sha256: Self::fact_hash(message)?,
            content_sha256: hex::encode(Sha256::digest(&payload)),
            payload,
        })
    }

    pub async fn archive_pending(&self, limit: u32) -> Result<usize> {
        ensure!((1..=128).contains(&limit), "archive batch must be bounded");
        let records = self.handler.pending(limit).await?;
        for record in &records {
            let path = Path::from(record.object_key.as_str());
            match self
                .store
                .put_opts(&path, record.payload.clone().into(), PutMode::Create.into())
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let existing = self.store.get(&path).await?.bytes().await?;
                    ensure!(
                        hex::encode(Sha256::digest(&existing)) == record.content_sha256,
                        "immutable log object checksum conflict"
                    );
                }
                Err(error) => return Err(error.into()),
            }
            let message: DurableMessage<LogLine> = serde_json::from_slice(&record.payload)?;
            self.handler
                .index(LogChunkIndex {
                    message: &message,
                    object_key: &record.object_key,
                    sha256: &record.content_sha256,
                    byte_length: record.payload.len() as i64,
                })
                .await?;
            // Retain only the idempotency tombstone, freeing durable buffer capacity.
            self.handler
                .mark_archived(&record.object_key, &record.content_sha256)
                .await?;
        }
        Ok(records.len())
    }

    pub async fn read(
        &self,
        stream: &DurableMessage<Value>,
        after_cursor: i64,
        limit: u32,
    ) -> Result<Vec<(i64, DurableMessage<LogLine>)>> {
        stream.validate()?;
        ensure!(
            after_cursor >= 0 && (1..=100).contains(&limit),
            "invalid log cursor/limit"
        );
        let rows = self
            .handler
            .read(
                &stream.scope,
                stream.workflow_id,
                stream.node_id,
                &stream.run_id,
                after_cursor,
                limit,
            )
            .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes = self.store.get(&Path::from(row.object_key)).await?.bytes().await?;
            ensure!(
                hex::encode(Sha256::digest(&bytes)) == row.sha256,
                "log chunk integrity failure"
            );
            result.push((row.cursor, serde_json::from_slice(&bytes)?));
        }
        Ok(result)
    }

    fn stream_key(message: &DurableMessage<LogLine>) -> String {
        let key = Self::object_key(message);
        key[..key.rfind('/').expect("archive object always has a stream prefix") + 1].to_owned()
    }

    fn fact_hash(message: &DurableMessage<LogLine>) -> Result<String> {
        let mut fact = serde_json::to_value(message)?;
        for key in ["attempt_no", "traceparent", "produced_at"] {
            fact.as_object_mut().context("log must be an object")?.remove(key);
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&fact)?)))
    }

    fn object_key(message: &DurableMessage<LogLine>) -> String {
        format!(
            "logs/{}/{}/{}/{}/{}/{}.json",
            message.scope.env,
            message.scope.tenant_id,
            message.workflow_id.unwrap_or_default(),
            message.node_id,
            hex::encode(Sha256::digest(message.run_id.as_bytes())),
            message.seq
        )
    }
}

#[async_trait::async_trait]
impl IProvider for S3Logger {
    async fn close(&self) -> Result<()> {
        self.handler.close().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::logger::ILogger for S3Logger {
    async fn append(&self, message: &DurableMessage<LogLine>) -> Result<bool> {
        Self::append(self, message).await
    }

    async fn archive_pending(&self, limit: u32) -> Result<usize> {
        Self::archive_pending(self, limit).await
    }

    async fn read(
        &self,
        stream: &DurableMessage<Value>,
        after_cursor: i64,
        limit: u32,
    ) -> Result<Vec<(i64, DurableMessage<LogLine>)>> {
        Self::read(self, stream, after_cursor, limit).await
    }
}
