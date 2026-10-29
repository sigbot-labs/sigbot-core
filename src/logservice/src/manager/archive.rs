// SPDX-License-Identifier: GPL-3.0-only
//! Shared append/read archive. ACK means SQLite WAL FULL commit on the host volume;
//! archival means conditional S3 create followed by a committed PostgreSQL index.
use anyhow::{ensure, Context, Result};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
    ObjectStore, PutMode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sigbot_types::modules::messager::envelope::DurableMessage;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    PgPool, Row, Sqlite, SqlitePool, Transaction,
};
use std::{path::PathBuf, str::FromStr, sync::Arc};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogArchiveConfig {
    pub spool_path: PathBuf,
    pub max_buffer_bytes: i64,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_env: String,
    pub secret_key_env: String,
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

pub struct LogArchiveFactory;
pub struct LogArchive {
    spool: SqlitePool,
    index: PgPool,
    store: Arc<dyn ObjectStore>,
    max_buffer_bytes: i64,
}

impl LogArchiveFactory {
    pub async fn create(config: &LogArchiveConfig, index: PgPool) -> Result<LogArchive> {
        ensure!(
            config.max_buffer_bytes >= 262_144,
            "log buffer must hold at least one maximum-size record"
        );
        ensure!(
            config.spool_path.is_absolute(),
            "log buffer must have an absolute persistent-volume path"
        );
        if let Some(parent) = config.spool_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new();
        file.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            file.mode(0o600);
            if config.spool_path.exists() {
                ensure!(
                    std::fs::metadata(&config.spool_path)?.permissions().mode() & 0o077 == 0,
                    "log buffer must not be accessible to other users"
                );
            }
        }
        file.open(&config.spool_path)?;
        let store = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_endpoint(&config.endpoint)
            .with_allow_http(config.allow_http)
            .with_access_key_id(std::env::var(&config.access_key_env)?)
            .with_secret_access_key(std::env::var(&config.secret_key_env)?)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", config.spool_path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full);
        let spool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS log_buffer(object_key TEXT PRIMARY KEY,fact_sha256 TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,payload BLOB,archived INTEGER NOT NULL DEFAULT 0)",
        )
        .execute(&spool)
        .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS log_sequence(stream_key TEXT PRIMARY KEY,next_seq INTEGER NOT NULL)")
            .execute(&spool)
            .await?;
        Ok(LogArchive {
            spool,
            index,
            store: Arc::new(store),
            max_buffer_bytes: config.max_buffer_bytes,
        })
    }
}

impl LogArchive {
    pub async fn append(&self, message: &DurableMessage<LogLine>) -> Result<bool> {
        let mut tx = self.spool.begin_with("BEGIN IMMEDIATE").await?;
        let inserted = self.append_in(&mut tx, message).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    /// Local sequence and full record commit in the same WAL transaction; neither PG
    /// nor S3 participates in this durability boundary. Existing explicit append remains valid.
    pub async fn append_generated(&self, draft: &mut LogAppend) -> Result<bool> {
        let mut tx = self.spool.begin_with("BEGIN IMMEDIATE").await?;
        if draft.allocated {
            let existing: Option<String> = sqlx::query_scalar("SELECT fact_sha256 FROM log_buffer WHERE object_key=?")
                .bind(Self::object_key(&draft.message))
                .fetch_optional(&mut *tx)
                .await?;
            if existing.as_deref() == Some(Self::fact_hash(&draft.message)?.as_str()) {
                tx.commit().await?;
                return Ok(false);
            }
            // The tentative transaction did not commit this fact. Another writer
            // may own the old sequence; allocate anew without ever replacing it.
            draft.allocated = false;
        }
        let stream = Self::stream_key(&draft.message);
        let existing: Option<i64> = sqlx::query_scalar("SELECT next_seq FROM log_sequence WHERE stream_key=?")
            .bind(&stream)
            .fetch_optional(&mut *tx)
            .await?;
        if existing.is_none() {
            // Recover pre-allocator buffers/tombstones without renumbering old records.
            let next: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(CAST(substr(object_key,?) AS INTEGER)),-1)+1 FROM log_buffer WHERE substr(object_key,1,?)=?")
                .bind(stream.len() as i64 + 1).bind(stream.len() as i64).bind(&stream).fetch_one(&mut *tx).await?;
            sqlx::query("INSERT INTO log_sequence(stream_key,next_seq) VALUES(?,?)")
                .bind(&stream)
                .bind(next)
                .execute(&mut *tx)
                .await?;
        }
        let seq: i64 = sqlx::query_scalar("UPDATE log_sequence SET next_seq=next_seq+1 WHERE stream_key=? AND next_seq<9223372036854775807 RETURNING next_seq-1")
            .bind(&stream).fetch_optional(&mut *tx).await?.context("log sequence exhausted")?;
        let mut message = draft.message.clone();
        message.seq = seq;
        let inserted = self.append_in(&mut tx, &message).await?;
        draft.message = message;
        draft.allocated = true; // An uncertain commit retries the same immutable identity.
        tx.commit().await?;
        Ok(inserted)
    }

    async fn append_in(&self, tx: &mut Transaction<'_, Sqlite>, message: &DurableMessage<LogLine>) -> Result<bool> {
        message.validate()?;
        ensure!(
            message.workflow_id.is_some() && !message.payload.content.is_empty(),
            "workflow log content required"
        );
        ensure!(
            ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"].contains(&message.payload.level.as_str()),
            "invalid log level"
        );
        let payload = serde_json::to_vec(message)?;
        ensure!(payload.len() <= 262_144, "log record exceeds 256 KiB");
        let fact_hash = Self::fact_hash(message)?;
        let hash = hex::encode(Sha256::digest(&payload));
        let object_key = Self::object_key(message);
        if let Some(existing) = sqlx::query_scalar::<_, String>("SELECT fact_sha256 FROM log_buffer WHERE object_key=?")
            .bind(&object_key)
            .fetch_optional(&mut **tx)
            .await?
        {
            ensure!(existing == fact_hash, "log sequence reused with different content");
            return Ok(false);
        }
        let buffered: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(length(payload)),0) FROM log_buffer WHERE archived=0")
                .fetch_one(&mut **tx)
                .await?;
        ensure!(
            buffered + payload.len() as i64 <= self.max_buffer_bytes,
            "durable log buffer full; apply producer backpressure"
        );
        sqlx::query("INSERT INTO log_buffer(object_key,fact_sha256,content_sha256,payload) VALUES(?,?,?,?)")
            .bind(object_key)
            .bind(fact_hash)
            .bind(hash)
            .bind(payload)
            .execute(&mut **tx)
            .await?;
        let next = message.seq.checked_add(1).context("log sequence exhausted")?;
        sqlx::query("INSERT INTO log_sequence(stream_key,next_seq) VALUES(?,?) ON CONFLICT(stream_key) DO UPDATE SET next_seq=MAX(log_sequence.next_seq,excluded.next_seq)")
            .bind(Self::stream_key(message)).bind(next).execute(&mut **tx).await?;
        Ok(true)
    }

    pub async fn archive_pending(&self, limit: u32) -> Result<usize> {
        ensure!((1..=128).contains(&limit), "archive batch must be bounded");
        let rows = sqlx::query(
            "SELECT object_key,content_sha256,payload FROM log_buffer WHERE archived=0 ORDER BY rowid LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.spool)
        .await?;
        for row in &rows {
            let key: String = row.get("object_key");
            let hash: String = row.get("content_sha256");
            let payload: Vec<u8> = row.get("payload");
            let path = Path::from(key.as_str());
            match self
                .store
                .put_opts(&path, payload.clone().into(), PutMode::Create.into())
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let existing = self.store.get(&path).await?.bytes().await?;
                    ensure!(
                        hex::encode(Sha256::digest(&existing)) == hash,
                        "immutable log object checksum conflict"
                    );
                }
                Err(error) => return Err(error.into()),
            }
            let message: DurableMessage<LogLine> = serde_json::from_slice(&payload)?;
            sqlx::query("INSERT INTO log_chunk(env,tenant_id,space_id,workflow_id,node_id,run_id,first_seq,last_seq,object_key,sha256,byte_length)
                VALUES($1,$2,$3,$4,$5,$6,$7,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
                .bind(&message.scope.env).bind(message.scope.tenant_id).bind(message.scope.space_id)
                .bind(message.workflow_id).bind(message.node_id).bind(&message.run_id).bind(message.seq)
                .bind(&key).bind(&hash).bind(payload.len() as i64).execute(&self.index).await?;
            let indexed: String = sqlx::query_scalar("SELECT sha256 FROM log_chunk WHERE object_key=$1")
                .bind(&key)
                .fetch_one(&self.index)
                .await?;
            ensure!(indexed == hash, "log index checksum conflict");
            // Retain only the idempotency tombstone, freeing durable buffer capacity.
            sqlx::query("UPDATE log_buffer SET archived=1,payload=NULL WHERE object_key=? AND content_sha256=?")
                .bind(key)
                .bind(hash)
                .execute(&self.spool)
                .await?;
        }
        Ok(rows.len())
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
        let rows = sqlx::query(
            "SELECT cursor,object_key,sha256 FROM log_chunk WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND workflow_id=$4 AND node_id=$5 AND run_id=$6 AND cursor>$7 ORDER BY cursor LIMIT $8",
        )
        .bind(&stream.scope.env)
        .bind(stream.scope.tenant_id)
        .bind(stream.scope.space_id)
        .bind(stream.workflow_id)
        .bind(stream.node_id)
        .bind(&stream.run_id)
        .bind(after_cursor)
        .bind(i64::from(limit))
        .fetch_all(&self.index)
        .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row.get("object_key");
            let bytes = self.store.get(&Path::from(key)).await?.bytes().await?;
            ensure!(
                hex::encode(Sha256::digest(&bytes)) == row.get::<String, _>("sha256"),
                "log chunk integrity failure"
            );
            result.push((row.get("cursor"), serde_json::from_slice(&bytes)?));
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
            "logs/{}/{}/{}/{}/{}/{}/{}.json",
            message.scope.env,
            message.scope.tenant_id,
            message.scope.space_id,
            message.workflow_id.unwrap_or_default(),
            message.node_id,
            hex::encode(Sha256::digest(message.run_id.as_bytes())),
            message.seq
        )
    }
}
