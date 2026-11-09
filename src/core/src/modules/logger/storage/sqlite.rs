// SPDX-License-Identifier: GPL-3.0-only

use anyhow::{ensure, Context, Result};
use sigbot_types::logger::models::{ArchiveRecord, GeneratedArchive};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::str::FromStr;

#[derive(Clone)]
pub struct LoggerSQLiteRepository {
    pool: SqlitePool,
    max_buffer_bytes: i64,
}

impl LoggerSQLiteRepository {
    pub async fn open(path: &Path, max_buffer_bytes: i64) -> Result<Self> {
        Self::prepare_buffer(path, max_buffer_bytes)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS log_buffer(object_key TEXT PRIMARY KEY,fact_sha256 TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,payload BLOB,archived INTEGER NOT NULL DEFAULT 0)",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS log_sequence(stream_key TEXT PRIMARY KEY,next_seq INTEGER NOT NULL)")
            .execute(&pool)
            .await?;
        Ok(Self { pool, max_buffer_bytes })
    }

    fn prepare_buffer(path: &Path, max_buffer_bytes: i64) -> Result<()> {
        ensure!(
            max_buffer_bytes >= 262_144,
            "log buffer must hold at least one maximum-size record"
        );
        ensure!(
            path.is_absolute(),
            "log buffer must have an absolute persistent-volume path"
        );
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new();
        file.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            use std::os::unix::fs::PermissionsExt;
            if path.exists() {
                ensure!(
                    std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                    "log buffer must not be accessible to other users"
                );
            }
            file.mode(0o600);
        }
        file.open(path)?;
        Ok(())
    }

    pub async fn append(&self, record: ArchiveRecord, next_sequence: i64, stream_key: &str) -> Result<bool> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let inserted = Self::append_in(&mut transaction, &record, self.max_buffer_bytes).await?;
        sqlx::query("INSERT INTO log_sequence(stream_key,next_seq) VALUES(?,?) ON CONFLICT(stream_key) DO UPDATE SET next_seq=MAX(log_sequence.next_seq,excluded.next_seq)")
            .bind(stream_key).bind(next_sequence).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn append_generated<F>(
        &self,
        stream_key: &str,
        tentative: Option<(i64, &str, &str)>,
        build: F,
    ) -> Result<GeneratedArchive>
    where
        F: FnOnce(i64) -> Result<ArchiveRecord>,
    {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some((sequence, object_key, fact_sha256)) = tentative {
            let existing: Option<String> = sqlx::query_scalar("SELECT fact_sha256 FROM log_buffer WHERE object_key=?")
                .bind(object_key)
                .fetch_optional(&mut *transaction)
                .await?;
            if existing.as_deref() == Some(fact_sha256) {
                transaction.commit().await?;
                return Ok(GeneratedArchive {
                    sequence,
                    inserted: false,
                });
            }
        }
        if sqlx::query_scalar::<_, i64>("SELECT next_seq FROM log_sequence WHERE stream_key=?")
            .bind(stream_key)
            .fetch_optional(&mut *transaction)
            .await?
            .is_none()
        {
            let next: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(CAST(substr(object_key,?) AS INTEGER)),-1)+1 FROM log_buffer WHERE substr(object_key,1,?)=?")
                .bind(stream_key.len() as i64 + 1).bind(stream_key.len() as i64).bind(stream_key)
                .fetch_one(&mut *transaction).await?;
            sqlx::query("INSERT INTO log_sequence(stream_key,next_seq) VALUES(?,?)")
                .bind(stream_key)
                .bind(next)
                .execute(&mut *transaction)
                .await?;
        }
        let sequence: i64 = sqlx::query_scalar("UPDATE log_sequence SET next_seq=next_seq+1 WHERE stream_key=? AND next_seq<9223372036854775807 RETURNING next_seq-1")
            .bind(stream_key).fetch_optional(&mut *transaction).await?.context("log sequence exhausted")?;
        let record = build(sequence)?;
        let inserted = Self::append_in(&mut transaction, &record, self.max_buffer_bytes).await?;
        transaction.commit().await?;
        Ok(GeneratedArchive { sequence, inserted })
    }

    async fn append_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        record: &ArchiveRecord,
        max_buffer_bytes: i64,
    ) -> Result<bool> {
        if let Some(existing) = sqlx::query_scalar::<_, String>("SELECT fact_sha256 FROM log_buffer WHERE object_key=?")
            .bind(&record.object_key)
            .fetch_optional(&mut **transaction)
            .await?
        {
            ensure!(
                existing == record.fact_sha256,
                "log sequence reused with different content"
            );
            return Ok(false);
        }
        let buffered: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(length(payload)),0) FROM log_buffer WHERE archived=0")
                .fetch_one(&mut **transaction)
                .await?;
        ensure!(
            buffered + record.payload.len() as i64 <= max_buffer_bytes,
            "durable log buffer full; apply producer backpressure"
        );
        sqlx::query("INSERT INTO log_buffer(object_key,fact_sha256,content_sha256,payload) VALUES(?,?,?,?)")
            .bind(&record.object_key)
            .bind(&record.fact_sha256)
            .bind(&record.content_sha256)
            .bind(&record.payload)
            .execute(&mut **transaction)
            .await?;
        Ok(true)
    }

    pub async fn pending(&self, limit: u32) -> Result<Vec<ArchiveRecord>> {
        let rows = sqlx::query("SELECT object_key,fact_sha256,content_sha256,payload FROM log_buffer WHERE archived=0 ORDER BY rowid LIMIT ?")
            .bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(ArchiveRecord {
                    object_key: row.try_get("object_key")?,
                    fact_sha256: row.try_get("fact_sha256")?,
                    content_sha256: row.try_get("content_sha256")?,
                    payload: row.try_get("payload")?,
                })
            })
            .collect()
    }

    pub async fn mark_archived(&self, object_key: &str, content_sha256: &str) -> Result<()> {
        sqlx::query("UPDATE log_buffer SET archived=1,payload=NULL WHERE object_key=? AND content_sha256=?")
            .bind(object_key)
            .bind(content_sha256)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}
