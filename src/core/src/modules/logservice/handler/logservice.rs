// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::logservice::storage::postgres::LogservicePostgresRepository;
use crate::modules::logservice::storage::sqlite::LogserviceSQLiteRepository;
use anyhow::Result;
use sigbot_types::logservice::models::{ArchiveRecord, GeneratedArchive, LogChunkCursor, LogChunkIndex};
use sigbot_types::messager::topic::MessageScope;
use sqlx::PgPool;
use std::path::Path;
use uuid::Uuid;

#[derive(Clone)]
pub struct LogHandler {
    index: LogservicePostgresRepository,
    spool: LogserviceSQLiteRepository,
}

impl LogHandler {
    pub async fn from_pool(pool: PgPool, spool_path: &Path, max_buffer_bytes: i64) -> Result<Self> {
        Ok(Self {
            index: LogservicePostgresRepository::from_pool(pool),
            spool: LogserviceSQLiteRepository::open(spool_path, max_buffer_bytes).await?,
        })
    }

    pub async fn connect(database_url: &str, spool_path: &Path, max_buffer_bytes: i64) -> Result<Self> {
        Ok(Self {
            index: LogservicePostgresRepository::connect(database_url).await?,
            spool: LogserviceSQLiteRepository::open(spool_path, max_buffer_bytes).await?,
        })
    }

    pub async fn configured(
        storage: &PostgresStorageProperties,
        spool_path: &Path,
        max_buffer_bytes: i64,
    ) -> Result<Self> {
        Ok(Self {
            index: LogservicePostgresRepository::configured(storage).await?,
            spool: LogserviceSQLiteRepository::open(spool_path, max_buffer_bytes).await?,
        })
    }

    pub async fn append(&self, record: ArchiveRecord, next_sequence: i64, stream_key: &str) -> Result<bool> {
        self.spool.append(record, next_sequence, stream_key).await
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
        self.spool.append_generated(stream_key, tentative, build).await
    }

    pub async fn pending(&self, limit: u32) -> Result<Vec<ArchiveRecord>> {
        self.spool.pending(limit).await
    }

    pub async fn mark_archived(&self, object_key: &str, content_sha256: &str) -> Result<()> {
        self.spool.mark_archived(object_key, content_sha256).await
    }

    pub async fn index<T>(&self, chunk: LogChunkIndex<'_, T>) -> Result<()> {
        self.index.index(chunk).await
    }

    pub async fn read(
        &self,
        scope: &MessageScope,
        workflow_id: Option<Uuid>,
        node_id: i64,
        run_id: &Uuid,
        after_cursor: i64,
        limit: u32,
    ) -> Result<Vec<LogChunkCursor>> {
        self.index
            .read(scope, workflow_id, node_id, run_id, after_cursor, limit)
            .await
    }

    pub async fn close(&self) {
        self.spool.close().await;
    }
}
