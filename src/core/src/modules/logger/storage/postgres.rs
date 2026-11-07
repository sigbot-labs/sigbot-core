// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::storage::postgres::PostgresRepository;
use anyhow::{ensure, Result};
use sigbot_types::messager::topic::MessageScope;
use sqlx::Row;
use uuid::Uuid;

use sigbot_types::logger::models::{LogChunkCursor, LogChunkIndex};

#[derive(Clone)]
pub struct LoggerPostgresRepository {
    pool: sqlx::PgPool,
}

impl LoggerPostgresRepository {
    pub async fn configured(storage: &PostgresStorageProperties) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Ok(Self::from_pool(repository.get_pool().clone()))
    }

    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(database_url)
                .await?,
        })
    }

    pub async fn index<T>(&self, chunk: LogChunkIndex<'_, T>) -> Result<()> {
        let message = chunk.message;
        sqlx::query("INSERT INTO log_chunk(env,tenant_id,workflow_id,node_id,run_id,first_seq,last_seq,object_key,sha256,byte_length)
            VALUES($1,$2,$3,$4,$5,$6,$6,$7,$8,$9) ON CONFLICT DO NOTHING")
            .bind(&message.scope.env).bind(message.scope.tenant_id)
            .bind(message.workflow_id).bind(message.node_id).bind(&message.run_id).bind(message.seq)
            .bind(chunk.object_key).bind(chunk.sha256).bind(chunk.byte_length).execute(&self.pool).await?;
        let indexed: String = sqlx::query_scalar("SELECT sha256 FROM log_chunk WHERE object_key=$1")
            .bind(chunk.object_key)
            .fetch_one(&self.pool)
            .await?;
        ensure!(indexed == chunk.sha256, "log index checksum conflict");
        Ok(())
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
        let rows = sqlx::query(
            "SELECT cursor,object_key,sha256 FROM log_chunk WHERE env=$1 AND tenant_id=$2
            AND workflow_id=$3 AND node_id=$4 AND run_id=$5 AND cursor>$6 ORDER BY cursor LIMIT $7",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(workflow_id)
        .bind(node_id)
        .bind(run_id)
        .bind(after_cursor)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(LogChunkCursor {
                    cursor: row.try_get("cursor")?,
                    object_key: row.try_get("object_key")?,
                    sha256: row.try_get("sha256")?,
                })
            })
            .collect()
    }
}
