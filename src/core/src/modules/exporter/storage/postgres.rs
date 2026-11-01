// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::MessageScope;
use sqlx::Postgres;
use sqlx::Transaction;
use uuid::Uuid;

pub struct ExportClaim<'a> {
    consumer: String,
    source: DurableMessage<Value>,
    transaction: Transaction<'a, Postgres>,
}

impl ExportClaim<'_> {
    pub async fn next_sequence(&mut self, scope: &MessageScope, node_id: i64, run_id: &Uuid) -> Result<i64> {
        MessagerPostgresRepository::next_sequence(&mut self.transaction, scope, node_id, run_id).await
    }

    pub async fn complete(
        mut self,
        connection_id: Uuid,
        partition: &str,
        cursor: Value,
        topic: &str,
        output: &DurableMessage<Value>,
    ) -> Result<()> {
        sqlx::query("INSERT INTO exp_checkpoint(env,tenant_id,connection_id,run_id,partition_key,seq,cursor)
            VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(env,tenant_id,connection_id,run_id,partition_key)
            DO UPDATE SET seq=GREATEST(exp_checkpoint.seq,EXCLUDED.seq),
            cursor=CASE WHEN EXCLUDED.seq>exp_checkpoint.seq THEN EXCLUDED.cursor ELSE exp_checkpoint.cursor END,updated_at=NOW()")
            .bind(&self.source.scope.env)
            .bind(self.source.scope.tenant_id)
            .bind(connection_id)
            .bind(&self.source.run_id)
            .bind(partition)
            .bind(self.source.seq)
            .bind(cursor)
            .execute(&mut *self.transaction)
            .await?;
        MessagerPostgresRepository::enqueue(&mut self.transaction, &self.consumer, topic, output).await?;
        MessagerPostgresRepository::complete(&mut self.transaction, &self.consumer, &self.source).await?;
        self.transaction.commit().await?;
        Ok(())
    }
}

pub struct ExporterPostgresRepository {
    pool: sqlx::PgPool,
}

impl ExporterPostgresRepository {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(url)
                .await?,
        })
    }

    pub fn message_store(&self) -> MessagerPostgresRepository {
        MessagerPostgresRepository::new(self.pool.clone())
    }

    pub async fn connection_exists(&self, connection_id: Uuid, scope: &MessageScope) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM cfg_connection WHERE id=$1 AND env=$2 AND tenant_id=$3")
                .bind(connection_id)
                .bind(&scope.env)
                .bind(scope.tenant_id)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<()> {
        let input = &message.durable;
        let mut transaction = self.pool.begin().await?;
        if MessagerPostgresRepository::accept(&mut transaction, consumer, topic, message).await? {
            sqlx::query(
                "UPDATE msg_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason=$7
                WHERE env=$1 AND tenant_id=$2 AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
            )
            .bind(&input.scope.env)
            .bind(input.scope.tenant_id)
            .bind(consumer)
            .bind(input.node_id)
            .bind(&input.run_id)
            .bind(input.seq)
            .bind(reason)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn claim(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<ExportClaim<'_>>> {
        let mut transaction = self.pool.begin().await?;
        if !MessagerPostgresRepository::accept(&mut transaction, consumer, topic, message).await? {
            transaction.commit().await?;
            return Ok(None);
        }
        Ok(Some(ExportClaim {
            consumer: consumer.to_owned(),
            source: message.durable.clone(),
            transaction,
        }))
    }
}
