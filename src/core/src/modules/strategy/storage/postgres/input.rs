// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::strategy::storage::postgres::StrategyStorageIdentity;
use crate::storage::postgres::PostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sqlx::Row;

pub struct StrategyInputPostgresRepository {
    pool: sqlx::PgPool,
}

impl StrategyInputPostgresRepository {
    pub async fn connect(storage: &PostgresStorageProperties) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Ok(Self {
            pool: repository.get_pool().clone(),
        })
    }

    pub async fn processed(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<bool> {
        let existing: Option<Value> = sqlx::query_scalar(
            "SELECT payload FROM msg_inbox WHERE env=$1 AND tenant_id=$2
            AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6 AND state='done'",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(format!("strategy.{}", identity.node_id))
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(mut existing) = existing {
            let mut proposed = serde_json::to_value(input)?;
            for field in ["attempt_no", "traceparent", "produced_at"] {
                existing
                    .as_object_mut()
                    .context("invalid persisted strategy input")?
                    .remove(field);
                proposed
                    .as_object_mut()
                    .context("invalid strategy input")?
                    .remove(field);
            }
            ensure!(
                existing == proposed,
                "processed strategy identity reused with different input"
            );
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn replay_clock(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<DateTime<Utc>> {
        let checkpoint =
            sqlx::query("SELECT virtual_time,state FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2 AND run_id=$3")
                .bind(&identity.scope.env)
                .bind(identity.scope.tenant_id)
                .bind(identity.run_id)
                .fetch_one(&self.pool)
                .await?;
        if checkpoint.get::<String, _>("state") != "running" {
            ensure!(
                self.processed(identity, input).await?,
                "completed replay cannot admit another decision"
            );
        }
        Ok(checkpoint.get("virtual_time"))
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        if MessagerPostgresRepository::accept(&mut transaction, consumer, topic, wire).await? {
            sqlx::query("UPDATE msg_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason=$7
                WHERE env=$1 AND tenant_id=$2 AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6 AND state='pending'")
                .bind(&wire.durable.scope.env).bind(wire.durable.scope.tenant_id)
                .bind(consumer).bind(wire.durable.node_id).bind(&wire.durable.run_id).bind(wire.durable.seq).bind(reason)
                .execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(false)
    }
}
