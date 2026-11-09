// SPDX-License-Identifier: GPL-3.0-only
// Shared persistence participates in the caller's domain transaction.
// No worker or database is deployed for this library.

use crate::storage::postgres::PostgresRepository;
use anyhow::ensure;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::messager::envelope::MessageIdentity;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct MessagerPostgresRepository {
    repository: Arc<PostgresRepository<()>>,
}

impl MessagerPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repository: Arc::new(PostgresRepository::from_pool(pool)),
        }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        self.repository.get_pool()
    }

    /// The transaction retains the row lock until the caller records the broker outcome.
    pub async fn claim(&self, producer: &str) -> Result<Option<PendingPostgresMessage<'_>>> {
        let mut tx = self.repository.get_pool().begin().await?;
        let row = sqlx::query(
            "SELECT payload,topic,attempt_no FROM msg_outbox WHERE producer=$1
             AND state='pending' AND available_at<=NOW() ORDER BY available_at,node_id,run_id,seq
             LIMIT 1 FOR UPDATE SKIP LOCKED",
        )
        .bind(producer)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else { return Ok(None) };
        Ok(Some(PendingPostgresMessage {
            message: serde_json::from_value(row.get("payload"))?,
            topic: row.get("topic"),
            attempt: row.get("attempt_no"),
            producer: producer.to_owned(),
            tx,
        }))
    }

    /// Returns false when the event was committed or durably quarantined.
    pub async fn accept(
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<bool> {
        message.validate()?;
        let durable = &message.durable;
        durable.validate_topic(topic)?;
        ensure!(!consumer.is_empty(), "consumer cannot be empty");
        let payload = serde_json::to_value(durable)?;
        let row = sqlx::query(
            "INSERT INTO msg_inbox(env,tenant_id,consumer,node_id,run_id,seq,topic,payload,attempt_no)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(env,tenant_id,consumer,node_id,run_id,seq)
             DO UPDATE SET attempt_no=GREATEST(msg_inbox.attempt_no,EXCLUDED.attempt_no)
             RETURNING state,topic,payload",
        )
        .bind(&durable.scope.env)
        .bind(durable.scope.tenant_id)
        .bind(consumer)
        .bind(durable.node_id)
        .bind(&durable.run_id)
        .bind(durable.seq)
        .bind(topic)
        .bind(&payload)
        .bind(i32::try_from(durable.attempt_no)?)
        .fetch_one(&mut **tx)
        .await?;
        MessageIdentity::validate(row.get("topic"), row.get("payload"), topic, payload)?;
        let state: String = row.get("state");
        Ok(!matches!(state.as_str(), "done" | "quarantined"))
    }

    pub async fn complete(
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        message: &DurableMessage<Value>,
    ) -> Result<()> {
        let count = sqlx::query(
            "UPDATE msg_inbox SET state='done',completed_at=NOW() WHERE env=$1 AND tenant_id=$2
            AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6 AND state='pending'",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(consumer)
        .bind(message.node_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        ensure!(
            count == 1,
            "inbox completion requires a pending delivery in this transaction"
        );
        Ok(())
    }

    pub async fn replay(
        &self,
        producer: &str,
        scope: &MessageScope,
        node_id: i64,
        run_id: &Uuid,
        sequences: std::ops::RangeInclusive<i64>,
    ) -> Result<u64> {
        scope.validate()?;
        ensure!(
            !producer.is_empty() && node_id > 0 && run_id.get_version_num() == 7,
            "invalid replay owner"
        );
        let (first, last) = (*sequences.start(), *sequences.end());
        ensure!(
            first >= 0 && last >= first && last - first < 10_000,
            "replay range exceeds its bound"
        );
        Ok(sqlx::query(
            "UPDATE msg_outbox SET state='pending',available_at=NOW(),sent_at=NULL
            WHERE env=$1 AND tenant_id=$2 AND producer=$3 AND node_id=$4 AND run_id=$5
            AND seq BETWEEN $6 AND $7 AND state='sent'",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(producer)
        .bind(node_id)
        .bind(run_id)
        .bind(first)
        .bind(last)
        .execute(self.repository.get_pool())
        .await?
        .rows_affected())
    }

    pub async fn next_sequence(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        node_id: i64,
        run_id: &Uuid,
    ) -> Result<i64> {
        scope.validate()?;
        ensure!(node_id > 0 && run_id.get_version_num() == 7, "invalid stream identity");
        Ok(sqlx::query_scalar(
            "INSERT INTO wf_sequence(env,tenant_id,node_id,run_id,next_seq)
            VALUES($1,$2,$3,$4,1) ON CONFLICT(env,tenant_id,node_id,run_id)
            DO UPDATE SET next_seq=wf_sequence.next_seq+1 RETURNING next_seq-1",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(node_id)
        .bind(run_id)
        .fetch_one(&mut **tx)
        .await?)
    }

    pub async fn enqueue(
        tx: &mut Transaction<'_, Postgres>,
        producer: &str,
        topic: &str,
        message: &DurableMessage<Value>,
    ) -> Result<bool> {
        message.validate_topic(topic)?;
        ensure!(!producer.is_empty(), "producer cannot be empty");
        let payload = serde_json::to_value(message)?;
        let rows = sqlx::query(
            "INSERT INTO msg_outbox(env,tenant_id,producer,node_id,run_id,seq,topic,payload,attempt_no)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(producer)
        .bind(message.node_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(topic)
        .bind(&payload)
        .bind(i32::try_from(message.attempt_no)?)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if rows == 0 {
            let existing = sqlx::query(
                "SELECT topic,payload FROM msg_outbox WHERE env=$1 AND tenant_id=$2
                AND producer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
            )
            .bind(&message.scope.env)
            .bind(message.scope.tenant_id)
            .bind(producer)
            .bind(message.node_id)
            .bind(&message.run_id)
            .bind(message.seq)
            .fetch_one(&mut **tx)
            .await?;
            MessageIdentity::validate(existing.get("topic"), existing.get("payload"), topic, payload)?;
        }
        Ok(rows == 1)
    }
}

/// An exclusive pending outbox row. Dropping it rolls back and permits redelivery.
pub struct PendingPostgresMessage<'a> {
    pub message: DurableMessage<Value>,
    pub topic: String,
    pub attempt: i32,
    producer: String,
    tx: Transaction<'a, Postgres>,
}
impl PendingPostgresMessage<'_> {
    pub async fn complete(mut self, delivered: bool) -> Result<()> {
        let (state, delay) = if delivered {
            ("sent", 0_i32)
        } else {
            ("pending", 2_i32.pow((self.attempt as u32).min(8)))
        };
        sqlx::query(
            "UPDATE msg_outbox SET state=$7,attempt_no=$8,payload=$9,
                available_at=NOW()+make_interval(secs=>$10),sent_at=CASE WHEN $7='sent' THEN NOW() ELSE NULL END
                WHERE env=$1 AND tenant_id=$2 AND producer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.message.scope.env)
        .bind(self.message.scope.tenant_id)
        .bind(&self.producer)
        .bind(self.message.node_id)
        .bind(&self.message.run_id)
        .bind(self.message.seq)
        .bind(state)
        .bind(
            self.attempt
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("attempt overflow"))?,
        )
        .bind(serde_json::to_value(&self.message)?)
        .bind(f64::from(delay))
        .execute(&mut *self.tx)
        .await?;
        self.tx.commit().await?;
        Ok(())
    }
}
