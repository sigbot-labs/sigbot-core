// SPDX-License-Identifier: GPL-3.0-only
//! Shared persistence participates in the caller's domain transaction.
//! No worker or database is deployed for this library.

use crate::client::messager_factory::ISigbotMessagerClient;
use crate::client::reply_pairing::ReplyPairingCache;
use anyhow::{ensure, Result};
use chrono::Utc;
use serde_json::Value;
use sigbot_types::modules::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::modules::messager::topic::MessageScope;
use sqlx::{PgPool, Postgres, Row, Transaction};

#[derive(Clone)]
pub struct PostgresMessageStore {
    pool: PgPool,
}

impl PostgresMessageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Restore missing v2 transport routing from the immutable scoped run binding.
    /// Do not guess from the current worker or rewrite an explicit Workflow/domain payload.
    pub async fn restore_legacy_workflow(&self, wire: &mut MessageEnvelope<Value>) -> Result<bool> {
        if wire.durable.workflow_id.is_some() {
            return Ok(false);
        }
        let input = &wire.durable;
        input.scope.validate()?;
        ensure!(
            input.version == 2 && !input.run_id.is_empty() && input.run_id.len() <= 128,
            "invalid legacy message identity"
        );
        let workflow: Option<i64> = sqlx::query_scalar(
            "SELECT workflow_id FROM workflow_run
            WHERE run_id=$1 AND env=$2 AND tenant_id=$3 AND space_id=$4",
        )
        .bind(&input.run_id)
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.scope.space_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(id) = workflow {
            ensure!(id > 0, "invalid persisted Workflow binding");
            wire.durable.workflow_id = Some(id);
        }
        Ok(workflow.is_some())
    }

    /// Explicit operational replay after broker loss or downstream repair. The host
    /// must authorize this scoped operation. Existing run/node/sequence keys survive.
    pub async fn replay(
        &self,
        producer: &str,
        scope: &MessageScope,
        node_id: i64,
        run_id: &str,
        sequences: std::ops::RangeInclusive<i64>,
    ) -> Result<u64> {
        scope.validate()?;
        ensure!(
            !producer.is_empty() && node_id > 0 && !run_id.is_empty(),
            "invalid replay owner"
        );
        let (first, last) = (*sequences.start(), *sequences.end());
        ensure!(
            first >= 0 && last >= first && last - first < 10_000,
            "replay range exceeds its bound"
        );
        Ok(sqlx::query(
            "UPDATE messager_outbox SET state='pending',available_at=NOW(),sent_at=NULL
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND producer=$4 AND node_id=$5 AND run_id=$6
            AND seq BETWEEN $7 AND $8 AND state='sent'",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(scope.space_id)
        .bind(producer)
        .bind(node_id)
        .bind(run_id)
        .bind(first)
        .bind(last)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn next_sequence(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        node_id: i64,
        run_id: &str,
    ) -> Result<i64> {
        scope.validate()?;
        ensure!(node_id > 0 && !run_id.is_empty(), "invalid stream identity");
        Ok(sqlx::query_scalar(
            "INSERT INTO workflow_sequence(env,tenant_id,space_id,node_id,run_id,next_seq)
            VALUES($1,$2,$3,$4,$5,1) ON CONFLICT(env,tenant_id,space_id,node_id,run_id)
            DO UPDATE SET next_seq=workflow_sequence.next_seq+1 RETURNING next_seq-1",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(scope.space_id)
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
            "INSERT INTO messager_outbox(env,tenant_id,space_id,producer,node_id,run_id,seq,topic,payload,attempt_no)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.scope.space_id)
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
                "SELECT topic,payload FROM messager_outbox WHERE env=$1 AND tenant_id=$2
                AND space_id=$3 AND producer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7",
            )
            .bind(&message.scope.env)
            .bind(message.scope.tenant_id)
            .bind(message.scope.space_id)
            .bind(producer)
            .bind(message.node_id)
            .bind(&message.run_id)
            .bind(message.seq)
            .fetch_one(&mut **tx)
            .await?;
            Self::same_fact(existing.get("topic"), existing.get("payload"), topic, payload)?;
        }
        Ok(rows == 1)
    }

    /// Returns false when the event was committed or durably quarantined by this consumer.
    /// Call complete in this same transaction after domain changes and outgoing enqueue.
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
            "INSERT INTO messager_inbox(env,tenant_id,space_id,consumer,node_id,run_id,seq,topic,payload,attempt_no)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(env,tenant_id,space_id,consumer,node_id,run_id,seq)
             DO UPDATE SET attempt_no=GREATEST(messager_inbox.attempt_no,EXCLUDED.attempt_no)
             RETURNING state,topic,payload",
        )
        .bind(&durable.scope.env)
        .bind(durable.scope.tenant_id)
        .bind(durable.scope.space_id)
        .bind(consumer)
        .bind(durable.node_id)
        .bind(&durable.run_id)
        .bind(durable.seq)
        .bind(topic)
        .bind(&payload)
        .bind(i32::try_from(durable.attempt_no)?)
        .fetch_one(&mut **tx)
        .await?;
        Self::same_fact(row.get("topic"), row.get("payload"), topic, payload)?;
        let state: String = row.get("state");
        Ok(!matches!(state.as_str(), "done" | "quarantined"))
    }

    pub async fn complete(
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        message: &DurableMessage<Value>,
    ) -> Result<()> {
        let count = sqlx::query(
            "UPDATE messager_inbox SET state='done',completed_at=NOW() WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7 AND state='pending'",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.scope.space_id)
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

    /// One batch is bounded. SKIP LOCKED permits independent relay processes.
    /// A crash after PUBACK but before commit replays the same business identity.
    /// next_traceparent must create a new producer span for each application retry.
    pub async fn dispatch_pending(
        &self,
        producer: &str,
        client: &dyn ISigbotMessagerClient,
        limit: u32,
        next_traceparent: &(dyn Fn(&str) -> Result<String> + Send + Sync),
    ) -> Result<usize> {
        ensure!((1..=256).contains(&limit), "relay batch must be between 1 and 256");
        let mut sent = 0;
        for _ in 0..limit {
            let mut tx = self.pool.begin().await?;
            let row = sqlx::query(
                "SELECT payload,topic,attempt_no FROM messager_outbox WHERE producer=$1
                AND state='pending' AND available_at<=NOW() ORDER BY available_at,node_id,run_id,seq
                LIMIT 1 FOR UPDATE SKIP LOCKED",
            )
            .bind(producer)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(row) = row else { break };
            let payload: Value = row.get("payload");
            let mut durable: DurableMessage<Value> = serde_json::from_value(payload)?;
            let attempt: i32 = row.get("attempt_no");
            if attempt > durable.attempt_no as i32 {
                durable.attempt_no = (attempt - 1).try_into()?;
                let traceparent = next_traceparent(&durable.traceparent)?;
                durable.retry(traceparent, Utc::now())?;
            }
            let topic: String = row.get("topic");
            durable.validate_topic(&topic)?;
            let mut envelope = MessageEnvelope::from_durable(durable.clone())?;
            if let Some(pairing) = ReplyPairingCache::shared().get(&durable) {
                envelope.request_span_id = Some(pairing);
            }
            let wire = serde_json::to_string(&envelope)?;
            let result = client.publish_confirmed(&topic, &wire).await;
            let (state, delay) = if result.is_ok() {
                ("sent", 0_i32)
            } else {
                ("pending", 2_i32.pow((attempt as u32).min(8)))
            };
            sqlx::query(
                "UPDATE messager_outbox SET state=$8,attempt_no=$9,payload=$10,
                available_at=NOW()+make_interval(secs=>$11),sent_at=CASE WHEN $8='sent' THEN NOW() ELSE NULL END
                WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND producer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7",
            )
            .bind(&durable.scope.env)
            .bind(durable.scope.tenant_id)
            .bind(durable.scope.space_id)
            .bind(producer)
            .bind(durable.node_id)
            .bind(&durable.run_id)
            .bind(durable.seq)
            .bind(state)
            .bind(
                attempt
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("attempt overflow"))?,
            )
            .bind(serde_json::to_value(&durable)?)
            .bind(f64::from(delay))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            result?;
            sent += 1;
        }
        Ok(sent)
    }

    fn same_fact(existing_topic: String, mut existing: Value, topic: &str, mut proposed: Value) -> Result<()> {
        for field in ["attempt_no", "traceparent", "produced_at"] {
            if let Some(object) = existing.as_object_mut() {
                object.remove(field);
            }
            if let Some(object) = proposed.as_object_mut() {
                object.remove(field);
            }
        }
        ensure!(
            existing_topic == topic && existing == proposed,
            "business identity reused with a different message"
        );
        Ok(())
    }
}
