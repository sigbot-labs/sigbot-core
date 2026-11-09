// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use serde_json::{json, Value};
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::notifier::request::NotificationRequest;
use sigbot_types::notifier::request::{NotificationScope, PendingNotification};
use sqlx::{Connection, Row};
use tokio::sync::Mutex;

pub struct NotifierPostgresRepository {
    pool: sqlx::PgPool,
    owner: Mutex<Option<sqlx::PgConnection>>,
    identity: NotificationScope,
}

impl NotifierPostgresRepository {
    pub async fn connect(url: &str, identity: NotificationScope) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(url)
                .await?,
            owner: Mutex::new(None),
            identity,
        })
    }

    pub fn message_store(&self) -> MessagerPostgresRepository {
        MessagerPostgresRepository::new(self.pool.clone())
    }

    pub async fn acquire_owner(&self, url: &str) -> Result<()> {
        let mut owner = self.owner.lock().await;
        ensure!(owner.is_none(), "notification node already owns a delivery lease");
        let mut connection = sqlx::PgConnection::connect(url).await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1,0))")
            .bind(&self.identity.consumer)
            .fetch_one(&mut connection)
            .await?;
        ensure!(acquired, "another process owns this notification node");
        *owner = Some(connection);
        Ok(())
    }

    pub async fn release_owner(&self) {
        self.owner.lock().await.take();
    }

    pub async fn ensure_owner(&self) -> Result<()> {
        let mut owner = self.owner.lock().await;
        sqlx::query("SELECT 1")
            .execute(owner.as_mut().context("notification node has no delivery lease")?)
            .await
            .context("notification delivery lease connection lost")?;
        Ok(())
    }

    pub async fn queue(
        &self,
        topic: &str,
        message: &MessageEnvelope<Value>,
        source: &DurableMessage<NotificationRequest>,
        template_version: &str,
        title: &str,
        body: &str,
        result_topic: &str,
    ) -> Result<Option<DurableMessage<Value>>> {
        let mut transaction = self.pool.begin().await?;
        if !MessagerPostgresRepository::accept(&mut transaction, &self.identity.consumer, topic, message).await? {
            transaction.commit().await?;
            return Ok(None);
        }
        let state = if source.payload.channel == "in_app" {
            "delivered"
        } else {
            "pending"
        };
        let inserted=sqlx::query("INSERT INTO ntf_delivery(env,tenant_id,workflow_id,owner_node_id,producer_node_id,run_id,seq,channel,recipient,template_version,payload,state,delivered_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,CASE WHEN $12='delivered' THEN NOW() ELSE NULL END) ON CONFLICT DO NOTHING")
            .bind(&source.scope.env).bind(source.scope.tenant_id)
            .bind(self.identity.workflow_id).bind(self.identity.node_id).bind(source.node_id).bind(&source.run_id)
            .bind(source.seq).bind(&source.payload.channel).bind(&source.payload.recipient).bind(template_version)
            .bind(json!({"title":title,"body":body,"source":source,"kind":source.payload.variables.get("kind")}))
            .bind(state).execute(&mut *transaction).await?.rows_affected()>0;
        let response = if inserted {
            Some(
                self.response(&mut transaction, source, state, None, 0, result_topic)
                    .await?,
            )
        } else {
            None
        };
        MessagerPostgresRepository::complete(&mut transaction, &self.identity.consumer, &message.durable).await?;
        transaction.commit().await?;
        Ok(response)
    }

    pub async fn recover_sending(&self, result_topic: &str) -> Result<()> {
        loop {
            let mut transaction = self.pool.begin().await?;
            let rows=sqlx::query("SELECT payload,attempt_no FROM ntf_delivery WHERE env=$1 AND tenant_id=$2 AND workflow_id=$3 AND owner_node_id=$4 AND state='sending' LIMIT 32 FOR UPDATE")
                .bind(&self.identity.scope.env).bind(self.identity.scope.tenant_id)
                .bind(self.identity.workflow_id).bind(self.identity.node_id).fetch_all(&mut *transaction).await?;
            if rows.is_empty() {
                transaction.commit().await?;
                return Ok(());
            }
            for row in rows {
                let source = serde_json::from_value(row.get::<Value, _>("payload")["source"].clone())?;
                self.finish_in(
                    &mut transaction,
                    &source,
                    "unknown",
                    None,
                    row.get::<i32, _>("attempt_no") as u32,
                    result_topic,
                )
                .await?;
            }
            transaction.commit().await?;
        }
    }

    pub async fn claim_pending(&self) -> Result<Option<PendingNotification>> {
        self.ensure_owner().await?;
        let mut transaction = self.pool.begin().await?;
        let row=sqlx::query("SELECT payload,attempt_no FROM ntf_delivery WHERE env=$1 AND tenant_id=$2 AND workflow_id=$3 AND owner_node_id=$4 AND state='pending' AND available_at<=NOW() ORDER BY available_at LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(&self.identity.scope.env).bind(self.identity.scope.tenant_id)
            .bind(self.identity.workflow_id).bind(self.identity.node_id).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let payload: Value = row.get("payload");
        let source = serde_json::from_value(payload["source"].clone())?;
        let attempt = row.get::<i32, _>("attempt_no") as u32 + 1;
        self.update(&mut transaction, &source, "sending", None, attempt).await?;
        transaction.commit().await?;
        Ok(Some(PendingNotification {
            source,
            body: payload["body"]
                .as_str()
                .context("notification content missing")?
                .to_owned(),
            attempt,
        }))
    }

    pub async fn finish(
        &self,
        pending: &PendingNotification,
        state: &str,
        receipt: Option<&str>,
        result_topic: &str,
    ) -> Result<DurableMessage<Value>> {
        let mut transaction = self.pool.begin().await?;
        let response = self
            .finish_in(
                &mut transaction,
                &pending.source,
                state,
                receipt,
                pending.attempt,
                result_topic,
            )
            .await?;
        transaction.commit().await?;
        Ok(response)
    }

    async fn finish_in(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt: u32,
        topic: &str,
    ) -> Result<DurableMessage<Value>> {
        self.update(transaction, source, state, receipt, attempt).await?;
        self.response(transaction, source, state, receipt, attempt, topic).await
    }

    async fn update(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt: u32,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE ntf_delivery SET state=$9,provider_receipt=$10,attempt_no=$11,updated_at=NOW(),available_at=CASE WHEN $9='pending' THEN NOW()+make_interval(secs=>$12) ELSE available_at END,delivered_at=CASE WHEN $9='delivered' THEN NOW() ELSE delivered_at END
            WHERE env=$1 AND tenant_id=$2 AND workflow_id=$3 AND owner_node_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7 AND channel=$8 AND recipient=$13")
            .bind(&self.identity.scope.env).bind(self.identity.scope.tenant_id).bind(self.identity.workflow_id).bind(self.identity.node_id)
            .bind(source.node_id).bind(&source.run_id).bind(source.seq).bind(&source.payload.channel).bind(state).bind(receipt)
            .bind(attempt as i32).bind((2_u32.pow(attempt.min(6))) as f64).bind(&source.payload.recipient).execute(&mut **transaction).await?.rows_affected();
        ensure!(changed == 1, "notification owner/delivery mismatch");
        Ok(())
    }

    async fn response(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt: u32,
        topic: &str,
    ) -> Result<DurableMessage<Value>> {
        let sequence = MessagerPostgresRepository::next_sequence(
            transaction,
            &self.identity.scope,
            self.identity.node_id,
            &source.run_id,
        )
        .await?;
        let response = DurableMessage {
            version: 2,
            scope: self.identity.scope.clone(),
            workflow_id: Some(self.identity.workflow_id),
            account_id: None,
            node_id: self.identity.node_id,
            target_node_id: Some(source.node_id),
            run_id: source.run_id.clone(),
            seq: sequence,
            attempt_no: attempt,
            kind: MessageKind::Result,
            event_time: source.event_time,
            available_time: source.available_time,
            produced_at: Utc::now(),
            traceparent: MessageTrace::child("notification.result", &source.traceparent).traceparent(),
            payload: json!({"producer_node_id":source.node_id,"seq":source.seq,"channel":source.payload.channel,"recipient":source.payload.recipient,"state":state,"provider_receipt":receipt}),
        };
        MessagerPostgresRepository::enqueue(transaction, &self.identity.consumer, topic, &response).await?;
        Ok(response)
    }
}
