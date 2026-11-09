// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::storage::postgres::PostgresRepository;
use anyhow::{ensure, Result};
use chrono::Utc;
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::{MessageScope, WorkflowTopic};
use sigbot_types::strategy::execution::{Effects, StrategyRequest, StrategyTimer};
use std::sync::Arc;
use uuid::Uuid;

pub type ProducerTrace = Arc<dyn Fn(&str) -> Result<String> + Send + Sync>;

pub struct StrategyStorageIdentity<'a> {
    pub scope: &'a MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub run_id: &'a Uuid,
}

pub struct StrategyPostgresRepository {
    pool: sqlx::PgPool,
    producer_trace: ProducerTrace,
    virtual_clock: bool,
}

impl StrategyPostgresRepository {
    pub async fn connect(storage: &PostgresStorageProperties, producer_trace: ProducerTrace) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Ok(Self {
            pool: repository.get_pool().clone(),
            producer_trace,
            virtual_clock: false,
        })
    }

    pub fn with_replay_clock(mut self) -> Self {
        self.virtual_clock = true;
        self
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::new(self.pool.clone())
    }

    pub fn producer(identity: StrategyStorageIdentity<'_>) -> String {
        format!(
            "strategy:{}:{}:{}:{}",
            identity.scope.env, identity.scope.tenant_id, identity.workflow_id, identity.node_id
        )
    }

    pub async fn load(&self, identity: StrategyStorageIdentity<'_>) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT state FROM wf_checkpoint WHERE env=$1 AND tenant_id=$2
            AND workflow_id=$3 AND node_id=$4 AND run_id=$5 AND partition_key='decision'",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.workflow_id)
        .bind(identity.node_id)
        .bind(identity.run_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input_topic: &str,
        input: &DurableMessage<Value>,
        expected_version: i64,
        checkpoint_version: i64,
        checkpoint: Value,
        effects: Effects,
    ) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        let consumer = format!("strategy.{}", identity.node_id);
        let wire = MessageEnvelope::from_durable(input.clone())?;
        if !MessagerPostgresRepository::accept(&mut transaction, &consumer, input_topic, &wire).await? {
            transaction.commit().await?;
            return Ok(false);
        }
        ensure!(
            checkpoint_version == expected_version + 1,
            "checkpoint version must advance once"
        );
        let changed = sqlx::query("INSERT INTO wf_checkpoint(env,tenant_id,workflow_id,node_id,run_id,
            partition_key,seq,state,watermark,version) VALUES($1,$2,$3,$4,$5,'decision',$6,$7,$8,$9)
            ON CONFLICT(env,tenant_id,workflow_id,node_id,run_id,partition_key)
            DO UPDATE SET seq=EXCLUDED.seq,state=EXCLUDED.state,watermark=GREATEST(wf_checkpoint.watermark,EXCLUDED.watermark),
            version=EXCLUDED.version,updated_at=NOW() WHERE wf_checkpoint.version=$10")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.workflow_id)
            .bind(identity.node_id).bind(identity.run_id).bind(input.seq).bind(checkpoint).bind(input.available_time)
            .bind(checkpoint_version).bind(expected_version).execute(&mut *transaction).await?.rows_affected();
        ensure!(
            changed == 1,
            "strategy owner/checkpoint changed; retry from durable state"
        );
        for intent in effects.intents {
            self.output(
                &mut transaction,
                &identity,
                input,
                WorkflowTopic::StrategySignal,
                serde_json::to_value(&intent)?,
                Some(intent.account_id),
            )
            .await?;
        }
        for request in effects.requests {
            let (topic, payload) = match request {
                StrategyRequest::Snapshot(value) => (WorkflowTopic::FeedSnapshotRequest, serde_json::to_value(value)?),
                StrategyRequest::Compute(value) => (WorkflowTopic::ComputeRequest, serde_json::to_value(value)?),
                StrategyRequest::Notification(value) => {
                    (WorkflowTopic::NotificationRequest, serde_json::to_value(value)?)
                }
                StrategyRequest::Export(value) => (WorkflowTopic::ExportRequest, serde_json::to_value(value)?),
            };
            self.output(&mut transaction, &identity, input, topic, payload, input.account_id)
                .await?;
        }
        for timer in effects.timers {
            self.schedule_timer(&mut transaction, &identity, input, timer).await?;
        }
        MessagerPostgresRepository::complete(&mut transaction, &consumer, input).await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn schedule_timer(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        identity: &StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
        timer: StrategyTimer,
    ) -> Result<()> {
        if self.virtual_clock {
            let sequence = MessagerPostgresRepository::next_sequence(
                transaction,
                identity.scope,
                identity.node_id,
                identity.run_id,
            )
            .await?;
            sqlx::query(
                "INSERT INTO wf_timer(env,tenant_id,workflow_id,node_id,run_id,seq,scheduled_at,payload,traceparent)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            )
            .bind(&identity.scope.env)
            .bind(identity.scope.tenant_id)
            .bind(identity.workflow_id)
            .bind(identity.node_id)
            .bind(identity.run_id)
            .bind(sequence)
            .bind(timer.at)
            .bind(timer.payload)
            .bind(&input.traceparent)
            .execute(&mut **transaction)
            .await?;
            return Ok(());
        }
        let sequence = self
            .output(
                transaction,
                identity,
                input,
                WorkflowTopic::RuntimeEvent,
                serde_json::json!({"source":"timer","symbol":null,"event_time":timer.at,
                    "available_time":timer.at,"data":{"kind":"timer","value":{
                        "scheduled_at":timer.at,"payload":timer.payload}}}),
                input.account_id,
            )
            .await?;
        sqlx::query(
            "UPDATE msg_outbox SET available_at=$6 WHERE env=$1 AND tenant_id=$2
            AND producer=$7 AND node_id=$3 AND run_id=$4 AND seq=$5",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.node_id)
        .bind(identity.run_id)
        .bind(sequence)
        .bind(timer.at)
        .bind(Self::producer(Self::copy_identity(identity)))
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn output(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        identity: &StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
        topic: WorkflowTopic,
        payload: Value,
        account_id: Option<Uuid>,
    ) -> Result<i64> {
        let sequence =
            MessagerPostgresRepository::next_sequence(transaction, identity.scope, identity.node_id, identity.run_id)
                .await?;
        let request = matches!(
            topic,
            WorkflowTopic::FeedSnapshotRequest
                | WorkflowTopic::ComputeRequest
                | WorkflowTopic::NotificationRequest
                | WorkflowTopic::ExportRequest
        );
        let outgoing = DurableMessage {
            version: 2,
            scope: identity.scope.clone(),
            workflow_id: Some(identity.workflow_id),
            account_id,
            node_id: identity.node_id,
            target_node_id: None,
            run_id: identity.run_id.to_owned(),
            seq: sequence,
            attempt_no: 0,
            kind: if request {
                MessageKind::Request
            } else {
                MessageKind::Event
            },
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: if self.virtual_clock {
                input.available_time
            } else {
                Utc::now()
            },
            traceparent: (self.producer_trace)(&input.traceparent)?,
            payload,
        };
        MessagerPostgresRepository::enqueue(
            transaction,
            &Self::producer(Self::copy_identity(identity)),
            &topic.topic(identity.scope, identity.workflow_id)?,
            &outgoing,
        )
        .await?;
        Ok(sequence)
    }

    fn copy_identity<'a>(identity: &'a StrategyStorageIdentity<'a>) -> StrategyStorageIdentity<'a> {
        StrategyStorageIdentity {
            scope: identity.scope,
            workflow_id: identity.workflow_id,
            node_id: identity.node_id,
            run_id: identity.run_id,
        }
    }
}
