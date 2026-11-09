// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::backtest::storage::postgres::history::BacktestHistoryPostgresRepository;
use crate::modules::backtest::storage::postgres::BacktestTransaction;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_types::backtest::{HistoricalRecord, ReplayCheckpoint, ReplayHistory, ReplayIdentity};
use sigbot_types::messager::envelope::{DurableMessage, MessageKind};
use sigbot_types::messager::topic::WorkflowTopic;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct ReplayDelivery {
    pub transaction: BacktestTransaction,
    pub message: DurableMessage<Value>,
}

pub struct BacktestReplayPostgresRepository {
    pool: sqlx::PgPool,
    owner: Mutex<Option<sqlx::PgConnection>>,
}

impl BacktestReplayPostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            owner: Mutex::new(None),
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::new(self.pool.clone())
    }

    pub async fn acquire_owner(&self, identity: ReplayIdentity<'_>, replay_of: &Uuid) -> Result<()> {
        let mut owner = self.owner.lock().await;
        ensure!(owner.is_none(), "replay owner already acquired");
        let mut connection = self.pool.acquire().await?.detach();
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text,0))")
            .bind(identity.run_id)
            .fetch_one(&mut connection)
            .await?;
        ensure!(locked, "another replay owner already holds this run");
        let binding: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM wf_run WHERE run_id=$1 AND workflow_id=$2
            AND env=$3 AND tenant_id=$4 AND replay_of=$5)",
        )
        .bind(identity.run_id)
        .bind(identity.workflow_id)
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(replay_of)
        .fetch_one(&self.pool)
        .await?;
        ensure!(
            binding,
            "replay run must be explicitly bound to its historical source before startup"
        );
        *owner = Some(connection);
        Ok(())
    }

    pub async fn release_owner(&self, run_id: &Uuid) {
        if let Some(mut owner) = self.owner.lock().await.take() {
            let _ = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1::text,0))")
                .bind(run_id)
                .execute(&mut owner)
                .await;
        }
    }

    pub async fn assert_owner(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(self.owner.lock().await.as_mut().context("replay owner lease missing")?)
            .await?;
        Ok(())
    }

    pub async fn checkpoint(
        &self,
        identity: ReplayIdentity<'_>,
        progress: ReplayCheckpoint,
        complete: bool,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE bt_checkpoint SET cursor=$4,virtual_time=$5,steps=$6,state=$7,pending_message=NULL
            WHERE env=$1 AND tenant_id=$2 AND run_id=$3",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.run_id)
        .bind(progress.cursor.as_ref().map(serde_json::to_value).transpose()?)
        .bind(progress.virtual_time)
        .bind(i64::try_from(progress.steps)?)
        .bind(if complete { "complete" } else { "running" })
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn begin_delivery(
        &self,
        identity: ReplayIdentity<'_>,
        history: ReplayHistory<'_>,
        record: &HistoricalRecord,
        node_id: i64,
        producer: &str,
        clock: DateTime<Utc>,
    ) -> Result<ReplayDelivery> {
        let mut transaction = BacktestTransaction::begin(&self.pool).await?;
        let pending: Option<Value> = sqlx::query_scalar(
            "SELECT pending_message FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND state='running' FOR UPDATE",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.run_id)
        .fetch_one(&mut *transaction.inner)
        .await?;
        if matches!(record.topic, WorkflowTopic::FeedMarket | WorkflowTopic::FeedNews) {
            BacktestHistoryPostgresRepository::import_in(
                &mut transaction,
                identity.scope,
                identity.run_id,
                &history,
                record,
                clock,
            )
            .await?;
        }
        sqlx::query("UPDATE bt_checkpoint SET virtual_time=$4 WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND virtual_time<=$4 AND state='running'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.run_id)
            .bind(clock).execute(&mut *transaction.inner).await?;
        let message = if let Some(pending) = pending {
            ensure!(
                pending.get("cursor") == Some(&serde_json::to_value(&record.cursor)?),
                "replay admission differs from its pending historical fact"
            );
            serde_json::from_value(
                pending
                    .get("message")
                    .cloned()
                    .context("pending replay message missing")?,
            )?
        } else {
            DurableMessage {
                version: 2,
                scope: identity.scope.clone(),
                workflow_id: Some(identity.workflow_id),
                account_id: None,
                node_id,
                target_node_id: None,
                run_id: identity.run_id.to_owned(),
                seq: MessagerPostgresRepository::next_sequence(
                    &mut transaction.inner,
                    identity.scope,
                    node_id,
                    identity.run_id,
                )
                .await?,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: record.event_time,
                available_time: clock,
                produced_at: clock,
                traceparent: MessageTrace::child("backtest.history", "").traceparent(),
                payload: record.payload.clone(),
            }
        };
        sqlx::query("UPDATE bt_checkpoint SET pending_message=$4 WHERE env=$1 AND tenant_id=$2 AND run_id=$3")
            .bind(&identity.scope.env)
            .bind(identity.scope.tenant_id)
            .bind(identity.run_id)
            .bind(serde_json::json!({"cursor":record.cursor,"message":message}))
            .execute(&mut *transaction.inner)
            .await?;
        MessagerPostgresRepository::enqueue(
            &mut transaction.inner,
            producer,
            &record.topic.topic(identity.scope, identity.workflow_id)?,
            &message,
        )
        .await?;
        Ok(ReplayDelivery { transaction, message })
    }
}
