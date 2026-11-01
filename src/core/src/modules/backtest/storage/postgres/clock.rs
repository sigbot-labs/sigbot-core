// SPDX-License-Identifier: GPL-3.0-only
// Backtest clock and deterministic barrier persistence.

use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_types::backtest::ReplayIdentity;
use sigbot_types::messager::envelope::{DurableMessage, MessageKind};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sqlx::Row;
use uuid::Uuid;

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;

#[derive(Clone, Copy)]
pub enum BarrierPayloadKind {
    Any,
    Execution,
    Funding,
    OrderStatus,
}

impl BarrierPayloadKind {
    fn matches(self, payload: &Value) -> bool {
        match self {
            Self::Any => true,
            Self::Execution => payload.get("exchange_trade_id").is_some(),
            Self::Funding => payload.get("source_event_id").is_some(),
            Self::OrderStatus => payload.get("receipt").is_some(),
        }
    }
}

pub struct BarrierRoute {
    pub producer_node_id: i64,
    pub target_node_id: i64,
    pub topic: String,
    pub consumer: String,
    pub payload_kind: BarrierPayloadKind,
}

pub struct BarrierQuery<'a> {
    pub scope: &'a MessageScope,
    pub run_id: &'a Uuid,
    pub routes: &'a [BarrierRoute],
    pub terminal_topics: &'a [String],
    pub max_messages: u32,
}

#[derive(Clone)]
pub struct BacktestClockPostgresRepository {
    pool: sqlx::PgPool,
}

impl BacktestClockPostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(database_url)
                .await?,
        })
    }

    pub async fn settled(&self, query: BarrierQuery<'_>) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let mut cursor = (0_i64, -1_i64);
        let mut inspected = 0_u32;
        loop {
            let rows = sqlx::query("SELECT node_id,seq,topic,state,(payload->>'target_node_id')::bigint target_node_id,payload->'payload' body
                FROM msg_outbox WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND (node_id,seq)>($4,$5)
                ORDER BY node_id,seq LIMIT 256")
                .bind(&query.scope.env).bind(query.scope.tenant_id).bind(query.run_id)
                .bind(cursor.0).bind(cursor.1).fetch_all(&mut *transaction).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                inspected += 1;
                ensure!(inspected <= query.max_messages, "replay graph exceeded message bound");
                let node: i64 = row.get("node_id");
                let sequence: i64 = row.get("seq");
                let topic: String = row.get("topic");
                let state: String = row.get("state");
                let target: Option<i64> = row.get("target_node_id");
                let body: Value = row.get("body");
                ensure!(state != "quarantined", "replay output was quarantined");
                if state != "sent" {
                    return Ok(false);
                }
                let routes: Vec<_> = query
                    .routes
                    .iter()
                    .filter(|route| {
                        route.producer_node_id == node
                            && route.topic == topic
                            && target.is_none_or(|value| value == route.target_node_id)
                            && route.payload_kind.matches(&body)
                    })
                    .collect();
                if routes.is_empty() {
                    ensure!(
                        query.terminal_topics.contains(&topic),
                        "replay output has no declared recipient or terminal boundary"
                    );
                }
                for route in routes {
                    let received: Option<String> = sqlx::query_scalar(
                        "SELECT state FROM msg_inbox WHERE env=$1 AND tenant_id=$2
                        AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
                    )
                    .bind(&query.scope.env)
                    .bind(query.scope.tenant_id)
                    .bind(&route.consumer)
                    .bind(node)
                    .bind(query.run_id)
                    .bind(sequence)
                    .fetch_optional(&mut *transaction)
                    .await?;
                    ensure!(
                        received.as_deref() != Some("quarantined"),
                        "replay participant quarantined its input"
                    );
                    if received.as_deref() != Some("done") {
                        return Ok(false);
                    }
                }
                cursor = (node, sequence);
            }
        }
        let unsettled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ord_order WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND state IN ('pending','reserved','submitting','unknown'))",
        )
        .bind(&query.scope.env)
        .bind(query.scope.tenant_id)
        .bind(query.run_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(!unsettled)
    }

    pub async fn next_timer(&self, identity: ReplayIdentity<'_>) -> Result<Option<DateTime<Utc>>> {
        Ok(sqlx::query_scalar(
            "SELECT MIN(scheduled_at) FROM wf_timer WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND state='pending'",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.run_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn fire_timers(&self, identity: ReplayIdentity<'_>, clock: DateTime<Utc>, limit: u32) -> Result<u32> {
        let mut transaction = self.pool.begin().await?;
        let timers = sqlx::query("SELECT node_id,seq,scheduled_at,payload,traceparent FROM wf_timer WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND state='pending' AND scheduled_at<=$4 ORDER BY scheduled_at,node_id,seq LIMIT $5 FOR UPDATE SKIP LOCKED")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.run_id).bind(clock)
            .bind(i64::from(limit)).fetch_all(&mut *transaction).await?;
        sqlx::query("UPDATE bt_checkpoint SET virtual_time=$4 WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND virtual_time<=$4 AND state='running'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.run_id).bind(clock)
            .execute(&mut *transaction).await?;
        for timer in &timers {
            let node: i64 = timer.get("node_id");
            let sequence: i64 = timer.get("seq");
            let at: DateTime<Utc> = timer.get("scheduled_at");
            let message = DurableMessage {
                version: 2,
                scope: identity.scope.clone(),
                workflow_id: Some(identity.workflow_id),
                account_id: None,
                node_id: node,
                target_node_id: Some(node),
                run_id: identity.run_id.to_owned(),
                seq: sequence,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: at,
                available_time: at,
                produced_at: clock,
                traceparent: MessageTrace::child("backtest.timer", timer.get::<String, _>("traceparent").as_str())
                    .traceparent(),
                payload: serde_json::json!({"source":"timer","symbol":null,"event_time":at,"available_time":at,
                    "data":{"kind":"timer","value":{"scheduled_at":at,"payload":timer.get::<Value,_>("payload")}}}),
            };
            let producer = format!(
                "strategy:{}:{}:{}:{node}",
                identity.scope.env, identity.scope.tenant_id, identity.workflow_id
            );
            MessagerPostgresRepository::enqueue(
                &mut transaction,
                &producer,
                &WorkflowTopic::RuntimeEvent.topic(identity.scope, identity.workflow_id)?,
                &message,
            )
            .await?;
            sqlx::query("UPDATE wf_timer SET state='delivered' WHERE env=$1 AND tenant_id=$2 AND node_id=$3 AND run_id=$4 AND seq=$5")
                .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(node).bind(identity.run_id)
                .bind(sequence).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(timers.len().try_into()?)
    }
}
