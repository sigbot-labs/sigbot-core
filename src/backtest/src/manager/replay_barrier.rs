// SPDX-License-Identifier: GPL-3.0-only
//! A repeatable-read barrier observes recipient commits and their outgoing effects.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sigbot_types::modules::messager::topic::{MessageScope, TopicAddress};
use sqlx::{PgPool, Row};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaySubscription {
    pub producer_node_id: i64,
    pub target_node_id: i64,
    pub topic: String,
    /// Exact Inbox consumer name used by that configured participant.
    pub consumer: String,
    #[serde(default)]
    pub payload_kind: ReplayPayloadKind,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPayloadKind {
    #[default]
    Any,
    Execution,
    Funding,
    OrderStatus,
}

impl ReplayPayloadKind {
    fn matches(self, payload: &serde_json::Value) -> bool {
        match self {
            Self::Any => true,
            Self::Execution => payload.get("exchange_trade_id").is_some(),
            Self::Funding => payload.get("source_event_id").is_some(),
            Self::OrderStatus => payload.get("receipt").is_some(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayBarrierConfig {
    pub scope: MessageScope,
    pub run_id: String,
    pub subscriptions: Vec<ReplaySubscription>,
    /// Explicit terminal outputs without another replay participant, such as an
    /// archived notification. Financial transitions still require their consumers.
    pub terminal_topics: Vec<String>,
    pub max_messages: u32,
    pub timeout_seconds: u64,
}

impl ReplayBarrierConfig {
    /// Normalize only the historical owner spelling; frozen replay semantics stay unchanged.
    pub fn migrate_legacy_owners(&mut self, workflow_id: i64) -> Result<()> {
        ensure!(workflow_id > 0, "replay Workflow identity required");
        self.scope.validate()?;
        for route in &mut self.subscriptions {
            for prefix in ["order", "order.execution", "order.status", "export"] {
                let old = format!(
                    "{prefix}:{}:{}:{}:{}",
                    self.scope.env, self.scope.tenant_id, self.scope.space_id, route.target_node_id
                );
                if let Some(suffix) = route.consumer.strip_prefix(&old) {
                    if matches!(suffix, "" | ":reservation" | ":venue") {
                        route.consumer = format!(
                            "{prefix}:{}:{}:{}:{workflow_id}:{}{suffix}",
                            self.scope.env, self.scope.tenant_id, self.scope.space_id, route.target_node_id
                        );
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct PostgresReplayBarrier {
    pool: PgPool,
    config: ReplayBarrierConfig,
}

impl PostgresReplayBarrier {
    pub fn new(pool: PgPool, config: ReplayBarrierConfig) -> Result<Self> {
        config.scope.validate()?;
        ensure!(
            !config.run_id.is_empty()
                && !config.subscriptions.is_empty()
                && config.subscriptions.len() <= 128
                && (1..=1_000_000).contains(&config.max_messages)
                && (1..=300).contains(&config.timeout_seconds),
            "invalid replay barrier bounds"
        );
        for route in &config.subscriptions {
            ensure!(
                route.producer_node_id > 0 && route.target_node_id > 0 && !route.consumer.is_empty(),
                "incomplete replay participant"
            );
            ensure!(
                TopicAddress::parse(&route.topic)?.scope() == &config.scope,
                "replay route crosses isolation scope"
            );
        }
        for topic in &config.terminal_topics {
            ensure!(
                TopicAddress::parse(topic)?.scope() == &config.scope,
                "terminal topic crosses scope"
            );
        }
        Ok(Self { pool, config })
    }

    pub async fn wait(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_seconds);
        loop {
            if self.settled().await? {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "replay clock barrier timed out waiting for durable recipients"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn settled(&self) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let mut cursor = (0_i64, -1_i64);
        let mut inspected = 0_u32;
        loop {
            let rows=sqlx::query("SELECT node_id,seq,topic,state,(payload->>'target_node_id')::bigint target_node_id,payload->'payload' body FROM messager_outbox
                WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND (node_id,seq)>($5,$6)
                ORDER BY node_id,seq LIMIT 256")
                .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(self.config.scope.space_id).bind(&self.config.run_id)
                .bind(cursor.0).bind(cursor.1).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                inspected += 1;
                ensure!(
                    inspected <= self.config.max_messages,
                    "replay graph exceeded message bound"
                );
                let node: i64 = row.get("node_id");
                let seq: i64 = row.get("seq");
                let topic: String = row.get("topic");
                let state: String = row.get("state");
                let target: Option<i64> = row.get("target_node_id");
                let body: serde_json::Value = row.get("body");
                ensure!(state != "quarantined", "replay output was quarantined");
                if state != "sent" {
                    return Ok(false);
                }
                let routes: Vec<_> = self
                    .config
                    .subscriptions
                    .iter()
                    .filter(|route| {
                        route.producer_node_id == node
                            && route.topic == topic
                            && target.is_none_or(|target| target == route.target_node_id)
                            && route.payload_kind.matches(&body)
                    })
                    .collect();
                if routes.is_empty() {
                    ensure!(
                        self.config.terminal_topics.contains(&topic),
                        "replay output has no declared recipient or terminal boundary"
                    );
                }
                for route in routes {
                    let received: Option<String> = sqlx::query_scalar(
                        "SELECT state FROM messager_inbox WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                        AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7",
                    )
                    .bind(&self.config.scope.env)
                    .bind(self.config.scope.tenant_id)
                    .bind(self.config.scope.space_id)
                    .bind(&route.consumer)
                    .bind(node)
                    .bind(&self.config.run_id)
                    .bind(seq)
                    .fetch_optional(&mut *tx)
                    .await?;
                    ensure!(
                        received.as_deref() != Some("quarantined"),
                        "replay participant quarantined its input"
                    );
                    if received.as_deref() != Some("done") {
                        return Ok(false);
                    }
                }
                cursor = (node, seq);
            }
        }
        // Recipient completion and every child Outbox commit are in one transaction;
        // this snapshot therefore cannot miss a child after observing its parent done.
        let unsettled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM order_record WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4
            AND state IN ('pending','reserved','submitting','unknown'))",
        )
        .bind(&self.config.scope.env)
        .bind(self.config.scope.tenant_id.to_string())
        .bind(self.config.scope.space_id)
        .bind(&self.config.run_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(!unsettled)
    }
}
