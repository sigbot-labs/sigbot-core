// SPDX-License-Identifier: GPL-3.0-only
//! Keyset reads of a frozen dataset; no model call exists in historical replay.
use super::replay::{ReplayCursor, ReplayRecord, ReplaySource};
use anyhow::{ensure, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sigbot_types::modules::messager::topic::{MessageScope, WorkflowTopic};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayHistoryConfig {
    pub scope: MessageScope,
    pub run_id: String,
    pub workflow_id: i64,
    pub sources: Vec<String>,
    pub from: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub frozen_at: DateTime<Utc>,
}

pub struct PostgresReplaySource {
    pool: PgPool,
    config: ReplayHistoryConfig,
}

impl PostgresReplaySource {
    pub fn new(pool: PgPool, config: ReplayHistoryConfig) -> Result<Self> {
        config.scope.validate()?;
        ensure!(
            !config.run_id.is_empty()
                && config.workflow_id > 0
                && !config.sources.is_empty()
                && config.sources.len() <= 32,
            "historical run, workflow and bounded sources are required"
        );
        ensure!(
            config.from < config.until && config.until <= config.frozen_at && config.frozen_at <= Utc::now(),
            "invalid frozen historical interval"
        );
        Ok(Self { pool, config })
    }
}

#[async_trait]
impl ReplaySource for PostgresReplaySource {
    async fn page(&self, after: Option<&ReplayCursor>, limit: u32) -> Result<Vec<ReplayRecord>> {
        ensure!((1..=1024).contains(&limit), "replay page exceeds bound");
        let beginning = ReplayCursor {
            available_at: self.config.from,
            priority: -1,
            source: String::new(),
            source_event_id: String::new(),
            revision: -1,
        };
        let cursor = after.unwrap_or(&beginning);
        let rows=sqlx::query("WITH history AS (
            SELECT available_at,CASE WHEN event_type='market' THEN 2 ELSE 3 END priority,source,source_event_id event_key,
                revision::bigint,event_at,event_type,
                COALESCE((SELECT d.payload->'payload' FROM market_event_delivery d WHERE d.env=market_event.env AND d.tenant_id=market_event.tenant_id
                    AND d.space_id=market_event.space_id AND d.run_id=$4 AND d.source=market_event.source AND d.source_event_id=market_event.source_event_id
                    AND d.revision=market_event.revision AND d.created_at<=$8 ORDER BY d.node_id LIMIT 1),
                jsonb_build_object('source',source,'source_event_id',source_event_id,'symbol',symbol,'event_type',event_type,
                    'event_time',event_at,'available_time',available_at,'seq',seq,'content',COALESCE(payload->>'text',payload->>'content'),'raw',payload)) payload
            FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND (run_id=$4 OR EXISTS(SELECT 1 FROM market_event_delivery d
                WHERE d.env=market_event.env AND d.tenant_id=market_event.tenant_id AND d.space_id=market_event.space_id
                AND d.run_id=$4 AND d.source=market_event.source AND d.source_event_id=market_event.source_event_id AND d.revision=market_event.revision AND d.created_at<=$8)) AND source=ANY($5)
                AND available_at >= $6 AND available_at <= $7 AND recorded_at <= $8 AND event_type IN ('market','news')
            UNION ALL
            SELECT available_at,0,'evaluation:'||node_id::text,run_id,version,as_of,'evaluation',
                jsonb_build_object('version',version,'as_of',as_of,'valid_until',valid_until,'probabilities',probabilities,
                    'parameters',parameters,'model_version',model_version,'classification_version',classification_version,'case_refs',case_refs)
            FROM evaluation_snapshot WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND workflow_id=$9
                AND available_at >= $6 AND available_at <= $7 AND recorded_at <= $8)
            SELECT available_at,priority,source,event_key,revision,event_at,event_type,
                CASE WHEN octet_length(payload::text)<=65536 THEN payload END payload
            FROM history WHERE (available_at,priority,source,event_key,revision)>($10,$11,$12,$13,$14)
            ORDER BY available_at,priority,source,event_key,revision LIMIT $15")
            .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(self.config.scope.space_id)
            .bind(&self.config.run_id).bind(&self.config.sources).bind(self.config.from).bind(self.config.until).bind(self.config.frozen_at)
            .bind(self.config.workflow_id).bind(cursor.available_at).bind(cursor.priority).bind(&cursor.source)
            .bind(&cursor.source_event_id).bind(cursor.revision).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let payload: Option<serde_json::Value> = row.get("payload");
                let topic = match row.get::<String, _>("event_type").as_str() {
                    "evaluation" => WorkflowTopic::EvaluationSnapshot,
                    "market" => WorkflowTopic::FeedMarket,
                    "news" => WorkflowTopic::FeedNews,
                    _ => anyhow::bail!("unsupported historical event"),
                };
                Ok(ReplayRecord {
                    cursor: ReplayCursor {
                        available_at: row.get("available_at"),
                        priority: row.get("priority"),
                        source: row.get("source"),
                        source_event_id: row.get("event_key"),
                        revision: row.get("revision"),
                    },
                    event_time: row.get("event_at"),
                    topic,
                    payload: payload.ok_or_else(|| anyhow::anyhow!("historical record exceeds 64 KiB"))?,
                })
            })
            .collect()
    }
}
