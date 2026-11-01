// SPDX-License-Identifier: GPL-3.0-only
// Bounded historical event and evaluation queries for deterministic replay.

use crate::modules::backtest::storage::postgres::BacktestTransaction;
use crate::modules::datafeed::storage::postgres::DatafeedPostgresRepository;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::backtest::{HistoricalRecord, HistoryCursor, ReplayCheckpoint, ReplayHistory, ReplayIdentity};
use sigbot_types::datafeed::event::HistoricalImport;
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::messager::topic::{MessageScope, WorkflowTopic};
use sqlx::Row;
use uuid::Uuid;

pub struct HistoryQuery<'a> {
    pub scope: &'a MessageScope,
    pub run_id: &'a Uuid,
    pub workflow_id: Uuid,
    pub sources: &'a [String],
    pub from: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub frozen_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct BacktestHistoryPostgresRepository {
    pool: sqlx::PgPool,
}

impl BacktestHistoryPostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?,
        })
    }

    async fn query_page(
        &self,
        query: HistoryQuery<'_>,
        after: Option<&HistoryCursor>,
        limit: u32,
    ) -> Result<Vec<HistoricalRecord>> {
        ensure!((1..=1024).contains(&limit), "replay page exceeds bound");
        let beginning = HistoryCursor {
            available_at: query.from,
            priority: -1,
            source: String::new(),
            source_event_id: String::new(),
            revision: -1,
        };
        let cursor = after.unwrap_or(&beginning);
        let rows = sqlx::query("WITH history AS (
            SELECT available_at,CASE WHEN event_type='market' THEN 2 ELSE 3 END priority,source,source_event_id event_key,
                revision::bigint,event_at,event_type,COALESCE((SELECT d.payload->'payload' FROM mkt_delivery d
                WHERE d.env=mkt_event.env AND d.tenant_id=mkt_event.tenant_id
                AND d.run_id=$3 AND d.source=mkt_event.source AND d.source_event_id=mkt_event.source_event_id
                AND d.revision=mkt_event.revision AND d.created_at<=$7 ORDER BY d.node_id LIMIT 1),
                jsonb_build_object('source',source,'source_event_id',source_event_id,'symbol',symbol,'event_type',event_type,
                'event_time',event_at,'available_time',available_at,'seq',seq,'content',COALESCE(payload->>'text',payload->>'content'),'raw',payload)) payload
            FROM mkt_event WHERE env=$1 AND tenant_id=$2 AND (run_id=$3 OR EXISTS(SELECT 1 FROM mkt_delivery d
                WHERE d.env=mkt_event.env AND d.tenant_id=mkt_event.tenant_id AND d.run_id=$3
                AND d.source=mkt_event.source AND d.source_event_id=mkt_event.source_event_id AND d.revision=mkt_event.revision
                AND d.created_at<=$7)) AND source=ANY($4) AND available_at >= $5 AND available_at <= $6 AND recorded_at <= $7
                AND event_type IN ('market','news') UNION ALL
            SELECT available_at,0,'evaluation:'||node_id::text,run_id::text,version,as_of,'evaluation',
                jsonb_build_object('version',version,'as_of',as_of,'valid_until',valid_until,'probabilities',probabilities,
                'parameters',parameters,'model_version',model_version,'classification_version',classification_version,'case_refs',case_refs)
            FROM eval_snapshot WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND workflow_id=$8
                AND available_at >= $5 AND available_at <= $6 AND recorded_at <= $7)
            SELECT available_at,priority,source,event_key,revision,event_at,event_type,
                CASE WHEN octet_length(payload::text)<=65536 THEN payload END payload FROM history
            WHERE (available_at,priority,source,event_key,revision)>($9,$10,$11,$12,$13)
            ORDER BY available_at,priority,source,event_key,revision LIMIT $14")
            .bind(&query.scope.env).bind(query.scope.tenant_id).bind(query.run_id)
            .bind(query.sources).bind(query.from).bind(query.until).bind(query.frozen_at).bind(query.workflow_id)
            .bind(cursor.available_at).bind(cursor.priority).bind(&cursor.source).bind(&cursor.source_event_id)
            .bind(cursor.revision).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let topic = match row.get::<String, _>("event_type").as_str() {
                    "evaluation" => WorkflowTopic::EvaluationSnapshot,
                    "market" => WorkflowTopic::FeedMarket,
                    "news" => WorkflowTopic::FeedNews,
                    _ => anyhow::bail!("unsupported historical event"),
                };
                Ok(HistoricalRecord {
                    cursor: HistoryCursor {
                        available_at: row.get("available_at"),
                        priority: row.get("priority"),
                        source: row.get("source"),
                        source_event_id: row.get("event_key"),
                        revision: row.get("revision"),
                    },
                    event_time: row.get("event_at"),
                    topic,
                    payload: row
                        .get::<Option<Value>, _>("payload")
                        .ok_or_else(|| anyhow::anyhow!("historical record exceeds 64 KiB"))?,
                })
            })
            .collect()
    }

    pub async fn page(
        &self,
        history: ReplayHistory<'_>,
        after: Option<&HistoryCursor>,
        limit: u32,
    ) -> Result<Vec<HistoricalRecord>> {
        self.query_page(
            HistoryQuery {
                scope: history.scope,
                run_id: history.run_id,
                workflow_id: history.workflow_id,
                sources: history.sources,
                from: history.from,
                until: history.until,
                frozen_at: history.frozen_at,
            },
            after,
            limit,
        )
        .await
    }

    pub async fn restore(
        &self,
        identity: ReplayIdentity<'_>,
        configuration: &Value,
        frozen_at: DateTime<Utc>,
        from: DateTime<Utc>,
    ) -> Result<ReplayCheckpoint> {
        sqlx::query(
            "INSERT INTO bt_checkpoint(env,tenant_id,run_id,configuration,frozen_at,virtual_time)
            VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.run_id)
        .bind(configuration)
        .bind(frozen_at)
        .bind(from)
        .execute(&self.pool)
        .await?;
        let row = sqlx::query("SELECT configuration,cursor,virtual_time,steps FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2 AND run_id=$3")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.run_id).fetch_one(&self.pool).await?;
        ensure!(
            row.get::<Value, _>("configuration") == *configuration,
            "replay restart changed its frozen configuration"
        );
        let timers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM wf_timer WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND state='delivered'",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.run_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(ReplayCheckpoint {
            cursor: row
                .get::<Option<Value>, _>("cursor")
                .map(serde_json::from_value)
                .transpose()?,
            virtual_time: row.get("virtual_time"),
            steps: u64::try_from(row.get::<i64, _>("steps"))?,
            timer_firings: timers.try_into()?,
        })
    }

    pub async fn import_records(
        &self,
        identity: ReplayIdentity<'_>,
        history: ReplayHistory<'_>,
        records: &[HistoricalRecord],
        clock: DateTime<Utc>,
    ) -> Result<()> {
        let mut transaction = BacktestTransaction::begin(&self.pool).await?;
        for record in records
            .iter()
            .filter(|record| record.topic == WorkflowTopic::FeedMarket)
        {
            Self::import_in(
                &mut transaction,
                identity.scope,
                identity.run_id,
                &history,
                record,
                clock,
            )
            .await?;
        }
        transaction.commit().await
    }

    pub(crate) async fn import_in(
        transaction: &mut BacktestTransaction,
        target_scope: &MessageScope,
        target_run: &Uuid,
        history: &ReplayHistory<'_>,
        record: &HistoricalRecord,
        clock: DateTime<Utc>,
    ) -> Result<()> {
        DatafeedPostgresRepository::replay_in(
            &mut transaction.inner,
            &HistoricalImport {
                mode: ExecutionMode::Backtest,
                source_scope: history.scope,
                source_run: history.run_id,
                target_scope,
                target_run,
                source: &record.cursor.source,
                source_event_id: &record.cursor.source_event_id,
                revision: record.cursor.revision,
                frozen_at: history.frozen_at,
                clock,
            },
        )
        .await
    }
}
