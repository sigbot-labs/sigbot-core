// SPDX-License-Identifier: GPL-3.0-only
//! Datafeed owns importing frozen historical facts into a separate simulation scope.
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::modules::messager::topic::MessageScope;
use sqlx::{Postgres, Row, Transaction};

pub struct ReplayImport<'a> {
    pub source_scope: &'a MessageScope,
    pub source_run: &'a str,
    pub target_scope: &'a MessageScope,
    pub target_run: &'a str,
    pub source: &'a str,
    pub source_event_id: &'a str,
    pub revision: i64,
    pub frozen_at: DateTime<Utc>,
    pub clock: DateTime<Utc>,
}

pub struct ReplayIngestor;

impl ReplayIngestor {
    pub async fn import(tx: &mut Transaction<'_, Postgres>, input: &ReplayImport<'_>) -> Result<()> {
        input.source_scope.validate()?;
        input.target_scope.validate()?;
        ensure!(
            input.source_scope != input.target_scope
                && input.source_run != input.target_run
                && (matches!(input.target_scope.env.as_str(), "backtest" | "replay")
                    || input.target_scope.env.starts_with("replay-")),
            "historical import requires a separate simulation scope and run"
        );
        ensure!(
            input.clock <= input.frozen_at && input.revision >= 0 && input.revision <= i32::MAX as i64,
            "invalid frozen historical boundary"
        );
        let source=sqlx::query("SELECT symbol,event_type,event_at,available_at,seq,payload FROM market_event
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND (run_id=$4 OR EXISTS(SELECT 1 FROM market_event_delivery d WHERE d.env=market_event.env
                AND d.tenant_id=market_event.tenant_id AND d.space_id=market_event.space_id AND d.run_id=$4 AND d.source=market_event.source
                AND d.source_event_id=market_event.source_event_id AND d.revision=market_event.revision AND d.created_at<=$8)) AND source=$5 AND source_event_id=$6 AND revision=$7
            AND recorded_at <= $8 AND available_at <= $9")
            .bind(&input.source_scope.env).bind(input.source_scope.tenant_id).bind(input.source_scope.space_id)
            .bind(input.source_run).bind(input.source).bind(input.source_event_id).bind(input.revision as i32)
            .bind(input.frozen_at).bind(input.clock).fetch_one(&mut **tx).await?;
        let payload: Value = source.get("payload");
        sqlx::query("INSERT INTO market_event(env,tenant_id,space_id,run_id,source,source_event_id,revision,symbol,event_type,event_at,available_at,seq,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT DO NOTHING")
            .bind(&input.target_scope.env).bind(input.target_scope.tenant_id).bind(input.target_scope.space_id).bind(input.target_run)
            .bind(input.source).bind(input.source_event_id).bind(input.revision as i32).bind(source.get::<String,_>("symbol"))
            .bind(source.get::<String,_>("event_type")).bind(source.get::<DateTime<Utc>,_>("event_at"))
            .bind(source.get::<DateTime<Utc>,_>("available_at")).bind(source.get::<i64,_>("seq")).bind(&payload).execute(&mut **tx).await?;
        let stored=sqlx::query("SELECT run_id,payload,event_at,available_at FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND source=$4 AND source_event_id=$5 AND revision=$6")
            .bind(&input.target_scope.env).bind(input.target_scope.tenant_id).bind(input.target_scope.space_id)
            .bind(input.source).bind(input.source_event_id).bind(input.revision as i32).fetch_one(&mut **tx).await?;
        ensure!(
            stored.get::<String, _>("run_id") == input.target_run
                && stored.get::<Value, _>("payload") == payload
                && stored.get::<DateTime<Utc>, _>("event_at") == source.get::<DateTime<Utc>, _>("event_at")
                && stored.get::<DateTime<Utc>, _>("available_at") == source.get::<DateTime<Utc>, _>("available_at"),
            "replay fact identity collision"
        );
        Ok(())
    }
}
