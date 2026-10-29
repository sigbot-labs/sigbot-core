// SPDX-License-Identifier: GPL-3.0-only
//! Whitelisted lazy plans execute inside PostgreSQL, returning bounded results only.
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::modules::{
    messager::topic::MessageScope,
    strategy::runtime::{Aggregation, ComputePlan, ModelPartitioning, StrategyEventData},
};
use sqlx::{Postgres, Transaction};
use std::collections::HashSet;

pub struct ComputeExecutor;

impl ComputeExecutor {
    pub fn validate(plan: &ComputePlan, now: DateTime<Utc>) -> Result<()> {
        plan.validate(now)?;
        ensure!(plan.window_ms <= 31_536_000_000, "compute window exceeds one year");
        ensure!(
            (plan.as_of - plan.history_from).num_days() <= 366,
            "history warming range exceeds one year"
        );
        ensure!(
            !plan.aggregations.is_empty() && plan.aggregations.len() <= 7,
            "compute requires bounded aggregations"
        );
        let groups: HashSet<_> = plan.group_by.iter().map(String::as_str).collect();
        ensure!(
            groups.len() == plan.group_by.len() && groups.iter().all(|key| ["source", "symbol"].contains(key)),
            "unsupported grouping dimension"
        );
        let aggregations: HashSet<_> = plan
            .aggregations
            .iter()
            .map(Self::aggregation)
            .map(|(name, _)| name)
            .collect();
        ensure!(aggregations.len() == plan.aggregations.len(), "duplicate aggregation");
        if let Some(model) = &plan.predict {
            ensure!(
                model.model_version == "mean-price-v1",
                "model is not registered; arbitrary Python prediction is never auto-parallelized"
            );
            match &model.partitioning {
                ModelPartitioning::SingleOwner => {}
                ModelPartitioning::IndependentSymbols => ensure!(
                    groups.contains("symbol"),
                    "independent-symbol model requires symbol grouping"
                ),
                ModelPartitioning::Mergeable { reducer_version } => {
                    ensure!(reducer_version == "sum-count-v1", "unregistered prediction reducer");
                }
            }
        }
        Ok(())
    }

    pub async fn execute(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        plan: &ComputePlan,
        now: DateTime<Utc>,
    ) -> Result<StrategyEventData> {
        Self::validate(plan, now)?;
        // Prevent a valid but expensive history request monopolizing database resources.
        sqlx::query("SET LOCAL statement_timeout='5s'")
            .execute(&mut **tx)
            .await?;
        sqlx::query("SET LOCAL work_mem='4MB'").execute(&mut **tx).await?;
        let timescale: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')")
                .fetch_one(&mut **tx)
                .await?;
        let sql = Self::statement(plan, timescale);
        let rows: Vec<Value> = sqlx::query_scalar(&sql)
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .bind(scope.space_id)
            .bind(&plan.sources)
            .bind(&plan.symbols)
            .bind(plan.history_from)
            .bind(plan.as_of)
            .bind(plan.window_ms as f64 / 1000.0)
            .bind(i64::from(plan.max_rows) + 1)
            .fetch_all(&mut **tx)
            .await?;
        ensure!(
            rows.len() <= plan.max_rows as usize,
            "compute result row bound exceeded; narrow history or widen window"
        );
        ensure!(
            serde_json::to_vec(&rows)?.len() <= plan.max_bytes as usize,
            "compute result byte bound exceeded"
        );
        let watermark: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT MAX(event_at) FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND event_type='market' AND source=ANY($4) AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7")
            .bind(&scope.env).bind(scope.tenant_id).bind(scope.space_id).bind(&plan.sources).bind(&plan.symbols)
            .bind(plan.history_from).bind(plan.as_of).fetch_one(&mut **tx).await?;
        let revision: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(revision),0) FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND event_type='market' AND source=ANY($4) AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7")
            .bind(&scope.env).bind(scope.tenant_id).bind(scope.space_id).bind(&plan.sources).bind(&plan.symbols)
            .bind(plan.history_from).bind(plan.as_of).fetch_one(&mut **tx).await?;
        Ok(StrategyEventData::Compute {
            rows,
            watermark: watermark.unwrap_or(plan.history_from),
            revision: revision.try_into()?,
        })
    }

    pub fn statement(plan: &ComputePlan, timescale: bool) -> String {
        let bucket = if timescale {
            "time_bucket(make_interval(secs=>$8),event_at,TIMESTAMPTZ '1970-01-01 00:00:00+00')"
        } else {
            "date_bin(make_interval(secs=>$8),event_at,TIMESTAMPTZ '1970-01-01 00:00:00+00')"
        };
        // All interpolated identifiers and SQL fragments come from the whitelist;
        // every source, instrument, time and bound remains a PostgreSQL parameter.
        let mut fields = vec!["'window_start',bucket".to_owned()];
        let mut groups = vec!["bucket".to_owned()];
        for name in &plan.group_by {
            if ["source", "symbol"].contains(&name.as_str()) {
                fields.push(format!("'{name}',{name}"));
                groups.push(name.clone());
            }
        }
        for aggregation in &plan.aggregations {
            let (name, expression) = Self::aggregation(aggregation);
            fields.push(format!("'{name}',{expression}"));
        }
        if plan.predict.is_some() {
            fields.push("'prediction',AVG(price)::TEXT,'model_version','mean-price-v1','inference',true".to_owned());
        }
        let fields = fields.join(",");
        let groups = groups.join(",");
        format!("WITH latest AS (SELECT DISTINCT ON(source,source_event_id) source,source_event_id,symbol,event_at,seq,revision,payload
            FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND event_type='market' AND source=ANY($4)
            AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7
            ORDER BY source,source_event_id,revision DESC), samples AS (
            SELECT source,source_event_id,symbol,event_at,seq,revision,(payload->>'price')::NUMERIC AS price,{bucket} AS bucket FROM latest),
            ranked AS (SELECT *,FIRST_VALUE(price) OVER(PARTITION BY {groups} ORDER BY event_at,source,source_event_id,revision) AS first_price,
            FIRST_VALUE(price) OVER(PARTITION BY {groups} ORDER BY event_at DESC,source DESC,source_event_id DESC,revision DESC) AS last_price FROM samples)
            SELECT jsonb_build_object({fields}) FROM ranked GROUP BY {groups} ORDER BY {groups} LIMIT $9")
    }

    fn aggregation(aggregation: &Aggregation) -> (&'static str, &'static str) {
        match aggregation {
            Aggregation::Count => ("count", "COUNT(*)"),
            Aggregation::Sum => ("sum", "SUM(price)::TEXT"),
            Aggregation::Mean => ("mean", "AVG(price)::TEXT"),
            Aggregation::Min => ("min", "MIN(price)::TEXT"),
            Aggregation::Max => ("max", "MAX(price)::TEXT"),
            Aggregation::First => ("first", "MIN(first_price)::TEXT"),
            Aggregation::Last => ("last", "MIN(last_price)::TEXT"),
        }
    }
}
