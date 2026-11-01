// SPDX-License-Identifier: GPL-3.0-only
//! Bounded market-window aggregation SQL shared by live and backtest queries.

use super::MarketQueryPostgresSession;
use sigbot_types::strategy::execution::{Aggregation, ComputePlan};

impl MarketQueryPostgresSession {
    pub fn statement(plan: &ComputePlan, timescale: bool) -> String {
        let bucket = if timescale {
            "time_bucket(make_interval(secs=>$8),event_at,TIMESTAMPTZ '1970-01-01 00:00:00+00')"
        } else {
            "date_bin(make_interval(secs=>$8),event_at,TIMESTAMPTZ '1970-01-01 00:00:00+00')"
        };
        let mut fields = vec!["'window_start',bucket".to_owned()];
        let mut groups = vec!["bucket".to_owned()];
        for name in &plan.group_by {
            if ["source", "symbol"].contains(&name.as_str()) {
                fields.push(format!("'{name}',{name}"));
                groups.push(name.clone());
            }
        }
        for aggregation in &plan.aggregations {
            let (name, expression) = match aggregation {
                Aggregation::Count => ("count", "COUNT(*)"),
                Aggregation::Sum => ("sum", "SUM(price)::TEXT"),
                Aggregation::Mean => ("mean", "AVG(price)::TEXT"),
                Aggregation::Min => ("min", "MIN(price)::TEXT"),
                Aggregation::Max => ("max", "MAX(price)::TEXT"),
                Aggregation::First => ("first", "MIN(first_price)::TEXT"),
                Aggregation::Last => ("last", "MIN(last_price)::TEXT"),
            };
            fields.push(format!("'{name}',{expression}"));
        }
        if plan.predict.is_some() {
            fields.push("'prediction',AVG(price)::TEXT,'model_version','mean-price-v1','inference',true".to_owned());
        }
        let fields = fields.join(",");
        let groups = groups.join(",");
        format!("WITH latest AS (SELECT DISTINCT ON(source,source_event_id) source,source_event_id,symbol,event_at,seq,revision,payload
            FROM mkt_event WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND event_type='market' AND source=ANY($4)
            AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7 ORDER BY source,source_event_id,revision DESC),
            samples AS (SELECT source,source_event_id,symbol,event_at,seq,revision,(payload->>'price')::NUMERIC AS price,{bucket} AS bucket FROM latest),
            ranked AS (SELECT *,FIRST_VALUE(price) OVER(PARTITION BY {groups} ORDER BY event_at,source,source_event_id,revision) AS first_price,
            FIRST_VALUE(price) OVER(PARTITION BY {groups} ORDER BY event_at DESC,source DESC,source_event_id DESC,revision DESC) AS last_price FROM samples)
            SELECT jsonb_build_object({fields}) FROM ranked GROUP BY {groups} ORDER BY {groups} LIMIT $9")
    }
}
