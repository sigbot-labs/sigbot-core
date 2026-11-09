// SPDX-License-Identifier: GPL-3.0-only
//! Versioned strategy inputs and bounded effects. There is no exchange capability.

use crate::order::execution::ExecutionFact;

use crate::order::intent::TradeIntent;
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Trend,
    Range,
    Stabilizing,
    Suppression,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegimeProbabilities {
    pub trend: f64,
    pub range: f64,
    pub stabilizing: f64,
    pub suppression: f64,
    pub unknown: f64,
}

impl RegimeProbabilities {
    pub fn validate(&self) -> Result<()> {
        let values = [self.trend, self.range, self.stabilizing, self.suppression, self.unknown];
        ensure!(
            values.iter().all(|p| p.is_finite() && (0.0..=1.0).contains(p)),
            "invalid regime probability"
        );
        ensure!(
            (values.iter().sum::<f64>() - 1.0).abs() <= 1e-6,
            "regime probabilities must sum to one"
        );
        Ok(())
    }

    /// Stable ties choose the earliest category; uncertainty is never silently removed.
    pub fn dominant(&self) -> Result<MarketRegime> {
        self.validate()?;
        let values = [
            (MarketRegime::Trend, self.trend),
            (MarketRegime::Range, self.range),
            (MarketRegime::Stabilizing, self.stabilizing),
            (MarketRegime::Suppression, self.suppression),
            (MarketRegime::Unknown, self.unknown),
        ];
        Ok(values
            .into_iter()
            .fold((MarketRegime::Unknown, -1.0), |best, candidate| {
                if candidate.1 > best.1 {
                    candidate
                } else {
                    best
                }
            })
            .0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseReference {
    pub source: String,
    pub case_id: String,
    pub available_time: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperparametersUpdate {
    pub version: u64,
    pub as_of: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub probabilities: RegimeProbabilities,
    pub parameters: BTreeMap<String, Value>,
    pub model_version: String,
    pub classification_version: String,
    pub case_refs: Vec<CaseReference>,
}

impl HyperparametersUpdate {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        ensure!(
            self.version > 0 && self.as_of <= now && now < self.valid_until,
            "hyperparameters are future, expired, or unversioned"
        );
        ensure!(
            !self.model_version.is_empty() && !self.classification_version.is_empty(),
            "model/classification version required"
        );
        ensure!(
            self.case_refs
                .iter()
                .all(|case| case.available_time <= self.as_of && !case.case_id.is_empty()),
            "case memory contains future or unidentified information"
        );
        self.probabilities.validate()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyEvent {
    pub source: String,
    pub symbol: Option<String>,
    pub event_time: DateTime<Utc>,
    pub available_time: DateTime<Utc>,
    pub data: StrategyEventData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StrategyEventData {
    Market {
        price: Decimal,
        fields: BTreeMap<String, Value>,
    },
    News {
        source_event_id: String,
        text: String,
        url: Option<String>,
    },
    Execution(ExecutionFact),
    OrderStatus(crate::order::execution::OrderStatusFact),
    Portfolio(Value),
    Timer {
        scheduled_at: DateTime<Utc>,
        #[serde(default)]
        payload: Value,
    },
    Snapshot(SnapshotResult),
    Compute {
        rows: Vec<Value>,
        watermark: DateTime<Utc>,
        revision: u64,
    },
    QueryFailure {
        operation: QueryOperation,
        reason: String,
        retryable: bool,
    },
    QueryResult {
        operation: QueryOperation,
        result: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperation {
    Snapshot,
    Compute,
    Notification,
    Export,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotQuery {
    pub sources: Vec<String>,
    pub symbols: Vec<String>,
    pub mode: SnapshotMode,
    pub max_age_ms: u64,
    pub max_skew_ms: u64,
    pub max_rows: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "as_of", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotMode {
    Latest,
    AsOf(DateTime<Utc>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotPoint {
    pub source: String,
    pub symbol: String,
    pub event_time: DateTime<Utc>,
    pub available_time: DateTime<Utc>,
    pub price: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResult {
    pub points: Vec<SnapshotPoint>,
}

impl SnapshotQuery {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        ensure!(
            !self.sources.is_empty()
                && !self.symbols.is_empty()
                && self.sources.len() <= 32
                && self.symbols.len() <= 256
                && (1..=4096).contains(&self.max_rows),
            "snapshot query exceeds bounds"
        );
        ensure!(
            self.max_age_ms <= 86_400_000 && self.max_skew_ms <= self.max_age_ms,
            "invalid snapshot freshness bounds"
        );
        if let SnapshotMode::AsOf(as_of) = self.mode {
            ensure!(as_of <= now, "future AS_OF snapshot");
        }
        Ok(())
    }

    /// Multi-source snapshots are sampled independently, never advertised as atomic.
    pub fn validate_result(&self, result: &SnapshotResult, now: DateTime<Utc>) -> Result<()> {
        self.validate(now)?;
        let as_of = match self.mode {
            SnapshotMode::Latest => now,
            SnapshotMode::AsOf(value) => value,
        };
        ensure!(
            !result.points.is_empty() && result.points.len() <= self.max_rows as usize,
            "snapshot size is out of bounds"
        );
        let mut min = as_of;
        let mut max = DateTime::<Utc>::MIN_UTC;
        for point in &result.points {
            ensure!(
                self.sources.contains(&point.source) && self.symbols.contains(&point.symbol),
                "unrequested snapshot source/instrument"
            );
            ensure!(
                point.event_time <= point.available_time
                    && point.available_time <= as_of
                    && (as_of - point.event_time).num_milliseconds() <= self.max_age_ms as i64
                    && point.price > Decimal::ZERO,
                "snapshot is future, stale, or invalid"
            );
            min = min.min(point.event_time);
            max = max.max(point.event_time);
        }
        ensure!(
            (max - min).num_milliseconds() <= self.max_skew_ms as i64,
            "snapshot source skew exceeds limit"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputePlan {
    pub history_from: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub sources: Vec<String>,
    pub symbols: Vec<String>,
    pub window_ms: u64,
    pub group_by: Vec<String>,
    pub aggregations: Vec<Aggregation>,
    pub predict: Option<Prediction>,
    pub max_rows: u32,
    pub max_bytes: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prediction {
    pub model_version: String,
    pub partitioning: ModelPartitioning,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPartitioning {
    SingleOwner,
    IndependentSymbols,
    Mergeable { reducer_version: String },
}

impl ComputePlan {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        ensure!(
            self.history_from < self.as_of && self.as_of <= now,
            "compute history includes future data"
        );
        ensure!(
            self.window_ms > 0 && !self.sources.is_empty() && !self.symbols.is_empty(),
            "compute plan needs window/source/instrument"
        );
        ensure!(
            (1..=4096).contains(&self.max_rows) && (1..=1_048_576).contains(&self.max_bytes),
            "compute result must be bounded"
        );
        ensure!(
            self.sources.len() <= 32 && self.symbols.len() <= 256 && self.group_by.len() <= 8,
            "compute plan exceeds input bounds"
        );
        if let Some(model) = &self.predict {
            ensure!(!model.model_version.is_empty(), "prediction needs a versioned model");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "query", rename_all = "snake_case")]
pub enum StrategyRequest {
    Snapshot(SnapshotQuery),
    Compute(ComputePlan),
    Notification(crate::notifier::request::NotificationRequest),
    Export(crate::exporter::request::ExportBatch),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyTimer {
    pub at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Effects {
    pub intents: Vec<TradeIntent>,
    pub requests: Vec<StrategyRequest>,
    pub timers: Vec<StrategyTimer>,
}

impl Effects {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        ensure!(
            self.intents.len() <= 64 && self.requests.len() <= 64 && self.timers.len() <= 64,
            "effects exceed transaction bounds"
        );
        for request in &self.requests {
            match request {
                StrategyRequest::Snapshot(query) => query.validate(now)?,
                StrategyRequest::Compute(plan) => plan.validate(now)?,
                StrategyRequest::Notification(request) => request.validate()?,
                StrategyRequest::Export(batch) => batch.validate_request()?,
            }
        }
        ensure!(
            self.timers.iter().all(|timer| timer.at >= now),
            "timer cannot move virtual time backwards"
        );
        Ok(())
    }
}
