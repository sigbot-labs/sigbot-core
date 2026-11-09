// SPDX-License-Identifier: GPL-3.0-only
//! Canonical source fact shared by ingestion, evaluation and strategy adapters.
use crate::exchange::mode::ExecutionMode;
use crate::messager::topic::MessageScope;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatafeedEvent {
    pub source: String,
    pub source_event_id: String,
    pub symbol: String,
    pub event_type: String,
    pub event_time: DateTime<Utc>,
    pub available_time: DateTime<Utc>,
    pub seq: i64,
    /// A source-declared revision. Sources without explicit versions remain at zero.
    #[serde(default, skip_serializing_if = "Self::is_initial_revision")]
    pub revision: i32,
    pub content: Option<String>,
    pub raw: Value,
}

impl DatafeedEvent {
    fn is_initial_revision(revision: &i32) -> bool {
        *revision == 0
    }
}

pub struct DatafeedPublication<'a> {
    pub scope: &'a MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub run_id: &'a Uuid,
    pub producer: &'a str,
    pub traceparent: &'a str,
}

pub struct HistoricalImport<'a> {
    pub mode: ExecutionMode,
    pub source_scope: &'a MessageScope,
    pub source_run: &'a Uuid,
    pub target_scope: &'a MessageScope,
    pub target_run: &'a Uuid,
    pub source: &'a str,
    pub source_event_id: &'a str,
    pub revision: i64,
    pub frozen_at: DateTime<Utc>,
    pub clock: DateTime<Utc>,
}
