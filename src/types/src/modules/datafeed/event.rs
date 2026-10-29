// SPDX-License-Identifier: GPL-3.0-only
//! Canonical source fact shared by ingestion, evaluation and strategy adapters.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
