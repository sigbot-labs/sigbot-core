// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::messager::messager::MessagerConfig;
use crate::messager::topic::{MessageScope, WorkflowTopic};
use anyhow::Context;
use anyhow::Error;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HistoryCursor {
    pub available_at: DateTime<Utc>,
    pub priority: i32,
    pub source: String,
    pub source_event_id: String,
    pub revision: i64,
}

#[derive(Clone)]
pub struct HistoricalRecord {
    pub cursor: HistoryCursor,
    pub event_time: DateTime<Utc>,
    pub topic: WorkflowTopic,
    pub payload: serde_json::Value,
}

pub struct ReplayIdentity<'a> {
    pub scope: &'a MessageScope,
    pub workflow_id: Uuid,
    pub run_id: &'a Uuid,
}

pub struct ReplayHistory<'a> {
    pub scope: &'a MessageScope,
    pub run_id: &'a Uuid,
    pub workflow_id: Uuid,
    pub sources: &'a [String],
    pub from: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub frozen_at: DateTime<Utc>,
}

pub struct ReplayCheckpoint {
    pub cursor: Option<HistoryCursor>,
    pub virtual_time: DateTime<Utc>,
    pub steps: u64,
    pub timer_firings: u64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct BacktestArgument {
    pub messager_config: Arc<MessagerConfig>,
    pub properties: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
}

impl BacktestArgument {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).context("Failed to parse backtest manager info from JSON")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub enum BacktestProvider {
    #[serde(rename = "KLINE")]
    Kline,
    #[serde(rename = "TRADES")]
    Trades,
}

impl BacktestProvider {
    pub fn of(provider: &str) -> Result<BacktestProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "KLINE" => Ok(BacktestProvider::Kline),
            "TRADES" => Ok(BacktestProvider::Trades),
            _ => Err(anyhow::anyhow!(
                "Unsupported the backtest manager provider: {}",
                provider
            )),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            BacktestProvider::Kline => "KLINE",
            BacktestProvider::Trades => "TRADES",
        }
    }
}

#[cfg(test)]
mod tests {}
