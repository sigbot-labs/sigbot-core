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

use crate::modules::datafeed::datafeed::DatafeedProvider;
use crate::modules::notification::notification::NotificationProvider;
use crate::modules::order::OrderMgrProvider;
use crate::modules::strategy::strategy::StrategyProvider;
use crate::{EntityBase, PageResponse};
use common_makestruct::MakeStructWith;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::collections::HashMap;
use validator::Validate;

// ---- Entity ---

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub status: Option<JobStatus>,
    #[serde(rename = "flow_json", skip_serializing_if = "Option::is_none")]
    pub flow_info: Option<WorkflowFlowInfo>,
    pub properties: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl Default for WorkflowInfo {
    fn default() -> Self {
        WorkflowInfo {
            base: EntityBase::new_empty(),
            name: None,
            status: Some(JobStatus::PENDING),
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        }
    }
}

impl WorkflowInfo {
    /// Get flow nodes matching a specific stage type
    /// If providers vec is empty, matches all nodes of that stage type.
    /// If providers vec is not empty, matches nodes that have any of the specified providers.
    /// For example:
    /// - `get_flow_nodes(WorkflowStageType::EVALUATION(vec![]))` returns all EVALUATION stage nodes
    /// - `get_flow_nodes(WorkflowStageType::EVALUATION(vec![WorkflowStageProvider::Strategy(StrategyProvider::PYCODE)]))`
    ///   returns only EVALUATION nodes with PYCODE provider
    pub fn get_stage_nodes(&self, stage: WorkflowStageType) -> Vec<i64> {
        if let Some(ref flow_info) = self.flow_info {
            use std::mem::discriminant;
            let target_discriminant = discriminant(&stage);
            flow_info
                .nodes
                .iter()
                .filter_map(|node| {
                    if discriminant(&node.stage) == target_discriminant {
                        Some(node.id)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum JobStatus {
    #[serde(rename = "PENDING")]
    PENDING,
    #[serde(rename = "STARTING")]
    STARTING,
    #[serde(rename = "RUNNING")]
    RUNNING,
    #[serde(rename = "STOPPING")]
    STOPPING,
    #[serde(rename = "STOPPED")]
    STOPPED,
    #[serde(rename = "SUCCESS")]
    SUCCESS,
    #[serde(rename = "FAILED")]
    FAILED,
}

impl JobStatus {
    pub fn of(status: &str) -> Result<JobStatus, String> {
        match status.to_lowercase().as_str() {
            "PENDING" => Ok(JobStatus::PENDING),
            "STARTING" => Ok(JobStatus::STARTING),
            "RUNNING" => Ok(JobStatus::RUNNING),
            "STOPPING" => Ok(JobStatus::STOPPING),
            "STOPPED" => Ok(JobStatus::STOPPED),
            "SUCCESS" => Ok(JobStatus::SUCCESS),
            "FAILED" => Ok(JobStatus::FAILED),
            _ => Err(format!("Unsupported job status: {}", status)),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            JobStatus::PENDING => "PENDING",
            JobStatus::STARTING => "STARTING",
            JobStatus::RUNNING => "RUNNING",
            JobStatus::STOPPING => "STOPPING",
            JobStatus::STOPPED => "STOPPED",
            JobStatus::SUCCESS => "SUCCESS",
            JobStatus::FAILED => "FAILED",
        }
    }
}

// ---- Flow Structure ----

/// Workflow flow information containing nodes and connections
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowFlowInfo {
    pub nodes: Vec<WorkflowNodeInfo>,
    pub connections: Vec<WorkflowConnectionInfo>,
}

/// Custom serialization for WorkflowNodeStage to/from string
/// Supports format: "PROVIDER@STAGE" (e.g., "TWITTER@INPUT", "PYCODE@PROCESS")
/// For multiple providers, serialize as comma-separated: "PROVIDER1,PROVIDER2@STAGE"
/// Also supports legacy format: "PROVIDER" or "STAGE" for backward compatibility
mod workflow_stage_type_serde {
    use super::{WorkflowStageType, WorkflowStageWrapper};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(node_stage: &WorkflowStageType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as "PROVIDER@STAGE" format for better frontend compatibility
        // Each node has a single provider
        match node_stage {
            WorkflowStageType::INPUT(providers) => {
                if let Some(provider) = providers.as_ref().and_then(|v| v.first()) {
                    let _: &WorkflowStageWrapper = provider; // Help type inference
                    serializer.serialize_str(&format!("{}@INPUT", provider.as_str()))
                } else {
                    serializer.serialize_str("INPUT")
                }
            }
            WorkflowStageType::EVALUATION(providers) => {
                if let Some(provider) = providers.as_ref().and_then(|v| v.first()) {
                    let _: &WorkflowStageWrapper = provider; // Help type inference
                    serializer.serialize_str(&format!("{}@EVALUATION", provider.as_str()))
                } else {
                    serializer.serialize_str("EVALUATION")
                }
            }
            WorkflowStageType::TRANSACTION(providers) => {
                if let Some(provider) = providers.as_ref().and_then(|v| v.first()) {
                    let _: &WorkflowStageWrapper = provider; // Help type inference
                    serializer.serialize_str(&format!("{}@TRANSACTION", provider.as_str()))
                } else {
                    serializer.serialize_str("TRANSACTION")
                }
            }
            WorkflowStageType::OUTPUT(providers) => {
                if let Some(provider) = providers.as_ref().and_then(|v| v.first()) {
                    let _: &WorkflowStageWrapper = provider; // Help type inference
                    serializer.serialize_str(&format!("{}@OUTPUT", provider.as_str()))
                } else {
                    serializer.serialize_str("OUTPUT")
                }
            }
            WorkflowStageType::POST(providers) => {
                if let Some(provider) = providers.as_ref().and_then(|v| v.first()) {
                    let _: &WorkflowStageWrapper = provider; // Help type inference
                    serializer.serialize_str(&format!("{}@POST", provider.as_str()))
                } else {
                    serializer.serialize_str("POST")
                }
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<WorkflowStageType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        WorkflowStageType::from_str(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("Invalid workflow node stage: {}", s)))
    }
}

/// Workflow node information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowNodeInfo {
    pub id: i64,
    pub name: String,
    pub status: Option<JobStatus>,
    #[serde(with = "workflow_stage_type_serde")]
    pub stage: WorkflowStageType,
    pub position: WorkflowNodePosition,
    pub style: Option<HashMap<String, Value>>,
    pub icon: Option<String>,
    pub config: Option<HashMap<String, Value>>,
}

/// Workflow node position information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowNodePosition {
    pub x: f64,
    pub y: f64,
}

/// Workflow connection information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowConnectionInfo {
    pub id: String,
    pub source: String,
    pub target: String,
}

/// Unified provider enumeration for workflow stages
/// This allows each stage to support multiple different provider types
/// New provider types can be easily added here without changing the stage structure
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
#[serde(tag = "type", content = "value")]
pub enum WorkflowStageWrapper {
    /// Datafeed provider (for INPUT stage)
    Datafeed(DatafeedProvider),
    /// Strategy provider (for PROCESS stage)
    Strategy(StrategyProvider),
    /// Order manager provider (for OUTPUT stage)
    OrderMgr(OrderMgrProvider),
    /// Notification provider (for POST stage)
    Notification(NotificationProvider),
    /// External writer provider (for OUTPUT stage, e.g., GOOGLE_SHEET)
    ExternalWriter(String),
}

impl WorkflowStageWrapper {
    pub fn as_str(&self) -> String {
        match self {
            WorkflowStageWrapper::Datafeed(p) => p.as_str().to_string(),
            WorkflowStageWrapper::Strategy(p) => p.as_str().to_string(),
            WorkflowStageWrapper::OrderMgr(p) => p.as_str().to_string(),
            WorkflowStageWrapper::Notification(p) => p.as_str().to_string(),
            WorkflowStageWrapper::ExternalWriter(s) => s.clone(),
        }
    }

    pub fn from_str(provider_str: &str, stage_type: &str) -> Option<Self> {
        match stage_type.to_uppercase().as_str() {
            "INPUT" => {
                if let Ok(provider) = DatafeedProvider::of(provider_str) {
                    Some(WorkflowStageWrapper::Datafeed(provider))
                } else {
                    None
                }
            }
            "EVALUATION" => {
                if let Ok(provider) = StrategyProvider::of(provider_str) {
                    Some(WorkflowStageWrapper::Strategy(provider))
                } else {
                    None
                }
            }
            "TRANSACTION" => {
                if let Ok(provider) = OrderMgrProvider::of(provider_str) {
                    Some(WorkflowStageWrapper::OrderMgr(provider))
                } else {
                    None
                }
            }
            "OUTPUT" => {
                // TODO: support external writer provider
                if let Ok(provider) = OrderMgrProvider::of(provider_str) {
                    Some(WorkflowStageWrapper::ExternalWriter(provider_str.to_string()))
                } else {
                    None
                }
            }
            "POST" => {
                if let Ok(provider) = NotificationProvider::of(provider_str) {
                    Some(WorkflowStageWrapper::Notification(provider))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Workflow node stage enumeration
/// Represents different stages of nodes in a workflow, each associated with providers
///
/// Workflow stages flow:
/// INPUT -> EVALUATION -> TRANSACTION -> OUTPUT -> POST
///
/// - INPUT: Input stage with DatafeedProvider (BINANCE, TWITTER, TRUTHSOCIAL)
/// - EVALUATION: Evaluation stage with StrategyProvider (LLM/AI_EVALUATOR, PYCODE/PY_EVALUATOR)
/// - TRANSACTION: Transaction stage with OrderMgrProvider (DEFAULT, etc.) for order execution
/// - OUTPUT: Output stage with ExternalWriterProvider (GOOGLE_SHEET, etc.) for external data writing
/// - POST: Post-processing stage with NotificationProvider (EMAIL, TELEGRAM)
///
/// Each stage can have 0 to N providers. None means match all providers of that stage.
/// Using Vec<WorkflowStageProvider> allows flexible combination of different provider types.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum WorkflowStageType {
    /// Input stage with datafeed providers (None matches all INPUT nodes)
    INPUT(Option<Vec<WorkflowStageWrapper>>),
    /// Evaluation stage with strategy providers (None matches all EVALUATION nodes)
    /// Contains AI Evaluator (LLM) and Py Runner (PYCODE) nodes
    EVALUATION(Option<Vec<WorkflowStageWrapper>>),
    /// Transaction stage with order manager providers (None matches all TRANSACTION nodes)
    /// Contains order executor nodes (e.g., Binance, Hyperliquid, Polymarket, IBKR)
    TRANSACTION(Option<Vec<WorkflowStageWrapper>>),
    /// Output stage with external writer providers (None matches all OUTPUT nodes)
    /// Contains external writer nodes (e.g., Google Sheet)
    OUTPUT(Option<Vec<WorkflowStageWrapper>>),
    /// Post-processing stage with notification providers (None matches all POST nodes)
    POST(Option<Vec<WorkflowStageWrapper>>),
}

impl WorkflowStageType {
    /// Get node stage string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowStageType::INPUT(_) => "INPUT",
            WorkflowStageType::EVALUATION(_) => "EVALUATION",
            WorkflowStageType::TRANSACTION(_) => "TRANSACTION",
            WorkflowStageType::OUTPUT(_) => "OUTPUT",
            WorkflowStageType::POST(_) => "POST",
        }
    }

    /// Match node type string to WorkflowNodeStage
    /// Supports format: "PROVIDER@STAGE" (e.g., "TWITTER@INPUT", "PYCODE@EVALUATION")
    /// Returns None if the string doesn't match any known node stage
    pub fn from_str(node_type: &str) -> Option<Self> {
        if let Some(at_pos) = node_type.find('@') {
            let provider_str = node_type[..at_pos].trim();
            let stage_str = &node_type[at_pos + 1..];

            // Parse single provider
            if provider_str.is_empty() {
                return None;
            }
            let provider = WorkflowStageWrapper::from_str(provider_str, stage_str)?;

            return match stage_str.to_uppercase().as_str() {
                "INPUT" => Some(WorkflowStageType::INPUT(Some(vec![provider]))),
                "EVALUATION" => Some(WorkflowStageType::EVALUATION(Some(vec![provider]))),
                "TRANSACTION" => Some(WorkflowStageType::TRANSACTION(Some(vec![provider]))),
                "OUTPUT" => Some(WorkflowStageType::OUTPUT(Some(vec![provider]))),
                "POST" => Some(WorkflowStageType::POST(Some(vec![provider]))),
                _ => None,
            };
        }
        None
    }
}

/// SqliteRow impl for Workflow.
impl<'r> FromRow<'r, SqliteRow> for WorkflowInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let status_str: Option<String> = row.try_get("status").ok().flatten();
        let status = status_str.as_ref().map(|s| JobStatus::of(s)).transpose().map_err(|e| {
            sqlx::Error::Decode(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse workflow status: {}", e),
                )
                .into(),
            )
        })?;

        // Read flow_json from database and deserialize to flow_info
        let flow_info = {
            let flow_json_str: Option<String> = row.try_get("flow_json").ok().flatten();
            flow_json_str
                .as_ref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .and_then(|json| serde_json::from_value::<WorkflowFlowInfo>(json).ok())
        };

        Ok(WorkflowInfo {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            status,
            flow_info,
            // TODO: auto convert and wrap to Map attribute.
            properties: None,
            secrets: None,
            description: row.try_get("description")?,
        })
    }
}

/// Postgres Row impl for Workflow.
impl<'r> FromRow<'r, PgRow> for WorkflowInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let status_str: Option<String> = row.try_get("status").ok().flatten();
        let status = status_str.as_ref().map(|s| JobStatus::of(s)).transpose().map_err(|e| {
            sqlx::Error::Decode(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse workflow status: {}", e),
                )
                .into(),
            )
        })?;

        // Read flow_json from database (PostgreSQL JSONB) and deserialize to flow_info
        let flow_info = {
            let flow_json: Option<Value> = row.try_get("flow_json").ok().flatten();
            flow_json
                .and_then(|v| if v.is_null() { None } else { Some(v) })
                .and_then(|json| serde_json::from_value::<WorkflowFlowInfo>(json).ok())
        };

        Ok(WorkflowInfo {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            status,
            flow_info,
            // TODO: auto convert and wrap to Map attribute.
            properties: None,
            secrets: None,
            description: row.try_get("description")?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryWorkflowRequest {
    #[validate(length(min = 1, max = 32))]
    pub name: Option<String>,
    pub status: Option<String>,
}

impl QueryWorkflowRequest {
    pub fn to_entity(&self) -> WorkflowInfo {
        WorkflowInfo {
            base: EntityBase::new_empty(),
            name: self.name.clone(),
            status: self.status.as_ref().and_then(|s| JobStatus::of(s).ok()),
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryWorkflowResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<WorkflowInfo>>,
}

impl QueryWorkflowResponse {
    pub fn new(page: PageResponse, data: Vec<WorkflowInfo>) -> Self {
        QueryWorkflowResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
pub struct SaveWorkflowRequest {
    pub id: Option<i64>,
    #[validate(length(min = 1, max = 32))]
    pub name: String,
    pub status: Option<String>,
    pub flow_info: Option<WorkflowFlowInfo>,
    #[validate(length(min = 1, max = 8192))]
    pub plain_configuration: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 8192))]
    pub secret_configuration: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 256))]
    pub description: Option<String>,
}

impl SaveWorkflowRequest {
    pub fn to_entity(&self) -> WorkflowInfo {
        WorkflowInfo {
            base: EntityBase::new_with_id(self.id),
            name: Some(self.name.clone()),
            status: self.status.as_ref().and_then(|s| JobStatus::of(s).ok()),
            flow_info: self.flow_info.clone(),
            properties: self.plain_configuration.clone(),
            secrets: self.secret_configuration.clone(),
            description: self.description.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveWorkflowResponse {
    pub id: i64,
}

impl SaveWorkflowResponse {
    pub fn new(id: i64) -> Self {
        SaveWorkflowResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteWorkflowRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteWorkflowResponse {
    pub count: u64,
}

impl DeleteWorkflowResponse {
    pub fn new(count: u64) -> Self {
        DeleteWorkflowResponse { count }
    }
}
