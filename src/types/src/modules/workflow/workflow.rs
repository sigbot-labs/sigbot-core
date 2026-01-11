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

use crate::{EntityBase, PageResponse};
use common_makestruct::MakeStructWith;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::collections::HashMap;
use validator::Validate;

// ---- Flow Structure ----

/// Workflow node position information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowNodePosition {
    pub x: f64,
    pub y: f64,
}

/// Workflow node information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowNodeInfo {
    pub id: String,
    pub name: String,
    pub r#type: String,
    pub icon: Option<String>,
    pub style: Option<HashMap<String, Value>>,
    pub position: WorkflowNodePosition,
    pub status: Option<String>,
    pub config: Option<HashMap<String, Value>>,
}

/// Workflow connection information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowConnectionInfo {
    pub id: String,
    pub source: String,
    pub target: String,
}

/// Workflow flow information containing nodes and connections
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowFlowInfo {
    pub nodes: Vec<WorkflowNodeInfo>,
    pub connections: Vec<WorkflowConnectionInfo>,
}

// ---- Entity ---

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct WorkflowInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub provider: Option<WorkflowProvider>,
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
            provider: None,
            status: Some(JobStatus::PENDING),
            flow_info: None,
            properties: None,
            secrets: None,
            description: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum WorkflowProvider {
    PYCODE,
    LLM,
}

impl WorkflowProvider {
    pub fn of(provider: &str) -> Result<WorkflowProvider, String> {
        match provider.to_uppercase().as_str() {
            "PYCODE" => Ok(WorkflowProvider::PYCODE),
            "LLM" => Ok(WorkflowProvider::LLM),
            _ => Err(format!("Unsupported workflow provider: {}", provider)),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            WorkflowProvider::PYCODE => "PYCODE",
            WorkflowProvider::LLM => "LLM",
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
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| WorkflowProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse workflow provider: {}", e),
                        )
                        .into(),
                    )
                })?,
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
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| WorkflowProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse workflow provider: {}", e),
                        )
                        .into(),
                    )
                })?,
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
    #[validate(length(min = 1, max = 16))]
    pub provider: Option<String>,
    pub status: Option<String>,
}

impl QueryWorkflowRequest {
    pub fn to_entity(&self) -> WorkflowInfo {
        WorkflowInfo {
            base: EntityBase::new_empty(),
            name: self.name.clone(),
            provider: self.provider.as_ref().and_then(|p| WorkflowProvider::of(p).ok()),
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
    #[validate(length(min = 1, max = 16))]
    pub provider: String,
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
            provider: WorkflowProvider::of(&self.provider).ok(),
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
