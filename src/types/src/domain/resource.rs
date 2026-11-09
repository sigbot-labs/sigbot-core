// SPDX-License-Identifier: GPL-3.0-only
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

/// The public domain registry is also the SQL identifier allowlist.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Domain {
    Tenant,
    Cluster,
    Datasource,
    Exchange,
    Strategy,
    Order,
    Ledger,
    Notifier,
    Exporter,
    Workflow,
}

impl Domain {
    pub const ALL: [Self; 10] = [
        Self::Tenant,
        Self::Cluster,
        Self::Datasource,
        Self::Exchange,
        Self::Strategy,
        Self::Order,
        Self::Ledger,
        Self::Notifier,
        Self::Exporter,
        Self::Workflow,
    ];
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Cluster => "cluster",
            Self::Datasource => "datasource",
            Self::Exchange => "exchange",
            Self::Strategy => "strategy",
            Self::Order => "order",
            Self::Ledger => "ledger",
            Self::Notifier => "notifier",
            Self::Exporter => "exporter",
            Self::Workflow => "workflow",
        }
    }
    pub fn table(self) -> String {
        format!("{}_info", self.as_str())
    }
    pub fn collection(self) -> String {
        match self {
            Self::Strategy => "strategies".into(),
            _ => format!("{}s", self.as_str()),
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|domain| domain.as_str() == value)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceOperation {
    List,
    Get,
    Create,
    Update,
    Delete,
}

/// Tenant and audit identity always come from verified protocol context.
/// Entity fields are checked against the owning table schema before SQL composition.
#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceCommand {
    pub domain: Domain,
    pub operation: ResourceOperation,
    pub env: String,
    #[serde(default)]
    pub id: Option<Uuid>,
    #[serde(default)]
    pub fields: Map<String, Value>,
    #[serde(default = "ResourceCommand::default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

impl ResourceCommand {
    fn default_limit() -> u32 {
        50
    }
}
