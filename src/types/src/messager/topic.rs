// SPDX-License-Identifier: GPL-3.0-only
//! The single v2 topic vocabulary. Tenant identifiers never accept MQTT wildcards.

use anyhow::bail;

use anyhow::ensure;

use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageScope {
    pub env: String,
    pub tenant_id: Uuid,
}

impl MessageScope {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.env.is_empty()
                && self.env.len() <= 64
                && self
                    .env
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "env must be a nonempty MQTT-safe identifier"
        );
        ensure!(self.tenant_id.get_version_num() == 7, "tenant_id must be UUIDv7");
        Ok(())
    }

    pub fn prefix(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("internal/v2/{}/{}", self.env, self.tenant_id))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowTopic {
    FeedMarket,
    FeedNews,
    FeedStatus,
    EvaluationSnapshot,
    StrategySignal,
    FeedSnapshotRequest,
    FeedSnapshotResult,
    ComputeRequest,
    ComputeResult,
    DeploymentRequest,
    DeploymentResult,
    NotificationRequest,
    NotificationResult,
    ExportRequest,
    ExportResult,
    RuntimeEvent,
    LogEvent,
}

impl WorkflowTopic {
    pub const ALL: [Self; 17] = [
        Self::FeedMarket,
        Self::FeedNews,
        Self::FeedStatus,
        Self::EvaluationSnapshot,
        Self::StrategySignal,
        Self::FeedSnapshotRequest,
        Self::FeedSnapshotResult,
        Self::ComputeRequest,
        Self::ComputeResult,
        Self::DeploymentRequest,
        Self::DeploymentResult,
        Self::NotificationRequest,
        Self::NotificationResult,
        Self::ExportRequest,
        Self::ExportResult,
        Self::RuntimeEvent,
        Self::LogEvent,
    ];

    pub const fn suffix(self) -> &'static str {
        match self {
            Self::FeedMarket => "feed/market",
            Self::FeedNews => "feed/news",
            Self::FeedStatus => "feed/status",
            Self::EvaluationSnapshot => "evaluation/snapshot",
            Self::StrategySignal => "strategy/signal",
            Self::FeedSnapshotRequest => "feed/snapshot/request",
            Self::FeedSnapshotResult => "feed/snapshot/result",
            Self::ComputeRequest => "compute/request",
            Self::ComputeResult => "compute/result",
            Self::DeploymentRequest => "deployment/request",
            Self::DeploymentResult => "deployment/result",
            Self::NotificationRequest => "notification/request",
            Self::NotificationResult => "notification/result",
            Self::ExportRequest => "export/request",
            Self::ExportResult => "export/result",
            Self::RuntimeEvent => "runtime/event",
            Self::LogEvent => "log/event",
        }
    }

    pub fn topic(self, scope: &MessageScope, workflow_id: Uuid) -> Result<String> {
        ensure!(workflow_id.get_version_num() == 7, "workflow_id must be UUIDv7");
        Ok(format!(
            "{}/workflow/{}/{}",
            scope.prefix()?,
            workflow_id,
            self.suffix()
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountTopic {
    OrderEvent,
    ExecutionEvent,
    WalletReservationRequest,
    WalletReservationResult,
    PortfolioSnapshot,
}

impl AccountTopic {
    pub const ALL: [Self; 5] = [
        Self::OrderEvent,
        Self::ExecutionEvent,
        Self::WalletReservationRequest,
        Self::WalletReservationResult,
        Self::PortfolioSnapshot,
    ];

    pub const fn suffix(self) -> &'static str {
        match self {
            Self::OrderEvent => "order/event",
            Self::ExecutionEvent => "execution/event",
            Self::WalletReservationRequest => "wallet/reservation/request",
            Self::WalletReservationResult => "wallet/reservation/result",
            Self::PortfolioSnapshot => "portfolio/snapshot",
        }
    }

    pub fn topic(self, scope: &MessageScope, account_id: Uuid) -> Result<String> {
        ensure!(account_id.get_version_num() == 7, "account_id must be UUIDv7");
        Ok(format!("{}/account/{}/{}", scope.prefix()?, account_id, self.suffix()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicAddress {
    Workflow {
        scope: MessageScope,
        workflow_id: Uuid,
        kind: WorkflowTopic,
    },
    Account {
        scope: MessageScope,
        account_id: Uuid,
        kind: AccountTopic,
    },
}

impl TopicAddress {
    pub fn parse(topic: &str) -> Result<Self> {
        let parts: Vec<_> = topic.split('/').collect();
        ensure!(
            parts.len() >= 8 && parts[0] == "internal" && parts[1] == "v2",
            "invalid v2 topic"
        );
        let scope = MessageScope {
            env: parts[2].into(),
            tenant_id: parts[3].parse()?,
        };
        scope.validate()?;
        let id: Uuid = parts[5].parse()?;
        ensure!(id.get_version_num() == 7, "topic resource identifier must be UUIDv7");
        let suffix = parts[6..].join("/");
        match parts[4] {
            "workflow" => {
                let kind = WorkflowTopic::ALL
                    .into_iter()
                    .find(|kind| kind.suffix() == suffix)
                    .ok_or_else(|| anyhow::anyhow!("unknown workflow topic suffix"))?;
                Ok(Self::Workflow {
                    scope,
                    workflow_id: id,
                    kind,
                })
            }
            "account" => {
                let kind = AccountTopic::ALL
                    .into_iter()
                    .find(|kind| kind.suffix() == suffix)
                    .ok_or_else(|| anyhow::anyhow!("unknown account topic suffix"))?;
                Ok(Self::Account {
                    scope,
                    account_id: id,
                    kind,
                })
            }
            _ => bail!("unknown topic resource"),
        }
    }

    pub fn scope(&self) -> &MessageScope {
        match self {
            Self::Workflow { scope, .. } | Self::Account { scope, .. } => scope,
        }
    }
}

/// Edges authorize delivery inside a workflow; topic subscription alone is insufficient.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEdge {
    pub source_node_id: i64,
    pub target_node_id: i64,
    pub kind: WorkflowTopic,
}

impl WorkflowEdge {
    pub fn permits(&self, source_node_id: i64, target_node_id: i64, kind: WorkflowTopic) -> bool {
        self.source_node_id == source_node_id && self.target_node_id == target_node_id && self.kind == kind
    }
}
