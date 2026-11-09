//! Shared validation before a workflow config is stored or deployed.
use super::workflow::WorkflowFlowInfo;
use crate::messager::topic::WorkflowTopic;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use serde_json::json;
use serde_json::Value;
use std::collections::HashSet;

pub struct WorkflowDeployment;
impl WorkflowDeployment {
    /// Freeze one trading mode independently of dev/sit/uat/prod isolation.
    /// The Workflow mode is mandatory and must match every Order adapter.
    pub fn execution_mode(
        flow: &WorkflowFlowInfo,
        requested: Option<&Value>,
    ) -> Result<crate::exchange::mode::ExecutionMode> {
        use crate::exchange::mode::ExecutionMode;
        let mode: ExecutionMode =
            serde_json::from_value(requested.cloned().context("Workflow execution mode required")?)?;
        for node in &flow.nodes {
            let Some(config) = &node.config else {
                continue;
            };
            if config.get("module").and_then(Value::as_str) != Some("order") {
                continue;
            }
            let Some(argument) = config.get("argument_json") else {
                continue;
            };
            let argument: Value = match argument.as_str() {
                Some(value) => serde_json::from_str(value)?,
                None => argument.clone(),
            };
            let Some(runtime) = argument.pointer("/properties/runtime_config") else {
                continue;
            };
            let runtime: Value = match runtime.as_str() {
                Some(value) => serde_json::from_str(value)?,
                None => runtime.clone(),
            };
            let adapter: ExecutionMode = serde_json::from_value(
                runtime
                    .pointer("/exchange/mode")
                    .cloned()
                    .context("Order exchange mode required")?,
            )?;
            ensure!(mode == adapter, "Workflow and Order execution modes differ");
        }
        Ok(mode)
    }

    pub fn validate(flow: &WorkflowFlowInfo, ready: bool) -> Result<()> {
        ensure!(
            flow.nodes.len() <= 128 && (!ready || !flow.nodes.is_empty()),
            "invalid workflow node count"
        );
        let ids: HashSet<i64> = flow.nodes.iter().map(|node| node.id).collect();
        ensure!(
            ids.len() == flow.nodes.len() && ids.iter().all(|id| *id > 0),
            "node IDs must be unique and positive"
        );
        for node in &flow.nodes {
            let config = node.config.as_ref().context("node configuration required")?;
            let module = config
                .get("module")
                .and_then(Value::as_str)
                .context("node module required")?;
            ensure!(
                matches!(
                    module,
                    "datafeed" | "evaluator" | "strategy" | "order" | "wallet" | "notifier" | "exporter" | "backtest"
                ),
                "unknown node module"
            );
            Self::validate_credentials(&serde_json::to_value(config)?)?;
            ensure!(
                !ready
                    || config
                        .get("argument_json")
                        .is_some_and(|value| value.is_object() || value.is_string()),
                "configure node argument_json before starting"
            );
        }
        for edge in &flow.connections {
            let source: i64 = edge.source.parse()?;
            let target: i64 = edge.target.parse()?;
            ensure!(
                source != target && ids.contains(&source) && ids.contains(&target),
                "invalid workflow edge"
            );
            if let Some(kind) = &edge.kind {
                let _: WorkflowTopic = serde_json::from_value(json!(kind))?;
            }
        }
        Ok(())
    }
    /// Persist only references to central secret keys, never credential material.
    pub fn validate_credentials(value: &Value) -> Result<()> {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    let key = key.to_ascii_lowercase();
                    if matches!(key.as_str(), "argument_json" | "runtime_config" | "matching_config") {
                        if let Some(encoded) = value.as_str() {
                            Self::validate_credentials(&serde_json::from_str::<Value>(encoded)?)?;
                            continue;
                        }
                    }
                    if key == "sys_environment" {
                        ensure!(
                            value.is_null() || value.as_object().is_some_and(|values| values.is_empty()),
                            "custom process environment is forbidden"
                        );
                    }
                    if key == "secret_keys" {
                        ensure!(
                            value.as_object().is_some_and(|values| values
                                .values()
                                .all(|value| value.as_str().is_some_and(Self::valid_secret_key))),
                            "secret_keys values must reference central configuration"
                        );
                        continue;
                    }
                    if key == "prepared_keys" {
                        ensure!(
                            value
                                .get("keys")
                                .and_then(Value::as_object)
                                .is_some_and(|keys| !keys.is_empty()
                                    && keys.len() <= 8
                                    && keys
                                        .values()
                                        .all(|value| value.as_str().is_some_and(Self::valid_secret_key))),
                            "prepared submission keys must reference central configuration"
                        );
                    }
                    if key == "secrets" {
                        if let Some(secrets) = value.as_object() {
                            ensure!(
                                secrets
                                    .values()
                                    .all(|value| value.as_str().is_some_and(Self::valid_secret_key)),
                                "secret values must reference central configuration"
                            );
                        } else {
                            ensure!(value.is_null(), "secret map required");
                        }
                    } else if key.ends_with("_secret") {
                        ensure!(
                            value.as_str().is_some_and(Self::valid_secret_key),
                            "invalid central secret key"
                        );
                    } else if key.ends_with("_env") {
                        anyhow::bail!("environment-variable references are forbidden in workflow configuration");
                    } else if key.contains("password")
                        || key.ends_with("token")
                        || key.ends_with("secret")
                        || key.ends_with("api_key")
                        || matches!(key.as_str(), "private_key" | "signing_key")
                        || key == "authorization"
                    {
                        ensure!(
                            value.is_null(),
                            "plaintext credentials are forbidden in workflow snapshots"
                        );
                    }
                    Self::validate_credentials(value)?;
                }
            }
            Value::Array(values) => {
                for value in values {
                    Self::validate_credentials(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn valid_secret_key(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value.as_bytes()[0].is_ascii_alphabetic()
            && value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    }
}
