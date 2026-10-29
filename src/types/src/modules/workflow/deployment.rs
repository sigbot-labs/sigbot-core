//! Shared validation before a workflow config is stored or deployed.
use super::workflow::WorkflowFlowInfo;
use crate::modules::messager::topic::WorkflowTopic;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;

pub struct WorkflowDeployment;
impl WorkflowDeployment {
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
                    "datafeed"
                        | "evaluator"
                        | "strategy"
                        | "order"
                        | "wallet"
                        | "notification"
                        | "exporter"
                        | "backtest"
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
    /// Persist only references to secret environment variables, never credential material.
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
                    if key == "secret_env" {
                        ensure!(
                            value.as_object().is_some_and(|values| values
                                .values()
                                .all(|value| value.as_str().is_some_and(Self::valid_env))),
                            "secret_env values must be environment names"
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
                                    && keys.values().all(|value| value.as_str().is_some_and(Self::valid_env))),
                            "prepared submission keys must reference host environment names"
                        );
                    }
                    if key == "secrets" {
                        if let Some(secrets) = value.as_object() {
                            ensure!(
                                secrets
                                    .iter()
                                    .all(|(key, value)| key.ends_with("_env")
                                        && value.as_str().is_some_and(Self::valid_env)),
                                "secret values must be environment references"
                            );
                        } else {
                            ensure!(value.is_null(), "secret map required");
                        }
                    } else if key.ends_with("_env") {
                        ensure!(
                            value.as_str().is_some_and(Self::valid_env),
                            "invalid secret environment reference"
                        );
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
    fn valid_env(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    }
}
