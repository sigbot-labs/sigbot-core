//! Converts a persisted Workflow snapshot into isolated service-host arguments.
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sigbot_types::modules::{
    messager::topic::{MessageScope, WorkflowEdge, WorkflowTopic},
    workflow::workflow::{WorkflowFlowInfo, WorkflowNodeInfo},
};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone)]
pub struct WorkerPlan {
    pub name: String,
    pub module: String,
    pub node_id: i64,
    pub command: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub persistent_logs: bool,
    pub service: Option<WorkerService>,
    pub service_targets: Vec<WorkerServiceTarget>,
}

#[derive(Clone, serde::Serialize)]
pub struct WorkerService {
    pub port: u16,
    pub client_workers: Vec<String>,
}

#[derive(Clone, serde::Serialize)]
pub struct WorkerServiceTarget {
    pub worker_name: String,
    pub port: u16,
}

impl WorkerPlan {
    pub fn from_snapshot(scope: &MessageScope, workflow_id: i64, run_id: &str, snapshot: &Value) -> Result<Vec<Self>> {
        scope.validate()?;
        ensure!(workflow_id > 0 && !run_id.is_empty(), "invalid workflow run identity");
        let flow: WorkflowFlowInfo = serde_json::from_value(
            snapshot
                .get("flow_json")
                .cloned()
                .context("run snapshot missing flow")?,
        )?;
        ensure!(
            !flow.nodes.is_empty() && flow.nodes.len() <= 128,
            "workflow node count out of bounds"
        );
        let ids: HashSet<i64> = flow.nodes.iter().map(|node| node.id).collect();
        ensure!(
            ids.len() == flow.nodes.len() && ids.iter().all(|id| *id > 0),
            "node IDs must be unique and positive"
        );
        let mut edges = Vec::new();
        let mut account_inputs: BTreeMap<i64, BTreeMap<String, Vec<i64>>> = BTreeMap::new();
        for connection in &flow.connections {
            let source: i64 = connection.source.parse()?;
            let target: i64 = connection.target.parse()?;
            ensure!(
                source != target && ids.contains(&source) && ids.contains(&target),
                "invalid workflow edge"
            );
            let node = flow
                .nodes
                .iter()
                .find(|node| node.id == source)
                .expect("validated source");
            let module = Self::module(node)?;
            let target_module = Self::module(
                flow.nodes
                    .iter()
                    .find(|node| node.id == target)
                    .expect("validated target"),
            )?;
            if target_module == "strategy" && connection.kind.is_none() {
                let keys: &[&str] = match module.as_str() {
                    "order" => &["order_node_ids", "execution_node_ids"],
                    "wallet" => &["wallet_node_ids"],
                    "backtest" => &["execution_node_ids"],
                    _ => &[],
                };
                if !keys.is_empty() {
                    for key in keys {
                        account_inputs
                            .entry(target)
                            .or_default()
                            .entry((*key).to_owned())
                            .or_default()
                            .push(source);
                    }
                    continue;
                }
            }
            let kinds = if let Some(kind) = &connection.kind {
                vec![serde_json::from_value(json!(kind))?]
            } else if module == "strategy" && target_module == "datafeed" {
                vec![WorkflowTopic::FeedSnapshotRequest, WorkflowTopic::ComputeRequest]
            } else if target_module == "notification" {
                vec![WorkflowTopic::NotificationRequest]
            } else if target_module == "exporter" {
                vec![WorkflowTopic::ExportRequest]
            } else {
                match module.as_str() {
                    "datafeed" => vec![
                        WorkflowTopic::FeedNews,
                        WorkflowTopic::FeedMarket,
                        WorkflowTopic::FeedStatus,
                        WorkflowTopic::FeedSnapshotResult,
                        WorkflowTopic::ComputeResult,
                    ],
                    "evaluator" => vec![WorkflowTopic::EvaluationSnapshot],
                    "strategy" => vec![WorkflowTopic::StrategySignal],
                    "order" | "wallet" => vec![WorkflowTopic::RuntimeEvent],
                    "notification" => vec![WorkflowTopic::NotificationResult],
                    "exporter" => vec![WorkflowTopic::ExportResult],
                    _ => vec![WorkflowTopic::RuntimeEvent],
                }
            };
            for kind in kinds {
                edges.push(WorkflowEdge {
                    source_node_id: source,
                    target_node_id: target,
                    kind,
                });
            }
        }
        let mut plans = flow
            .nodes
            .iter()
            .map(|node| {
                Self::node(
                    scope,
                    workflow_id,
                    run_id,
                    node,
                    &edges,
                    &account_inputs,
                    snapshot
                        .get("traceparent")
                        .and_then(Value::as_str)
                        .context("run traceparent missing")?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::connect_services(&mut plans)?;
        // Persistent MQTT subscriptions must exist before feeds publish their first event.
        plans.sort_by_key(|plan| match plan.module.as_str() {
            "backtest" => 0,
            "wallet" => 1,
            "order" => 2,
            "strategy" => 3,
            "evaluator" => 4,
            "notification" | "exporter" => 5,
            _ => 6,
        });
        Ok(plans)
    }

    /// HTTP execution uses the same stable worker name in Kubernetes DNS and
    /// Docker network DNS. Only declared Order execution owners may call it.
    fn connect_services(plans: &mut [Self]) -> Result<()> {
        for index in 0..plans.len() {
            if plans[index].module != "order" {
                continue;
            }
            let mut argument: Value = serde_json::from_slice(&STANDARD.decode(&plans[index].command[5])?)?;
            let raw = argument
                .pointer("/properties/runtime_config")
                .and_then(Value::as_str)
                .context("order runtime_config required")?;
            let mut runtime: Value = serde_json::from_str(raw)?;
            if !matches!(
                runtime.pointer("/exchange/mode").and_then(Value::as_str),
                Some("paper" | "simulated")
            ) {
                continue;
            }
            let owners = runtime
                .get("execution_node_ids")
                .and_then(Value::as_array)
                .context("simulated execution owner required")?;
            ensure!(owners.len() == 1, "simulated execution requires exactly one owner");
            let owner = owners[0].as_i64().context("invalid execution owner")?;
            let target = plans
                .iter()
                .position(|plan| plan.node_id == owner && plan.module == "backtest")
                .context("simulated execution owner must be a Backtest node in this Workflow")?;
            let port = plans[target]
                .service
                .as_ref()
                .context("Backtest HTTP service required")?
                .port;
            let name = plans[target].name.clone();
            runtime["exchange"]["endpoint"] = json!(format!("http://{name}:{port}"));
            argument["properties"]["runtime_config"] = json!(runtime.to_string());
            plans[index].command[5] = STANDARD.encode(serde_json::to_vec(&argument)?);
            plans[index].service_targets.push(WorkerServiceTarget {
                worker_name: name,
                port,
            });
            let client = plans[index].name.clone();
            plans[target].service.as_mut().unwrap().client_workers.push(client);
        }
        Ok(())
    }

    fn module(node: &WorkflowNodeInfo) -> Result<String> {
        let module = node
            .config
            .as_ref()
            .and_then(|config| config.get("module"))
            .and_then(Value::as_str)
            .context("node config.module required")?;
        ensure!(
            matches!(
                module,
                "datafeed" | "evaluator" | "strategy" | "order" | "wallet" | "notification" | "exporter" | "backtest"
            ),
            "unsupported service module"
        );
        Ok(module.to_owned())
    }

    fn node(
        scope: &MessageScope,
        workflow: i64,
        run: &str,
        node: &WorkflowNodeInfo,
        edges: &[WorkflowEdge],
        account_inputs: &BTreeMap<i64, BTreeMap<String, Vec<i64>>>,
        traceparent: &str,
    ) -> Result<Self> {
        let module = Self::module(node)?;
        let mut persistent_logs = false;
        let config = node.config.as_ref().context("node configuration required")?;
        let raw = config.get("argument_json").context("node argument_json required")?;
        let mut argument: Value = if let Some(text) = raw.as_str() {
            serde_json::from_str(text)?
        } else {
            raw.clone()
        };
        Self::validate_credentials(&argument)?;
        ensure!(
            argument
                .get("sys_environment")
                .and_then(Value::as_object)
                .is_none_or(Map::is_empty),
            "user process environment is forbidden"
        );
        let identity = json!({"scope":scope,"workflow_id":workflow,"node_id":node.id,"run_id":run});
        let config_key = match module.as_str() {
            "datafeed" => Some("datafeed_config"),
            "notification" => Some("notification_config"),
            _ => None,
        };
        let property_prefix = if module == "datafeed" {
            argument
                .pointer("/datafeed_config/provider")
                .and_then(Value::as_str)
                .context("datafeed provider required")?
                .to_ascii_lowercase()
        } else {
            module.clone()
        };
        let inner = if let Some(key) = config_key {
            argument.get_mut(key).context("service configuration missing")?
        } else {
            &mut argument
        };
        let object = inner
            .as_object_mut()
            .context("service configuration must be an object")?;
        let properties = object
            .entry("properties")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .context("properties must be an object")?;
        let service = if module == "backtest" {
            let bind: std::net::SocketAddr = properties
                .get("bind_addr")
                .and_then(Value::as_str)
                .context("Backtest HTTP bind_addr required")?
                .parse()?;
            ensure!(
                bind.port() >= 1024 && bind.port() != 19090,
                "invalid Backtest service port"
            );
            properties.insert("bind_addr".into(), json!(format!("0.0.0.0:{}", bind.port())));
            Some(WorkerService {
                port: bind.port(),
                client_workers: Vec::new(),
            })
        } else {
            None
        };
        let incoming = edges
            .iter()
            .filter(|edge| edge.target_node_id == node.id)
            .map(|edge| edge.source_node_id)
            .collect::<HashSet<_>>();
        let mut inputs = incoming.into_iter().collect::<Vec<_>>();
        inputs.sort_unstable();
        for (key, value) in [
            ("runtime_v2", "true".to_owned()),
            ("env", scope.env.clone()),
            ("tenant_id", scope.tenant_id.to_string()),
            ("space_id", scope.space_id.to_string()),
            ("workflow_id", workflow.to_string()),
            ("node_id", node.id.to_string()),
            ("run_id", run.to_owned()),
            (
                "input_node_ids",
                inputs.iter().map(i64::to_string).collect::<Vec<_>>().join(","),
            ),
        ] {
            // Only the provider's explicit scope alias may be overwritten.
            // Arbitrary suffix matching would corrupt token_env/database_url_env.
            for (existing, item) in properties.iter_mut() {
                if existing == &format!("{property_prefix}_{key}")
                    || (property_prefix == "truthsocial" && existing == &format!("trushsocial_{key}"))
                {
                    *item = Value::String(value.clone());
                }
            }
            properties.insert(key.into(), Value::String(value));
        }
        if module == "datafeed" {
            properties.insert("traceparent".into(), json!(traceparent));
        }
        for key in ["runtime_config", "matching_config", "matching_feed_config"] {
            if let Some(raw) = properties.get(key).and_then(Value::as_str) {
                let mut runtime: Value = serde_json::from_str(raw)?;
                Self::validate_credentials(&runtime)?;
                let runtime_object = runtime.as_object_mut().context("runtime config must be an object")?;
                if key == "matching_feed_config" {
                    ensure!(module == "backtest", "matching feed belongs to the execution simulator");
                    runtime_object.insert("workflow_id".into(), json!(workflow));
                    runtime_object.insert("run_id".into(), json!(run));
                    runtime_object.insert("input_node_ids".into(), json!(inputs));
                } else if module == "strategy" {
                    if let Some(archive) = runtime_object.get_mut("log_archive").filter(|value| !value.is_null()) {
                        archive
                            .as_object_mut()
                            .context("log archive must be an object")?
                            .insert("spool_path".into(), json!("/var/lib/sigbot/logs/archive.sqlite"));
                        persistent_logs = true;
                    }
                    runtime_object.insert("identity".into(), identity.clone());
                    runtime_object.insert("edges".into(), serde_json::to_value(edges)?);
                    for key in ["execution_node_ids", "order_node_ids", "wallet_node_ids"] {
                        let nodes = account_inputs
                            .get(&node.id)
                            .and_then(|inputs| inputs.get(key))
                            .cloned()
                            .unwrap_or_default();
                        runtime_object.insert(key.into(), json!(nodes));
                    }
                } else {
                    runtime_object.insert("scope".into(), serde_json::to_value(scope)?);
                    runtime_object.insert("node_id".into(), json!(node.id));
                    if matches!(module.as_str(), "order" | "notification" | "exporter") {
                        runtime_object.insert("workflow_id".into(), json!(workflow));
                        runtime_object.insert("input_node_ids".into(), json!(inputs));
                    }
                }
                properties.insert(key.into(), json!(serde_json::to_string(&runtime)?));
            }
        }
        let name = Self::name(scope, workflow, node.id);
        let mqtt = argument
            .get_mut("messager_config")
            .and_then(Value::as_object_mut)
            .context("messager configuration required")?;
        let props = mqtt
            .entry("properties")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .context("MQTT properties required")?;
        let run_hash = format!("{:x}", Sha256::digest(run.as_bytes()));
        props.insert("mqtt_client_id".into(), json!(format!("{name}-{}", &run_hash[..16])));
        props.insert("mqtt_qos".into(), json!("1"));
        props.insert("mqtt_clean_start".into(), json!("false"));
        let flag = match module.as_str() {
            "datafeed" => "datafeed-configuration",
            "strategy" => "strategy-runner-configuration",
            "evaluator" => "evaluator-runner-configuration",
            "notification" => "notification-configuration",
            "order" => "order-manager-configuration",
            "wallet" => "wallet-manager-configuration",
            "exporter" => "exporter-manager-configuration",
            _ => "backtest-manager-configuration",
        };
        let mut command = vec![
            "sigbot".into(),
            module.clone(),
            "--messager-provider".into(),
            "MQTT".into(),
            format!("--{flag}"),
            STANDARD.encode(serde_json::to_vec(&argument)?),
        ];
        if module == "datafeed" {
            command.extend([
                "--datafeed-providers".into(),
                argument
                    .pointer("/datafeed_config/provider")
                    .and_then(Value::as_str)
                    .context("datafeed provider required")?
                    .to_owned(),
            ]);
        }
        let environment = BTreeMap::from([
            (
                "SIGBOT__AUTHGUARD__SERVICE_TENANT_ID".into(),
                scope.tenant_id.to_string(),
            ),
            ("SIGBOT__AUTHGUARD__SERVICE_SPACE_ID".into(), scope.space_id.to_string()),
            ("SIGBOT__AUTHGUARD__SERVICE_ENV".into(), scope.env.clone()),
            (
                "SIGBOT__AUTHGUARD__SERVICE_PRINCIPAL_ID".into(),
                format!("worker:{name}"),
            ),
            ("SIGBOT__MGMT__HOST".into(), "0.0.0.0".into()),
            ("SIGBOT__MGMT__PORT".into(), "19090".into()),
        ]);
        Ok(Self {
            name,
            module,
            node_id: node.id,
            command,
            environment,
            persistent_logs,
            service,
            service_targets: Vec::new(),
        })
    }

    pub fn log_volume_name(&self, prefix: &str) -> Result<String> {
        ensure!(
            self.persistent_logs && !prefix.is_empty(),
            "durable worker log volume prefix required"
        );
        let name = format!("{prefix}{}", self.name);
        ensure!(
            name.len() <= 63
                && name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                && name.as_bytes()[0].is_ascii_alphanumeric(),
            "invalid persistent log volume name"
        );
        Ok(name)
    }

    pub fn name(scope: &MessageScope, workflow: i64, node: i64) -> String {
        let digest = Sha256::digest(format!(
            "{}:{}:{}:{workflow}:{node}",
            scope.env, scope.tenant_id, scope.space_id
        ));
        format!("sigbot-worker-{:x}", digest)[..38].to_owned()
    }

    pub fn specification_hash(&self, image: &str, settings: &impl serde::Serialize) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                image,
                &self.command,
                &self.environment,
                &self.service,
                &self.service_targets,
                settings
            ))?)
        ))
    }

    pub fn validate_credentials(value: &Value) -> Result<()> {
        sigbot_types::modules::workflow::deployment::WorkflowDeployment::validate_credentials(value)
    }
}
