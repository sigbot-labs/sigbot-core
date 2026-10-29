use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use sigbot_deployer::{kubernetes::deployer_kubernetes::SigbotKubernetesDeployer, worker_plan::WorkerPlan};
use sigbot_types::modules::messager::topic::MessageScope;

struct Fixture;
impl Fixture {
    fn snapshot() -> Value {
        json!({"traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01","flow_json":{"nodes":[
            {"id":1,"name":"feed","status":null,"stage":"TWITTER@INPUT","position":{"x":0,"y":0},"style":null,"icon":null,"config":{"module":"datafeed","argument_json":{
                "messager_config":{"properties":{"mqtt_server":"broker"},"secrets":{"mqtt_password_env":"BROKER_PASSWORD"}},
                "datafeed_config":{"provider":"TWITTER","properties":{"tenant_id":"999","twitter_tenant_id":"999","database_url_env":"DATABASE_URL"}}}}},
            {"id":2,"name":"strategy","status":null,"stage":"PYCODE@EVALUATION","position":{"x":1,"y":0},"style":null,"icon":null,"config":{"module":"strategy","argument_json":{
                "messager_config":{"properties":{}},"run_mode":"V2","properties":{"runtime_config":json!({"identity":{"scope":{"env":"live","tenant_id":999,"space_id":999},"workflow_id":999,"node_id":999,"run_id":"forged"},"edges":[],"account_id":3,"class_name":"Strategy","code":"class Strategy: pass","configuration":{}}).to_string()}}}}
        ],"connections":[{"id":"edge","source":"1","target":"2"}]}})
    }
    fn plans() -> Vec<WorkerPlan> {
        WorkerPlan::from_snapshot(
            &MessageScope {
                env: "paper".into(),
                tenant_id: 7,
                space_id: 8,
            },
            9,
            "run-one",
            &Self::snapshot(),
        )
        .unwrap()
    }
    fn argument(plan: &WorkerPlan) -> Value {
        serde_json::from_slice(&STANDARD.decode(&plan.command[5]).unwrap()).unwrap()
    }
}
#[test]
fn snapshot_identity_cannot_be_overridden_and_edges_come_from_graph() {
    let plans = Fixture::plans();
    assert_eq!(plans[0].module, "strategy");
    let feed = Fixture::argument(&plans[1]);
    assert_eq!(feed["datafeed_config"]["properties"]["tenant_id"], "7");
    assert_eq!(feed["datafeed_config"]["properties"]["twitter_tenant_id"], "7");
    assert_eq!(
        feed["datafeed_config"]["properties"]["database_url_env"],
        "DATABASE_URL"
    );
    let strategy = Fixture::argument(&plans[0]);
    let runtime: Value = serde_json::from_str(strategy["properties"]["runtime_config"].as_str().unwrap()).unwrap();
    assert_eq!(runtime["identity"]["scope"]["tenant_id"], 7);
    assert_eq!(runtime["identity"]["run_id"], "run-one");
    assert_eq!(runtime["edges"].as_array().unwrap().len(), 5);
}
#[test]
fn reconciliation_is_stable_and_manifests_enforce_single_owner_and_secret_references() {
    let left = Fixture::plans();
    let right = Fixture::plans();
    let config = sigbot_core::config::config::DeployerProperties::default();
    for (left, right) in left.iter().zip(&right) {
        assert_eq!(left.command, right.command);
        assert_eq!(
            left.specification_hash("sigbot:test", &config.workers).unwrap(),
            right.specification_hash("sigbot:test", &config.workers).unwrap()
        );
        let manifest = serde_json::to_value(SigbotKubernetesDeployer::manifest(left, &config).unwrap()).unwrap();
        assert_eq!(manifest["spec"]["replicas"], 1);
        assert_eq!(manifest["spec"]["strategy"]["type"], "Recreate");
        assert_eq!(
            manifest["spec"]["template"]["spec"]["automountServiceAccountToken"],
            false
        );
        assert_eq!(
            manifest["spec"]["template"]["spec"]["containers"][0]["envFrom"][1]["secretRef"]["name"],
            "sigbot-runtime"
        );
        let container = &manifest["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["readinessProbe"]["httpGet"]["path"], "/_/healthz");
        assert_eq!(container["readinessProbe"]["timeoutSeconds"], 5);
        assert_eq!(container["livenessProbe"]["httpGet"]["path"], "/_/livez");
        assert_eq!(container["livenessProbe"]["httpGet"]["port"], "management");
        assert!(container["livenessProbe"]["tcpSocket"].is_null());
        let mut rotated = config.clone();
        rotated.workers.secret_revision = "2".into();
        let replacement = serde_json::to_value(SigbotKubernetesDeployer::manifest(left, &rotated).unwrap()).unwrap();
        assert_eq!(replacement["metadata"]["name"], manifest["metadata"]["name"]);
        assert_ne!(
            replacement["spec"]["template"]["metadata"]["annotations"],
            manifest["spec"]["template"]["metadata"]["annotations"]
        );
    }
}
#[test]
fn plaintext_secret_values_and_unknown_modules_are_rejected() {
    assert!(WorkerPlan::validate_credentials(&json!({"secrets":{"mqtt_password":"never-persist"}})).is_err());
    assert!(WorkerPlan::validate_credentials(&json!({"token":"never-persist"})).is_err());
    assert!(WorkerPlan::validate_credentials(&json!({"private_key":"never-persist"})).is_err());
    assert!(WorkerPlan::validate_credentials(
        &json!({"prepared_keys":{"active_key":"v1","keys":{"v1":"credential-material"}}})
    )
    .is_err());
    assert!(WorkerPlan::validate_credentials(
        &json!({"prepared_keys":{"active_key":"v1","keys":{"v1":"SUBMISSION_KEY_V1"}}})
    )
    .is_ok());
    let mut snapshot = Fixture::snapshot();
    snapshot["flow_json"]["nodes"][0]["config"]["module"] = json!("messager");
    assert!(WorkerPlan::from_snapshot(
        &MessageScope {
            env: "paper".into(),
            tenant_id: 7,
            space_id: 8
        },
        9,
        "run-one",
        &snapshot
    )
    .is_err());
}

#[test]
fn persistent_sessions_survive_restarts_but_do_not_share_queues_between_runs() {
    let first = Fixture::plans();
    let restarted = Fixture::plans();
    let next = WorkerPlan::from_snapshot(
        &MessageScope {
            env: "paper".into(),
            tenant_id: 7,
            space_id: 8,
        },
        9,
        "run-two",
        &Fixture::snapshot(),
    )
    .unwrap();
    for ((first, restarted), next) in first.iter().zip(&restarted).zip(&next) {
        let client =
            |plan: &WorkerPlan| Fixture::argument(plan)["messager_config"]["properties"]["mqtt_client_id"].clone();
        assert_eq!(first.name, next.name);
        assert_eq!(client(first), client(restarted));
        assert_ne!(client(first), client(next));
    }
}

#[test]
fn durable_log_ack_requires_an_existing_per_worker_volume_and_survives_run_replacement() {
    let mut snapshot = Fixture::snapshot();
    let raw = snapshot["flow_json"]["nodes"][1]["config"]["argument_json"]["properties"]["runtime_config"]
        .as_str()
        .unwrap();
    let mut runtime: Value = serde_json::from_str(raw).unwrap();
    runtime["log_archive"] = json!({"spool_path":"/tmp/unsafe.sqlite"});
    snapshot["flow_json"]["nodes"][1]["config"]["argument_json"]["properties"]["runtime_config"] =
        json!(runtime.to_string());
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: 7,
        space_id: 8,
    };
    let plans = WorkerPlan::from_snapshot(&scope, 9, "run-one", &snapshot).unwrap();
    let next = WorkerPlan::from_snapshot(&scope, 9, "run-two", &snapshot).unwrap();
    let plan = &plans[0];
    assert!(plan.persistent_logs);
    let mut config = sigbot_core::config::config::DeployerProperties::default();
    assert!(SigbotKubernetesDeployer::manifest(plan, &config).is_err());
    config.workers.log_pvc_prefix = "logs-".into();
    let manifest = serde_json::to_value(SigbotKubernetesDeployer::manifest(plan, &config).unwrap()).unwrap();
    let claim = plan.log_volume_name("logs-").unwrap();
    assert_eq!(next[0].log_volume_name("logs-").unwrap(), claim);
    let volume = manifest["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "logs")
        .unwrap();
    assert_eq!(volume["persistentVolumeClaim"]["claimName"], claim);
    assert!(volume.get("emptyDir").is_none());
    let argument = Fixture::argument(plan);
    let runtime: Value = serde_json::from_str(argument["properties"]["runtime_config"].as_str().unwrap()).unwrap();
    assert_eq!(
        runtime["log_archive"]["spool_path"],
        "/var/lib/sigbot/logs/archive.sqlite"
    );
}

#[test]
fn execution_service_dns_and_network_permissions_follow_the_workflow_owner() {
    let mut snapshot = Fixture::snapshot();
    let nodes = snapshot["flow_json"]["nodes"].as_array_mut().unwrap();
    nodes.push(json!({"id":3,"name":"matching","status":null,"stage":"DEFAULT@TRANSACTION","position":{"x":2,"y":0},"style":null,"icon":null,
        "config":{"module":"backtest","argument_json":{"messager_config":{"properties":{}},"properties":{"bind_addr":"127.0.0.1:18484"}}}}));
    nodes.push(json!({"id":4,"name":"order","status":null,"stage":"DEFAULT@TRANSACTION","position":{"x":3,"y":0},"style":null,"icon":null,
        "config":{"module":"order","argument_json":{"messager_config":{"properties":{}},"properties":{"runtime_config":
            json!({"execution_node_ids":[3],"exchange":{"mode":"simulated","endpoint":"http://127.0.0.1:18484"}}).to_string()}}}}));
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: 7,
        space_id: 8,
    };
    let plans = WorkerPlan::from_snapshot(&scope, 9, "run-one", &snapshot).unwrap();
    let execution = plans.iter().find(|plan| plan.node_id == 3).unwrap();
    let order = plans.iter().find(|plan| plan.node_id == 4).unwrap();
    assert_eq!(Fixture::argument(execution)["properties"]["bind_addr"], "0.0.0.0:18484");
    let runtime: Value = serde_json::from_str(
        Fixture::argument(order)["properties"]["runtime_config"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        runtime["exchange"]["endpoint"],
        format!("http://{}:18484", execution.name)
    );
    let config = sigbot_core::config::config::DeployerProperties::default();
    let service =
        serde_json::to_value(SigbotKubernetesDeployer::service(execution, "sigbot").unwrap().unwrap()).unwrap();
    assert_eq!(service["spec"]["type"], "ClusterIP");
    assert_eq!(service["spec"]["selector"]["sigbot.io/worker"], execution.name);
    let incoming =
        serde_json::to_value(SigbotKubernetesDeployer::network_policy(execution, &config.workers).unwrap()).unwrap();
    assert_eq!(incoming["spec"]["ingress"].as_array().unwrap().len(), 1);
    assert_eq!(incoming["spec"]["ingress"][0]["from"].as_array().unwrap().len(), 1);
    assert_eq!(
        incoming["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]["sigbot.io/worker"],
        order.name
    );
    assert_eq!(incoming["spec"]["ingress"][0]["ports"][0]["port"], 18484);
    let outgoing =
        serde_json::to_value(SigbotKubernetesDeployer::network_policy(order, &config.workers).unwrap()).unwrap();
    assert_eq!(outgoing["spec"]["ingress"], json!([]));
    assert_eq!(
        outgoing["spec"]["egress"][1]["to"][0]["podSelector"]["matchLabels"]["sigbot.io/worker"],
        execution.name
    );
    snapshot["flow_json"]["nodes"][3]["config"]["argument_json"]["properties"]["runtime_config"] =
        json!(json!({"execution_node_ids":[999],"exchange":{"mode":"paper"}}).to_string());
    assert!(WorkerPlan::from_snapshot(&scope, 9, "run-one", &snapshot).is_err());
}
