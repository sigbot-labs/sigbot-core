use serde_json::json;
use serde_json::Value;
use sigbot_controller::kubernetes::kubernetes::KubernetesController;
use sigbot_controller::worker::middleware::MiddlewareTopology;
use sigbot_controller::worker::plan::WorkerPlan;
use sigbot_types::controller::{
    MiddlewareDeployment, MiddlewareResources, TenantMiddlewareConfiguration, WorkerAuthConfig, WorkerConfiguration,
    WorkerInfo, WorkerProvider, WorkerStatus,
};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::collections::BTreeMap;

struct Fixture;
impl Fixture {
    fn tenant() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000301").unwrap()
    }
    fn workflow() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000302").unwrap()
    }
    fn run() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000303").unwrap()
    }
    fn next_run() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000304").unwrap()
    }
    fn snapshot() -> Value {
        json!({"execution_mode":"paper","traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01","dag_config":{"nodes":[
            {"id":1,"name":"feed","status":null,"stage":"TWITTER@INPUT","position":{"x":0,"y":0},"style":null,"icon":null,"config":{"module":"datafeed","argument_json":{
                "messager_config":{"properties":{"mqtt_server":"broker"},"secrets":{"mqtt_password_secret":"broker_password"}},
                "datafeed_config":{"provider":"TWITTER","properties":{"tenant_id":"999","twitter_tenant_id":"999","token_secret":"marketpulse_api_token"}}}}},
            {"id":2,"name":"strategy","status":null,"stage":"PYCODE@EVALUATION","position":{"x":1,"y":0},"style":null,"icon":null,"config":{"module":"strategy","argument_json":{
                "messager_config":{"properties":{}},"run_mode":"V2","properties":{"runtime_config":json!({"identity":{"scope":{"env":"live","tenant_id":"0192f7b2-5c00-7000-8000-000000000399"},"workflow_id":"0192f7b2-5c00-7000-8000-000000000398","node_id":999,"run_id":"0192f7b2-5c00-7000-8000-000000000397"},"edges":[],"account_id":"0192f7b2-5c00-7000-8000-000000000396","class_name":"Strategy","code":"class Strategy: pass","configuration":{}}).to_string()}}}}
        ],"connections":[{"id":"edge","source":"1","target":"2"}]}})
    }
    fn plans() -> Vec<WorkerPlan> {
        WorkerPlan::from_snapshot(
            &MessageScope {
                env: "paper".into(),
                tenant_id: Self::tenant(),
            },
            Self::workflow(),
            &Self::run(),
            &Self::snapshot(),
        )
        .unwrap()
    }
    fn argument(plan: &WorkerPlan) -> Value {
        plan.configuration["services"]["component"][&plan.module].clone()
    }
    fn messager(plan: &WorkerPlan) -> Value {
        plan.configuration["services"]["component"]["messager"].clone()
    }

    fn target() -> WorkerInfo {
        let now = chrono::Utc::now();
        WorkerInfo {
            id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000305").unwrap(),
            env: "paper".into(),
            tenant_id: Self::tenant(),
            workflow_id: Some(Self::workflow()),
            name: "test-kubernetes".into(),
            provider: WorkerProvider::Kubernetes,
            conn_url: "https://127.0.0.1:6443".into(),
            auth_config: WorkerAuthConfig::KubernetesJwt {
                token_secret: "test_kubernetes_token".into(),
                certificate_authority_secret: None,
            },
            configuration: WorkerConfiguration::Kubernetes {
                namespace: "sigbot".into(),
                runtime_secret: "sigbot-runtime".into(),
                runtime_config_map: "sigbot-runtime".into(),
                service_account: "sigbot-worker".into(),
                secret_revision: "1".into(),
                log_pvc_prefix: String::new(),
                egress_cidrs: Vec::new(),
                egress_ports: vec![1883, 5432, 6379, 443],
            },
            middleware: TenantMiddlewareConfiguration::default(),
            labels: BTreeMap::new(),
            status: WorkerStatus::Online,
            priority: 100,
            max_concurrent_workflows: 16,
            heartbeat_at: Some(now),
            created_at: now,
            created_by: "test".into(),
            updated_at: now,
            updated_by: "test".into(),
        }
    }
}
#[test]
fn snapshot_identity_cannot_be_overridden_and_edges_come_from_graph() {
    let plans = Fixture::plans();
    assert_eq!(plans[0].module, "strategy");
    let feed = Fixture::argument(&plans[1]);
    assert_eq!(feed["properties"]["tenant_id"], Fixture::tenant().to_string());
    assert_eq!(feed["properties"]["twitter_tenant_id"], Fixture::tenant().to_string());
    assert_eq!(feed["properties"]["token_secret"], "marketpulse_api_token");
    let strategy = Fixture::argument(&plans[0]);
    let runtime: Value = serde_json::from_str(strategy["properties"]["runtime_config"].as_str().unwrap()).unwrap();
    assert_eq!(runtime["identity"]["scope"]["tenant_id"], Fixture::tenant().to_string());
    assert_eq!(runtime["identity"]["run_id"], Fixture::run().to_string());
    assert_eq!(runtime["edges"].as_array().unwrap().len(), 5);
}
#[test]
fn reconciliation_is_stable_and_manifests_enforce_single_owner_and_secret_references() {
    let left = Fixture::plans();
    let right = Fixture::plans();
    let config = sigbot_core::config::config::ControllerProperties::default();
    let target = Fixture::target();
    for (left, right) in left.iter().zip(&right) {
        assert_eq!(left.command, right.command);
        assert_eq!(
            left.specification_hash("sigbot:test", &config.workers).unwrap(),
            right.specification_hash("sigbot:test", &config.workers).unwrap()
        );
        let manifest = serde_json::to_value(KubernetesController::manifest(left, &config, &target).unwrap()).unwrap();
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
        let mut rotated = target.clone();
        if let WorkerConfiguration::Kubernetes { secret_revision, .. } = &mut rotated.configuration {
            *secret_revision = "2".into();
        }
        let replacement =
            serde_json::to_value(KubernetesController::manifest(left, &config, &rotated).unwrap()).unwrap();
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
    snapshot["dag_config"]["nodes"][0]["config"]["module"] = json!("messager");
    assert!(WorkerPlan::from_snapshot(
        &MessageScope {
            env: "paper".into(),
            tenant_id: Fixture::tenant(),
        },
        Fixture::workflow(),
        &Fixture::run(),
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
            tenant_id: Fixture::tenant(),
        },
        Fixture::workflow(),
        &Fixture::next_run(),
        &Fixture::snapshot(),
    )
    .unwrap();
    for ((first, restarted), next) in first.iter().zip(&restarted).zip(&next) {
        let client = |plan: &WorkerPlan| Fixture::messager(plan)["properties"]["mqtt_client_id"].clone();
        assert_eq!(first.name, next.name);
        assert_eq!(client(first), client(restarted));
        assert_ne!(client(first), client(next));
    }
}

#[test]
fn tenant_middleware_endpoints_are_bound_after_worker_selection() {
    let mut target = Fixture::target();
    let deployment = |image: &str| MiddlewareDeployment {
        image: image.into(),
        resources: MiddlewareResources {
            cpu_millis: 500,
            memory_mb: 512,
            storage_gb: 8,
        },
    };
    target.middleware = TenantMiddlewareConfiguration {
        redis: Some(deployment("redis:test")),
        pgvector: Some(deployment("pgvector:test")),
        timescaledb: Some(deployment("timescaledb:test")),
        emqx: Some(deployment("emqx:test")),
    };
    let mut plans = Fixture::plans();
    for plan in &mut plans {
        MiddlewareTopology::new(&target).bind(plan).unwrap();
        assert_eq!(plan.environment["SIGBOT__CACHE__PROVIDER"], "Redis");
        assert_eq!(plan.environment["SIGBOT__STORAGE__PROVIDER"], "Postgres");
        assert!(plan.environment["SIGBOT__CACHE__REDIS__NODES"].starts_with("redis://sigbot-"));
        let messager = Fixture::messager(plan);
        assert!(messager["properties"]["mqtt_server"]
            .as_str()
            .unwrap()
            .starts_with("sigbot-"));
        assert_eq!(messager["properties"]["mqtt_port"], "1883");
    }
}

#[test]
fn durable_log_ack_requires_an_existing_per_worker_volume_and_survives_run_replacement() {
    let mut snapshot = Fixture::snapshot();
    let raw = snapshot["dag_config"]["nodes"][1]["config"]["argument_json"]["properties"]["runtime_config"]
        .as_str()
        .unwrap();
    let mut runtime: Value = serde_json::from_str(raw).unwrap();
    runtime["log_archive"] = json!({"spool_path":"/tmp/unsafe.sqlite"});
    snapshot["dag_config"]["nodes"][1]["config"]["argument_json"]["properties"]["runtime_config"] =
        json!(runtime.to_string());
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: Fixture::tenant(),
    };
    let plans = WorkerPlan::from_snapshot(&scope, Fixture::workflow(), &Fixture::run(), &snapshot).unwrap();
    let next = WorkerPlan::from_snapshot(&scope, Fixture::workflow(), &Fixture::next_run(), &snapshot).unwrap();
    let plan = &plans[0];
    assert!(plan.persistent_logs);
    let config = sigbot_core::config::config::ControllerProperties::default();
    let mut target = Fixture::target();
    assert!(KubernetesController::manifest(plan, &config, &target).is_err());
    if let WorkerConfiguration::Kubernetes { log_pvc_prefix, .. } = &mut target.configuration {
        *log_pvc_prefix = "logs-".into();
    }
    let manifest = serde_json::to_value(KubernetesController::manifest(plan, &config, &target).unwrap()).unwrap();
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
    snapshot["execution_mode"] = json!("backtest");
    let nodes = snapshot["dag_config"]["nodes"].as_array_mut().unwrap();
    nodes.push(json!({"id":3,"name":"matching","status":null,"stage":"DEFAULT@TRANSACTION","position":{"x":2,"y":0},"style":null,"icon":null,
        "config":{"module":"backtest","argument_json":{"messager_config":{"properties":{}},"properties":{"bind_addr":"127.0.0.1:18484"}}}}));
    nodes.push(json!({"id":4,"name":"order","status":null,"stage":"DEFAULT@TRANSACTION","position":{"x":3,"y":0},"style":null,"icon":null,
        "config":{"module":"order","argument_json":{"messager_config":{"properties":{}},"properties":{"runtime_config":
            json!({"execution_node_ids":[3],"exchange":{"mode":"backtest","endpoint":"http://127.0.0.1:18484"}}).to_string()}}}}));
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: Fixture::tenant(),
    };
    let deployment =
        WorkerPlan::deployment_from_snapshot(&scope, Fixture::workflow(), &Fixture::run(), &snapshot).unwrap();
    let execution = deployment.workers.iter().find(|plan| plan.node_id == 3).unwrap();
    let order = deployment.workers.iter().find(|plan| plan.node_id == 4).unwrap();
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
    let target = Fixture::target();
    let service = serde_json::to_value(KubernetesController::service(execution, "sigbot").unwrap().unwrap()).unwrap();
    assert_eq!(service["spec"]["type"], "ClusterIP");
    assert_eq!(service["spec"]["selector"]["sigbot.io/worker"], execution.name);
    let incoming = serde_json::to_value(KubernetesController::network_policy(execution, &target).unwrap()).unwrap();
    assert_eq!(incoming["spec"]["ingress"].as_array().unwrap().len(), 1);
    assert_eq!(incoming["spec"]["ingress"][0]["from"].as_array().unwrap().len(), 1);
    assert_eq!(
        incoming["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]["sigbot.io/module"],
        "order"
    );
    assert_eq!(incoming["spec"]["ingress"][0]["ports"][0]["port"], 18484);
    assert_eq!(deployment.workers.len(), 4);
    snapshot["dag_config"]["nodes"][3]["config"]["argument_json"]["properties"]["runtime_config"] =
        json!(json!({"execution_node_ids":[999],"exchange":{"mode":"paper"}}).to_string());
    assert!(WorkerPlan::from_snapshot(&scope, Fixture::workflow(), &Fixture::run(), &snapshot).is_err());
}

#[test]
fn worker_configuration_is_mounted_and_part_of_the_specification() {
    let mut plan = Fixture::plans()
        .into_iter()
        .find(|plan| plan.module == "datafeed")
        .unwrap();
    assert_eq!(
        plan.command,
        vec!["sigbot", "datafeed", "--config", "/etc/sigbot/runtime/sigbot.yaml"]
    );
    let settings = sigbot_core::config::config::AppConfigProperties::default();
    let loaded: sigbot_core::config::config::ComponentServicesProperties =
        serde_json::from_value(plan.configuration["services"]["component"].clone()).unwrap();
    assert_eq!(loaded.messager.provider.as_str(), "MQTT");
    let before = plan
        .specification_hash("test-image", &settings.services.platform.controller.workers)
        .unwrap();
    plan.configuration["services"]["component"]["messager"]["provider"] = json!("LOCAL");
    assert_ne!(
        before,
        plan.specification_hash("test-image", &settings.services.platform.controller.workers)
            .unwrap()
    );
    let manifest =
        KubernetesController::manifest(&plan, &settings.services.platform.controller, &Fixture::target()).unwrap();
    let value = serde_json::to_value(manifest).unwrap();
    assert_eq!(
        value.pointer("/spec/template/spec/volumes/0/configMap/name").unwrap(),
        &json!(plan.name)
    );
}
