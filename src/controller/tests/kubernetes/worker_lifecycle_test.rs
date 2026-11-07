//! Actual kube API, product containers, PostgreSQL, EMQX and restricted Python.
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::DeleteParams;
use kube::api::ListParams;
use kube::api::PostParams;
use kube::Api;
use kube::Client;
use serde_json::json;
use serde_json::Value;
use sigbot_controller::kubernetes::kubernetes::KubernetesController;
use sigbot_controller::worker::plan::WorkerPlan;
use sigbot_controller::worker::reconciler::WorkerProvider;
use sigbot_core::config::config::AppConfig;
use sigbot_core::config::config::AppConfigProperties;
use sigbot_core::context::state::SigbotState;
use sigbot_types::controller::{
    TenantMiddlewareConfiguration, WorkerAuthConfig, WorkerConfiguration, WorkerInfo, WorkerProvider as WorkerKind,
    WorkerStatus,
};
use sigbot_types::Uuid;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

struct Fixture {
    state: Arc<SigbotState>,
    provider: Arc<KubernetesController>,
    target: WorkerInfo,
    client: Client,
    pool: PgPool,
    workflow: Uuid,
    run: Uuid,
    snapshot: Value,
    plans: Vec<WorkerPlan>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        ensure!(
            std::env::var("KUBECONFIG")? == "/etc/rancher/k3s/k3s.yaml",
            "explicit local k3s configuration required"
        );
        let config: AppConfigProperties =
            serde_json::from_slice(&std::fs::read(std::env::var("MARKETPULSE_WORKER_CONFIG_PATH")?)?)?;
        let namespace = std::env::var("MARKETPULSE_WORKER_NAMESPACE")?;
        ensure!(
            namespace.starts_with("sigbot-marketpulse-"),
            "isolated test namespace required"
        );
        let scope = config.authguard.message_scope()?;
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let snapshot: Value =
            serde_json::from_slice(&std::fs::read(std::env::var("MARKETPULSE_WORKER_SNAPSHOT_PATH")?)?)?;
        let egress_cidrs: Vec<String> = serde_json::from_str(&std::env::var("MARKETPULSE_WORKER_EGRESS_CIDRS")?)?;
        let workflow = Uuid::now_v7();
        sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name,dag_config) VALUES($1,$2,$3,'Kubernetes provider acceptance',$4)")
            .bind(workflow).bind(&scope.env).bind(scope.tenant_id).bind(&snapshot["dag_config"]).execute(&pool).await?;
        let run = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state,params) VALUES($1,$2,$3,$4,'running',$5)",
        )
        .bind(run)
        .bind(workflow)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(&snapshot)
        .execute(&pool)
        .await?;
        let plans = WorkerPlan::from_snapshot(&scope, workflow, &run, &snapshot)?;
        ensure!(
            plans.len() == 2
                && plans.iter().any(|plan| plan.module == "strategy")
                && plans.iter().any(|plan| plan.module == "datafeed"),
            "real feed and strategy hosts required"
        );
        let state = Arc::new(SigbotState::new(&AppConfig::new(&config)).await);
        let client = Client::try_default().await?;
        let provider = Arc::new(KubernetesController::for_client(state.clone(), client.clone()));
        let now = Utc::now();
        let target = WorkerInfo {
            id: Uuid::now_v7(),
            env: scope.env,
            tenant_id: scope.tenant_id,
            workflow_id: Some(workflow),
            name: "local-k3s".into(),
            provider: WorkerKind::Kubernetes,
            conn_url: "https://127.0.0.1:6443".into(),
            auth_config: WorkerAuthConfig::KubernetesJwt {
                token_secret: "marketpulse_kubernetes_token".into(),
                certificate_authority_secret: None,
            },
            configuration: WorkerConfiguration::Kubernetes {
                namespace,
                runtime_secret: "sigbot-runtime".into(),
                runtime_config_map: "sigbot-runtime".into(),
                service_account: "sigbot-worker".into(),
                secret_revision: "1".into(),
                log_pvc_prefix: "sigbot-marketpulse-logs-".into(),
                egress_cidrs,
                egress_ports: vec![1883, 5432, 6379, 8080, 443],
            },
            middleware: TenantMiddlewareConfiguration::default(),
            labels: BTreeMap::new(),
            status: WorkerStatus::Online,
            priority: 100,
            max_concurrent_workflows: 16,
            heartbeat_at: Some(now),
            created_at: now,
            created_by: "worker-lifecycle-test".into(),
            updated_at: now,
            updated_by: "worker-lifecycle-test".into(),
        };
        Ok(Self {
            state,
            provider,
            target,
            client,
            pool,
            workflow,
            run,
            snapshot,
            plans,
        })
    }
    fn deployments(&self) -> Api<Deployment> {
        Api::namespaced(self.client.clone(), self.namespace())
    }
    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), self.namespace())
    }
    fn namespace(&self) -> &str {
        match &self.target.configuration {
            WorkerConfiguration::Kubernetes { namespace, .. } => namespace,
            _ => unreachable!("Kubernetes lifecycle target"),
        }
    }
    async fn ready(&self, old: Option<(&str, &str)>) -> Result<BTreeMap<String, (String, i64)>> {
        let deadline = Instant::now() + Duration::from_secs(150);
        loop {
            let mut observations = BTreeMap::new();
            for plan in &self.plans {
                let health = self.provider.apply(&self.target, plan).await?;
                if !health.ready {
                    break;
                }
                let deployment = self.deployments().get(&plan.name).await?;
                let pods = self
                    .pods()
                    .list(&ListParams::default().labels(&format!("sigbot.io/worker={}", plan.name)))
                    .await?;
                let running = pods
                    .items
                    .iter()
                    .filter(|pod| pod.metadata.deletion_timestamp.is_none())
                    .collect::<Vec<_>>();
                ensure!(running.len() <= 1, "more than one decision owner");
                let Some(pod) = running.first() else {
                    break;
                };
                let uid = pod.metadata.uid.as_deref().context("Pod UID missing")?;
                if old.is_some_and(|(name, old_uid)| name == plan.name && old_uid == uid) {
                    break;
                }
                let ready = pod
                    .status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .is_some_and(|conditions| {
                        conditions
                            .iter()
                            .any(|condition| condition.type_ == "Ready" && condition.status == "True")
                    });
                if !ready {
                    break;
                }
                observations.insert(
                    plan.name.clone(),
                    (
                        uid.to_owned(),
                        deployment.metadata.generation.context("generation missing")?,
                    ),
                );
            }
            if observations.len() == self.plans.len() {
                return Ok(observations);
            }
            ensure!(
                Instant::now() < deadline,
                "product workers did not become ready within 150 seconds"
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    async fn checkpoint(&self) -> Result<(i64, i64)> {
        let strategy = self.plans.iter().find(|plan| plan.module == "strategy").unwrap();
        let row = sqlx::query("SELECT version,COALESCE((state#>>'{state,user,callbacks}')::bigint,0) AS callbacks FROM wf_checkpoint WHERE workflow_id=$1 AND run_id=$2 AND node_id=$3 AND partition_key='decision'")
            .bind(self.workflow).bind(&self.run).bind(strategy.node_id).fetch_optional(&self.pool).await?;
        Ok(row
            .map(|row| (row.get("version"), row.get("callbacks")))
            .unwrap_or((0, 0)))
    }
    async fn verify(&mut self) -> Result<()> {
        let before = self.ready(None).await?;
        ensure!(self.ready(None).await? == before, "idempotent apply replaced a worker");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.checkpoint().await?.1 > 0 {
                break;
            }
            ensure!(Instant::now() < deadline, "no real Python callback committed");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let deliveries: i64 =
            sqlx::query_scalar("SELECT count(*) FROM mkt_delivery WHERE workflow_id=$1 AND run_id=$2")
                .bind(self.workflow)
                .bind(&self.run)
                .fetch_one(&self.pool)
                .await?;
        ensure!(deliveries > 0, "Datafeed must persist an actual source observation");
        let checkpoint = self.checkpoint().await?;
        self.provider = Arc::new(KubernetesController::for_client(
            self.state.clone(),
            self.client.clone(),
        ));
        ensure!(
            self.ready(None).await? == before,
            "controller restart replaced unchanged hosts"
        );
        let node = self.snapshot["dag_config"]["nodes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| node["config"]["module"] == "strategy")
            .unwrap();
        let raw = node["config"]["argument_json"]["properties"]["runtime_config"]
            .as_str()
            .context("runtime config missing")?;
        let mut runtime: Value = serde_json::from_str(raw)?;
        runtime["configuration"]["deployment_revision"] = json!(2);
        node["config"]["argument_json"]["properties"]["runtime_config"] = json!(runtime.to_string());
        self.plans = WorkerPlan::from_snapshot(
            &self.state.config.authguard.message_scope()?,
            self.workflow,
            &self.run,
            &self.snapshot,
        )?;
        let strategy = self
            .plans
            .iter()
            .find(|plan| plan.module == "strategy")
            .unwrap()
            .name
            .clone();
        let updated = self.ready(Some((&strategy, &before[&strategy].0))).await?;
        ensure!(
            updated[&strategy].1 > before[&strategy].1,
            "changed specification did not advance generation"
        );
        for plan in self.plans.iter().filter(|plan| plan.module != "strategy") {
            ensure!(updated[&plan.name] == before[&plan.name], "unrelated host was replaced");
        }
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&format!("sigbot.io/worker={strategy}")))
            .await?;
        let pod = pods
            .items
            .iter()
            .find(|pod| pod.metadata.deletion_timestamp.is_none())
            .unwrap();
        self.pods()
            .delete(pod.metadata.name.as_ref().unwrap(), &DeleteParams::default())
            .await?;
        let recovered = self.ready(Some((&strategy, &updated[&strategy].0))).await?;
        ensure!(
            recovered[&strategy].1 == updated[&strategy].1,
            "Pod recovery changed the Workflow specification"
        );
        let restored = self.checkpoint().await?;
        ensure!(
            restored.0 >= checkpoint.0 && restored.1 >= checkpoint.1,
            "durable strategy state regressed"
        );
        common_telemetry::info!(
            "workflow={} run={} deliveries={} callbacks={} deployment_generation={} recovered_generation={}",
            self.workflow,
            self.run,
            deliveries,
            restored.1,
            updated[&strategy].1,
            recovered[&strategy].1
        );
        self.stop().await?;
        // A name collision must never grant ownership over an existing object.
        let plan = &self.plans[0];
        let mut unmanaged =
            KubernetesController::manifest(plan, &self.state.config.services.platform.controller, &self.target)?;
        unmanaged
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("app.kubernetes.io/managed-by".into(), "unrelated-controller".into());
        unmanaged.spec.as_mut().unwrap().replicas = Some(0);
        self.deployments().create(&PostParams::default(), &unmanaged).await?;
        let apply_rejected = self.provider.apply(&self.target, plan).await.is_err();
        let remove_rejected = self.provider.remove(&self.target, plan).await.is_err();
        self.deployments()
            .delete(&plan.name, &DeleteParams::foreground())
            .await?;
        ensure!(
            apply_rejected && remove_rejected,
            "controller altered an unmanaged Deployment"
        );
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let mut removed = true;
            for plan in &self.plans {
                removed &= self.provider.remove(&self.target, plan).await?;
            }
            if removed {
                break;
            }
            ensure!(Instant::now() < deadline, "workers did not stop");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let namespace = self.namespace();
        let policies: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        for plan in &self.plans {
            ensure!(
                self.provider.remove(&self.target, plan).await?,
                "repeated stop must succeed"
            );
            ensure!(
                self.deployments().get_opt(&plan.name).await?.is_none()
                    && policies.get_opt(&plan.name).await?.is_none()
                    && services.get_opt(&plan.name).await?.is_none(),
                "owned worker resources remain"
            );
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires local k3s, real product image, EMQX, PostgreSQL and external-source fixture"]
async fn real_kubernetes_worker_lifecycle_and_scope() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let result = fixture.verify().await;
    if result.is_err() {
        for plan in &fixture.plans {
            let pods = fixture
                .pods()
                .list(&ListParams::default().labels(&format!("sigbot.io/worker={}", plan.name)))
                .await?;
            for pod in pods.items {
                common_telemetry::warn!(
                    "worker={} phase={:?} conditions={:?}",
                    plan.name,
                    pod.status.as_ref().and_then(|status| status.phase.as_ref()),
                    pod.status.as_ref().and_then(|status| status.conditions.as_ref())
                );
            }
        }
        fixture.stop().await?;
    }
    sqlx::query("UPDATE wf_run SET state='stopped',ended_at=now() WHERE run_id=$1")
        .bind(&fixture.run)
        .execute(&fixture.pool)
        .await?;
    result
}
