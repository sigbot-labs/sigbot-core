//! Kubernetes service hosts use server-side apply and a single replica per decision owner.
use crate::{
    deployer_factory::ISigbotDeployer,
    worker_plan::WorkerPlan,
    worker_reconciler::{WorkerHealth, WorkerProvider, WorkerReconciler, WorkerScheduler},
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{PersistentVolumeClaim, Service},
    networking::v1::NetworkPolicy,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams},
    Client,
};
use serde_json::json;
use sigbot_core::context::state::SigbotState;
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotKubernetesDeployer {
    state: Arc<SigbotState>,
    client: Client,
    scheduler: WorkerScheduler,
}
impl SigbotKubernetesDeployer {
    pub const NAME: &'static str = "KUBERNETES";
    pub async fn new(cron: Option<String>, channels: Option<usize>, state: Option<Arc<SigbotState>>) -> Arc<Self> {
        // Exchange SDKs can enable both rustls backends. Select a provider
        // explicitly before kube builds TLS, retaining any host-selected one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        Arc::new(Self {
            state: state.expect("controller state required"),
            client: Client::try_default().await.expect("Kubernetes configuration required"),
            scheduler: WorkerScheduler::new(cron, channels),
        })
    }
    pub fn manifest(plan: &WorkerPlan, config: &sigbot_core::config::config::DeployerProperties) -> Result<Deployment> {
        let settings = &config.workers;
        ensure!(
            !settings.namespace.is_empty()
                && !settings.runtime_secret.is_empty()
                && !settings.runtime_config_map.is_empty(),
            "worker namespace and existing runtime Secret/ConfigMap required"
        );
        let labels = json!({"app.kubernetes.io/managed-by":"sigbot-deployer","sigbot.io/worker":plan.name,"sigbot.io/module":plan.module});
        let env = plan
            .environment
            .iter()
            .map(|(name, value)| json!({"name":name,"value":value}))
            .collect::<Vec<_>>();
        let mut manifest = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": plan.name, "namespace": settings.namespace, "labels": labels},
            "spec": {
                "replicas": 1,
                "revisionHistoryLimit": 2,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"sigbot.io/worker": plan.name}},
                "template": {
                    "metadata": {
                        "labels": labels,
                        "annotations": {"sigbot.io/specification": plan.specification_hash(&config.images.sigbot, settings)?}
                    },
                    "spec": {
                        "automountServiceAccountToken": false,
                        "serviceAccountName": settings.service_account,
                        "terminationGracePeriodSeconds": 30,
                        "securityContext": {
                            "runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000,
                            "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}
                        },
                        "containers": [{
                            "name": "host", "image": config.images.sigbot, "command": plan.command, "env": env,
                            "envFrom": [
                                {"configMapRef": {"name": settings.runtime_config_map}},
                                {"secretRef": {"name": settings.runtime_secret}}
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
                                "capabilities": {"drop": ["ALL"]}
                            },
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "64Mi"},
                                "limits": {"cpu": format!("{}m", settings.cpu_millis), "memory": format!("{}Mi", settings.memory_mb)}
                            },
                            "ports": [{"name": "management", "containerPort": 19090}],
                            "readinessProbe": {
                                "httpGet": {"path": "/_/healthz", "port": "management"},
                                "initialDelaySeconds": 3, "periodSeconds": 5, "timeoutSeconds": 5
                            },
                            "livenessProbe": {
                                "httpGet": {"path": "/_/livez", "port": "management"},
                                "initialDelaySeconds": 30, "periodSeconds": 10, "timeoutSeconds": 1, "failureThreshold": 3
                            },
                            "volumeMounts": [
                                {"name": "scratch", "mountPath": "/tmp"},
                                {"name": "storage", "mountPath": "/var/lib/sigbot/storage"}
                            ]
                        }],
                        "volumes": [
                            {"name": "scratch", "emptyDir": {"sizeLimit": "64Mi"}},
                            {"name": "storage", "emptyDir": {"sizeLimit": "128Mi"}}
                        ]
                    }
                }
            }
        });
        if plan.persistent_logs {
            let claim = plan.log_volume_name(&settings.log_pvc_prefix)?;
            manifest
                .pointer_mut("/spec/template/spec/volumes")
                .unwrap()
                .as_array_mut()
                .unwrap()
                .push(json!({"name":"logs","persistentVolumeClaim":{"claimName":claim}}));
            manifest
                .pointer_mut("/spec/template/spec/containers/0/volumeMounts")
                .unwrap()
                .as_array_mut()
                .unwrap()
                .push(json!({"name":"logs","mountPath":"/var/lib/sigbot/logs"}));
        }
        Ok(serde_json::from_value(manifest)?)
    }
    pub fn service(plan: &WorkerPlan, namespace: &str) -> Result<Option<Service>> {
        let Some(service) = &plan.service else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {
                "name": plan.name, "namespace": namespace,
                "labels": {"app.kubernetes.io/managed-by": "sigbot-deployer", "sigbot.io/worker": plan.name}
            },
            "spec": {
                "type": "ClusterIP", "selector": {"sigbot.io/worker": plan.name},
                "ports": [{"name": "execution", "protocol": "TCP", "port": service.port, "targetPort": service.port}]
            }
        }))?))
    }

    pub fn network_policy(
        plan: &WorkerPlan,
        settings: &sigbot_core::config::config::WorkerDeployProperties,
    ) -> Result<NetworkPolicy> {
        let mut egress = vec![
            json!({"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"kube-system"}},"podSelector":{"matchLabels":{"k8s-app":"kube-dns"}}}],"ports":[{"protocol":"UDP","port":53},{"protocol":"TCP","port":53}]}),
        ];
        if !settings.egress_cidrs.is_empty() {
            ensure!(
                !settings.egress_ports.is_empty(),
                "explicit worker egress ports required"
            );
            egress.push(json!({"to":settings.egress_cidrs.iter().map(|cidr|json!({"ipBlock":{"cidr":cidr}})).collect::<Vec<_>>(),"ports":settings.egress_ports.iter().map(|port|json!({"protocol":"TCP","port":port})).collect::<Vec<_>>() }));
        }
        // Pod selectors without namespace selectors are restricted to this
        // namespace; service routing never opens an arbitrary tenant subnet.
        for target in &plan.service_targets {
            egress.push(
                json!({"to":[{"podSelector":{"matchLabels":{"sigbot.io/worker":target.worker_name}}}],
                "ports":[{"protocol":"TCP","port":target.port}]}),
            );
        }
        let ingress = plan.service.iter().filter(|service| !service.client_workers.is_empty()).map(|service| {
            json!({"from":service.client_workers.iter().map(|worker|json!({"podSelector":{"matchLabels":{"sigbot.io/worker":worker}}})).collect::<Vec<_>>(),
                "ports":[{"protocol":"TCP","port":service.port}]})
        }).collect::<Vec<_>>();
        Ok(serde_json::from_value(
            json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":plan.name,"namespace":settings.namespace,"labels":{"app.kubernetes.io/managed-by":"sigbot-deployer","sigbot.io/worker":plan.name}},"spec":{"podSelector":{"matchLabels":{"sigbot.io/worker":plan.name}},"policyTypes":["Ingress","Egress"],"ingress":ingress,"egress":egress}}),
        )?)
    }
    async fn remove_service(&self, plan: &WorkerPlan) -> Result<()> {
        let services: Api<Service> = Api::namespaced(
            self.client.clone(),
            &self.state.config.services.deployer.workers.namespace,
        );
        if let Some(service) = services.get_opt(&plan.name).await? {
            Self::require_ownership(&service.metadata, plan)?;
            match services.delete(&plan.name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(response)) if response.code == 404 => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    fn require_ownership(metadata: &ObjectMeta, plan: &WorkerPlan) -> Result<()> {
        ensure!(
            metadata.labels.as_ref().is_some_and(|labels| labels
                .get("app.kubernetes.io/managed-by")
                .map(String::as_str)
                == Some("sigbot-deployer")
                && labels.get("sigbot.io/worker") == Some(&plan.name)),
            "refusing to alter a Kubernetes object outside this worker controller"
        );
        Ok(())
    }
}
#[async_trait]
impl WorkerProvider for SigbotKubernetesDeployer {
    async fn apply(&self, plan: &WorkerPlan) -> Result<WorkerHealth> {
        if plan.persistent_logs {
            let settings = &self.state.config.services.deployer.workers;
            let claims: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &settings.namespace);
            let claim = claims
                .get(&plan.log_volume_name(&settings.log_pvc_prefix)?)
                .await
                .context("pre-provisioned durable worker log claim missing")?;
            ensure!(
                claim
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("sigbot.io/worker"))
                    == Some(&plan.name),
                "log claim belongs to a different worker"
            );
        }
        let namespace = &self.state.config.services.deployer.workers.namespace;
        let policies: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), namespace);
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let params = PatchParams::apply("sigbot-deployer");
        let old = deployments.get_opt(&plan.name).await?;
        if let Some(old) = &old {
            Self::require_ownership(&old.metadata, plan)?;
        }
        if let Some(policy) = policies.get_opt(&plan.name).await? {
            Self::require_ownership(&policy.metadata, plan)?;
        }
        if let Some(service) = services.get_opt(&plan.name).await? {
            Self::require_ownership(&service.metadata, plan)?;
        }
        policies
            .patch(
                &plan.name,
                &params,
                &Patch::Apply(Self::network_policy(
                    plan,
                    &self.state.config.services.deployer.workers,
                )?),
            )
            .await
            .context("worker NetworkPolicy apply failed")?;
        if let Some(service) = Self::service(plan, namespace)? {
            services
                .patch(&plan.name, &params, &Patch::Apply(service))
                .await
                .context("worker Service apply failed")?;
        } else {
            self.remove_service(plan).await?;
        }
        let desired = Self::manifest(plan, &self.state.config.services.deployer)?;
        let result = deployments
            .patch(&plan.name, &params, &Patch::Apply(desired))
            .await
            .context("worker Deployment apply failed")?;
        let updated = old
            .as_ref()
            .is_some_and(|old| old.metadata.generation != result.metadata.generation);
        let status = result.status;
        let ready = status.as_ref().is_some_and(|status| {
            status.observed_generation == result.metadata.generation
                && status.ready_replicas == Some(1)
                && status.updated_replicas == Some(1)
        });
        Ok(WorkerHealth {
            ready,
            restarted: updated,
        })
    }
    async fn remove(&self, plan: &WorkerPlan) -> Result<bool> {
        let namespace = &self.state.config.services.deployer.workers.namespace;
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        if let Some(deployment) = deployments.get_opt(&plan.name).await? {
            Self::require_ownership(&deployment.metadata, plan)?;
            let params = DeleteParams::foreground();
            match deployments.delete(&plan.name, &params).await {
                Ok(_) => {}
                Err(kube::Error::Api(response)) if response.code == 404 => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(false);
        }
        self.remove_service(plan).await?;
        let policies: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), namespace);
        if let Some(policy) = policies.get_opt(&plan.name).await? {
            Self::require_ownership(&policy.metadata, plan)?;
        }
        match policies.delete(&plan.name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 404 => {}
            Err(error) => return Err(error.into()),
        }
        Ok(true)
    }
}
#[async_trait]
impl ISigbotDeployer for SigbotKubernetesDeployer {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    async fn startup(&self) {
        let provider = Arc::new(self.clone());
        let reconciler = Arc::new(
            WorkerReconciler::new(self.state.clone(), provider).expect("invalid Kubernetes controller configuration"),
        );
        self.scheduler
            .start(reconciler)
            .await
            .expect("Kubernetes controller startup failed");
    }
    async fn shutdown(&self) {
        self.scheduler.stop().await;
    }
}
