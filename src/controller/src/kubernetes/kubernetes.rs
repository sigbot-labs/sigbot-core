//! Kubernetes service hosts use server-side apply and a single replica per decision owner.
use crate::worker::plan::WorkerPlan;
use crate::worker::reconciler::WorkerHealth;
use crate::worker::reconciler::WorkerProvider;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::Api;
use kube::api::DeleteParams;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_types::controller::{WorkerAuthConfig, WorkerConfiguration, WorkerInfo};
use std::sync::Arc;

pub(super) struct KubernetesRuntime<'a> {
    pub(super) namespace: &'a str,
    pub(super) runtime_secret: &'a str,
    pub(super) runtime_config_map: &'a str,
    pub(super) service_account: &'a str,
    pub(super) log_pvc_prefix: &'a str,
    pub(super) egress_cidrs: &'a [String],
    pub(super) egress_ports: &'a [u16],
}

#[derive(Clone)]
pub struct KubernetesController {
    state: Arc<SigbotState>,
    pub(super) client: Client,
}
impl KubernetesController {
    pub async fn for_worker(state: Arc<SigbotState>, target: &WorkerInfo) -> Result<Self> {
        // Dependencies may enable more than one rustls backend; retain an existing
        // process choice or install the backend used by kube before creating TLS.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = get_config();
        let (auth, certificate_authority) = match &target.auth_config {
            WorkerAuthConfig::KubernetesJwt {
                token_secret,
                certificate_authority_secret,
            } => (
                json!({"token": config.secrets.require(token_secret)?}),
                certificate_authority_secret
                    .as_ref()
                    .map(|key| config.secrets.require(key).map(str::to_owned))
                    .transpose()
                    .context("Kubernetes CA secret unavailable")?,
            ),
            WorkerAuthConfig::KubernetesPrivateKey {
                client_certificate_secret,
                private_key_secret,
                certificate_authority_secret,
            } => (
                json!({
                    "client-certificate-data": config.secrets.require(client_certificate_secret)?,
                    "client-key-data": config.secrets.require(private_key_secret)?,
                }),
                Some(config.secrets.require(certificate_authority_secret)?.to_owned()),
            ),
            _ => anyhow::bail!("invalid Kubernetes worker authentication"),
        };
        let mut cluster = json!({"server": target.conn_url});
        if let Some(certificate_authority) = certificate_authority {
            cluster["certificate-authority-data"] = json!(certificate_authority);
        }
        let kubeconfig: Kubeconfig = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": "worker", "cluster": cluster}],
            "users": [{"name": "controller", "user": auth}],
            "contexts": [{"name": "worker", "context": {"cluster": "worker", "user": "controller"}}],
            "current-context": "worker"
        }))?;
        let config = kube::Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default()).await?;
        let client = Client::try_from(config)?;
        client
            .apiserver_version()
            .await
            .context("Kubernetes worker is unreachable")?;
        Ok(Self { state, client })
    }

    pub fn for_client(state: Arc<SigbotState>, client: Client) -> Self {
        Self { state, client }
    }

    pub(super) fn runtime(target: &WorkerInfo) -> Result<KubernetesRuntime<'_>> {
        match &target.configuration {
            WorkerConfiguration::Kubernetes {
                namespace,
                runtime_secret,
                runtime_config_map,
                service_account,
                log_pvc_prefix,
                egress_cidrs,
                egress_ports,
                ..
            } => Ok(KubernetesRuntime {
                namespace,
                runtime_secret,
                runtime_config_map,
                service_account,
                log_pvc_prefix,
                egress_cidrs,
                egress_ports,
            }),
            _ => anyhow::bail!("Kubernetes worker requires Kubernetes runtime configuration"),
        }
    }

    fn specification_hash(
        target: &WorkerInfo,
        plan: &WorkerPlan,
        config: &sigbot_core::config::config::ControllerProperties,
    ) -> Result<String> {
        let base = plan.specification_hash(&config.images.sigbot, &config.workers)?;
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(base, target.id, &target.configuration))?)
        ))
    }

    pub fn manifest(
        plan: &WorkerPlan,
        config: &sigbot_core::config::config::ControllerProperties,
        target: &WorkerInfo,
    ) -> Result<Deployment> {
        let settings = &config.workers;
        let runtime = Self::runtime(target)?;
        ensure!(
            !runtime.namespace.is_empty()
                && !runtime.runtime_secret.is_empty()
                && !runtime.runtime_config_map.is_empty(),
            "worker namespace and existing runtime Secret/ConfigMap required"
        );
        let labels = json!({"app.kubernetes.io/managed-by":"sigbot-controller","sigbot.io/worker":plan.name,"sigbot.io/worker-target":target.id.to_string(),"sigbot.io/module":plan.module});
        let env = plan
            .environment
            .iter()
            .map(|(name, value)| json!({"name":name,"value":value}))
            .collect::<Vec<_>>();
        let mut manifest = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": plan.name, "namespace": runtime.namespace, "labels": labels},
            "spec": {
                "replicas": 1,
                "revisionHistoryLimit": 2,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"sigbot.io/worker": plan.name}},
                "template": {
                    "metadata": {
                        "labels": labels,
                        "annotations": {
                            "sigbot.io/specification": Self::specification_hash(target, plan, config)?,
                            "prometheus.io/scrape": "true",
                            "prometheus.io/port": "19090",
                            "prometheus.io/path": "/metrics"
                        }
                    },
                    "spec": {
                        "automountServiceAccountToken": false,
                        "serviceAccountName": runtime.service_account,
                        "terminationGracePeriodSeconds": 30,
                        "securityContext": {
                            "runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000,
                            "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}
                        },
                        "containers": [{
                            "name": "host", "image": config.images.sigbot, "command": plan.command, "env": env,
                            "envFrom": [
                                {"configMapRef": {"name": runtime.runtime_config_map}},
                                {"secretRef": {"name": runtime.runtime_secret}}
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
                                {"name":"runtime-config","mountPath":"/etc/sigbot/runtime","readOnly":true},
                                {"name": "scratch", "mountPath": "/tmp"},
                                {"name": "storage", "mountPath": "/var/lib/sigbot/storage"}
                            ]
                        }],
                        "volumes": [
                            {"name":"runtime-config","configMap":{"name":plan.name}},
                            {"name": "scratch", "emptyDir": {"sizeLimit": "64Mi"}},
                            {"name": "storage", "emptyDir": {"sizeLimit": "128Mi"}}
                        ]
                    }
                }
            }
        });
        if plan.persistent_logs {
            let claim = plan.log_volume_name(runtime.log_pvc_prefix)?;
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
                "labels": {"app.kubernetes.io/managed-by": "sigbot-controller", "sigbot.io/worker": plan.name}
            },
            "spec": {
                "type": "ClusterIP", "selector": {"sigbot.io/worker": plan.name},
                "ports": [{"name": "execution", "protocol": "TCP", "port": service.port, "targetPort": service.port}]
            }
        }))?))
    }

    pub fn network_policy(plan: &WorkerPlan, target: &WorkerInfo) -> Result<NetworkPolicy> {
        let runtime = Self::runtime(target)?;
        let mut egress = vec![
            json!({"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"kube-system"}},"podSelector":{"matchLabels":{"k8s-app":"kube-dns"}}}],"ports":[{"protocol":"UDP","port":53},{"protocol":"TCP","port":53}]}),
        ];
        if !runtime.egress_cidrs.is_empty() {
            ensure!(
                !runtime.egress_ports.is_empty(),
                "explicit worker egress ports required"
            );
            egress.push(json!({"to":runtime.egress_cidrs.iter().map(|cidr|json!({"ipBlock":{"cidr":cidr}})).collect::<Vec<_>>(),"ports":runtime.egress_ports.iter().map(|port|json!({"protocol":"TCP","port":port})).collect::<Vec<_>>() }));
        }
        // Pod selectors without namespace selectors are restricted to this
        // namespace; service routing never opens an arbitrary tenant subnet.
        for target in &plan.service_targets {
            egress.push(
                json!({"to":[{"podSelector":{"matchLabels":{"sigbot.io/worker":target.worker_name}}}],
                "ports":[{"protocol":"TCP","port":target.port}]}),
            );
        }
        for kind in crate::worker::middleware::MiddlewareKind::ALL {
            if crate::worker::middleware::MiddlewareTopology::new(target)
                .deployment(kind)
                .is_some()
            {
                egress.push(json!({
                    "to":[{"podSelector":{"matchLabels":{
                        "sigbot.io/worker-target":target.id.to_string(),
                        "sigbot.io/middleware":kind.as_str()
                    }}}],
                    "ports":[{"protocol":"TCP","port":kind.port()}]
                }));
            }
        }
        let ingress = plan
            .service
            .iter()
            .filter(|service| !service.client_workers.is_empty() || !service.client_modules.is_empty())
            .map(|service| {
                let mut sources = service
                    .client_workers
                    .iter()
                    .map(|worker| json!({"podSelector":{"matchLabels":{"sigbot.io/worker":worker}}}))
                    .collect::<Vec<_>>();
                sources.extend(
                    service
                        .client_modules
                        .iter()
                        .map(|module| json!({"podSelector":{"matchLabels":{"sigbot.io/module":module}}})),
                );
                json!({"from":sources,"ports":[{"protocol":"TCP","port":service.port}]})
            })
            .collect::<Vec<_>>();
        Ok(serde_json::from_value(
            json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":plan.name,"namespace":runtime.namespace,"labels":{"app.kubernetes.io/managed-by":"sigbot-controller","sigbot.io/worker":plan.name}},"spec":{"podSelector":{"matchLabels":{"sigbot.io/worker":plan.name}},"policyTypes":["Ingress","Egress"],"ingress":ingress,"egress":egress}}),
        )?)
    }
    async fn remove_service(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<()> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), Self::runtime(target)?.namespace);
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
                == Some("sigbot-controller")
                && labels.get("sigbot.io/worker") == Some(&plan.name)),
            "refusing to alter a Kubernetes object outside this worker controller"
        );
        Ok(())
    }
}
#[async_trait]
impl WorkerProvider for KubernetesController {
    async fn prepare(&self, target: &WorkerInfo) -> Result<WorkerHealth> {
        crate::kubernetes::middleware::KubernetesMiddleware::new(
            self.client.clone(),
            target,
            &self.state.config.services.platform.controller.images.sigbot,
        )
        .reconcile()
        .await
    }

    async fn apply(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<WorkerHealth> {
        let runtime = Self::runtime(target)?;
        if plan.persistent_logs {
            let claims: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), runtime.namespace);
            let claim = claims
                .get(&plan.log_volume_name(runtime.log_pvc_prefix)?)
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
        let namespace = runtime.namespace;
        let policies: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), namespace);
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let params = PatchParams::apply("sigbot-controller");
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
            .patch(&plan.name, &params, &Patch::Apply(Self::network_policy(plan, target)?))
            .await
            .context("worker NetworkPolicy apply failed")?;
        if let Some(service) = Self::service(plan, namespace)? {
            services
                .patch(&plan.name, &params, &Patch::Apply(service))
                .await
                .context("worker Service apply failed")?;
        } else {
            self.remove_service(target, plan).await?;
        }
        let configs: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        if let Some(existing) = configs.get_opt(&plan.name).await? {
            Self::require_ownership(&existing.metadata, plan)?;
        }
        configs
            .patch(
                &plan.name,
                &params,
                &Patch::Apply(json!({
                    "apiVersion":"v1", "kind":"ConfigMap", "metadata":{
                        "name":plan.name, "namespace":namespace,
                        "labels":{"sigbot.io/worker":plan.name,"app.kubernetes.io/managed-by":"sigbot-controller"}},
                    "data":{"sigbot.yaml":serde_json::to_string_pretty(&plan.configuration)?}
                })),
            )
            .await
            .context("worker YAML ConfigMap apply failed")?;
        let desired = Self::manifest(plan, &self.state.config.services.platform.controller, target)?;
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
    async fn remove(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<bool> {
        let namespace = Self::runtime(target)?.namespace;
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
        self.remove_service(target, plan).await?;
        let configs: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        if let Some(existing) = configs.get_opt(&plan.name).await? {
            Self::require_ownership(&existing.metadata, plan)?;
            configs.delete(&plan.name, &DeleteParams::default()).await?;
        }
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
