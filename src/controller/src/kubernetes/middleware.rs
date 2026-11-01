//! Idempotent tenant middleware provisioning on a registered Kubernetes worker.
use crate::kubernetes::kubernetes::KubernetesController;
use crate::worker::middleware::{MiddlewareKind, MiddlewareTopology};
use crate::worker::reconciler::WorkerHealth;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::Client;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sigbot_types::controller::{MiddlewareDeployment, WorkerInfo};

pub(super) struct KubernetesMiddleware<'a> {
    client: Client,
    target: &'a WorkerInfo,
    sigbot_image: &'a str,
}

impl<'a> KubernetesMiddleware<'a> {
    pub(super) const fn new(client: Client, target: &'a WorkerInfo, sigbot_image: &'a str) -> Self {
        Self {
            client,
            target,
            sigbot_image,
        }
    }

    pub(super) async fn reconcile(&self) -> Result<WorkerHealth> {
        let mut health = WorkerHealth {
            ready: true,
            restarted: false,
        };
        let topology = MiddlewareTopology::new(self.target);
        for kind in MiddlewareKind::ALL {
            let current = if let Some(deployment) = topology.deployment(kind) {
                self.apply(kind, deployment).await?
            } else {
                self.remove(kind).await?
            };
            health.ready &= current.ready;
            health.restarted |= current.restarted;
        }
        let migration = if topology.deployment(MiddlewareKind::Timescaledb).is_some() {
            if health.ready {
                self.migrate().await?
            } else {
                WorkerHealth::default()
            }
        } else {
            self.remove_migration().await?
        };
        health.ready &= migration.ready;
        health.restarted |= migration.restarted;
        Ok(health)
    }

    async fn migrate(&self) -> Result<WorkerHealth> {
        let runtime = KubernetesController::runtime(self.target)?;
        let base = MiddlewareTopology::new(self.target).name(MiddlewareKind::Timescaledb);
        let name = format!("{base}-migration");
        let jobs: Api<Job> = Api::namespaced(self.client.clone(), runtime.namespace);
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                self.sigbot_image,
                self.target.id,
                &self.target.configuration
            ))?)
        );
        if let Some(current) = jobs.get_opt(&name).await? {
            self.require_ownership(&current.metadata, MiddlewareKind::Timescaledb)?;
            let actual = current
                .metadata
                .annotations
                .as_ref()
                .and_then(|values| values.get("sigbot.io/specification"));
            if actual != Some(&hash) {
                jobs.delete(&name, &DeleteParams::foreground()).await?;
                return Ok(WorkerHealth {
                    ready: false,
                    restarted: true,
                });
            }
            let status = current.status.as_ref();
            ensure!(
                !status.is_some_and(|value| value.failed.unwrap_or_default() > 0),
                "tenant database migration failed"
            );
            return Ok(WorkerHealth {
                ready: status.is_some_and(|value| value.succeeded.unwrap_or_default() > 0),
                restarted: false,
            });
        }
        let labels = self.labels(MiddlewareKind::Timescaledb);
        let job: Job = serde_json::from_value(json!({
            "apiVersion":"batch/v1",
            "kind":"Job",
            "metadata":{
                "name":name,
                "namespace":runtime.namespace,
                "labels":labels,
                "annotations":{"sigbot.io/specification":hash}
            },
            "spec":{
                "backoffLimit":2,
                "template":{
                    "metadata":{"labels":labels},
                    "spec":{
                        "automountServiceAccountToken":false,
                        "serviceAccountName":runtime.service_account,
                        "restartPolicy":"Never",
                        "securityContext":{"seccompProfile":{"type":"RuntimeDefault"}},
                        "containers":[{
                            "name":"migration",
                            "image":self.sigbot_image,
                            "imagePullPolicy":"IfNotPresent",
                            "command":["python3","/app/migration/migrate.py","--provider","postgres"],
                            "env":[
                                {"name":"PGHOST","value":base},
                                {"name":"PGPORT","value":"5432"},
                                {"name":"PGUSER","value":"postgres"},
                                {"name":"PGDATABASE","value":"postgres"},
                                {"name":"PGPASSWORD","valueFrom":{"secretKeyRef":{"name":runtime.runtime_secret,"key":"SIGBOT__STORAGE__POSTGRES__PASSWORD"}}}
                            ],
                            "resources":{"requests":{"cpu":"50m","memory":"64Mi"},"limits":{"cpu":"500m","memory":"256Mi"}},
                            "securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}},
                            "volumeMounts":[{"name":"scratch","mountPath":"/tmp"}]
                        }],
                        "volumes":[{"name":"scratch","emptyDir":{"sizeLimit":"64Mi"}}]
                    }
                }
            }
        }))?;
        jobs.patch(&name, &PatchParams::apply("sigbot-controller"), &Patch::Apply(job))
            .await
            .context("tenant database migration Job apply failed")?;
        Ok(WorkerHealth {
            ready: false,
            restarted: false,
        })
    }

    async fn remove_migration(&self) -> Result<WorkerHealth> {
        let runtime = KubernetesController::runtime(self.target)?;
        let name = format!(
            "{}-migration",
            MiddlewareTopology::new(self.target).name(MiddlewareKind::Timescaledb)
        );
        let jobs: Api<Job> = Api::namespaced(self.client.clone(), runtime.namespace);
        if let Some(current) = jobs.get_opt(&name).await? {
            self.require_ownership(&current.metadata, MiddlewareKind::Timescaledb)?;
            jobs.delete(&name, &DeleteParams::foreground()).await?;
        }
        Ok(WorkerHealth {
            ready: true,
            restarted: false,
        })
    }

    async fn apply(&self, kind: MiddlewareKind, deployment: &MiddlewareDeployment) -> Result<WorkerHealth> {
        let runtime = KubernetesController::runtime(self.target)?;
        let topology = MiddlewareTopology::new(self.target);
        let name = topology.name(kind);
        let services: Api<Service> = Api::namespaced(self.client.clone(), runtime.namespace);
        let stateful_sets: Api<StatefulSet> = Api::namespaced(self.client.clone(), runtime.namespace);
        let old = stateful_sets.get_opt(&name).await?;
        if let Some(current) = &old {
            self.require_ownership(&current.metadata, kind)?;
        }
        if let Some(current) = services.get_opt(&name).await? {
            self.require_ownership(&current.metadata, kind)?;
        }
        let params = PatchParams::apply("sigbot-controller");
        services
            .patch(&name, &params, &Patch::Apply(self.service(kind)?))
            .await
            .with_context(|| format!("{} Service apply failed", kind.as_str()))?;
        let result = stateful_sets
            .patch(&name, &params, &Patch::Apply(self.stateful_set(kind, deployment)?))
            .await
            .with_context(|| format!("{} StatefulSet apply failed", kind.as_str()))?;
        let replicas = Self::replicas(kind);
        let ready = result.status.as_ref().is_some_and(|status| {
            status.observed_generation == result.metadata.generation
                && status.ready_replicas == Some(replicas)
                && status.updated_replicas == Some(replicas)
        });
        Ok(WorkerHealth {
            ready,
            restarted: old
                .as_ref()
                .is_some_and(|current| current.metadata.generation != result.metadata.generation),
        })
    }

    async fn remove(&self, kind: MiddlewareKind) -> Result<WorkerHealth> {
        let runtime = KubernetesController::runtime(self.target)?;
        let name = MiddlewareTopology::new(self.target).name(kind);
        let stateful_sets: Api<StatefulSet> = Api::namespaced(self.client.clone(), runtime.namespace);
        if let Some(current) = stateful_sets.get_opt(&name).await? {
            self.require_ownership(&current.metadata, kind)?;
            stateful_sets.delete(&name, &DeleteParams::foreground()).await?;
            return Ok(WorkerHealth {
                ready: false,
                restarted: true,
            });
        }
        let services: Api<Service> = Api::namespaced(self.client.clone(), runtime.namespace);
        if let Some(current) = services.get_opt(&name).await? {
            self.require_ownership(&current.metadata, kind)?;
            services.delete(&name, &DeleteParams::default()).await?;
        }
        // StatefulSet claims are deliberately retained for recoverable re-registration.
        Ok(WorkerHealth {
            ready: true,
            restarted: false,
        })
    }

    fn service(&self, kind: MiddlewareKind) -> Result<Service> {
        let runtime = KubernetesController::runtime(self.target)?;
        let name = MiddlewareTopology::new(self.target).name(kind);
        let mut ports = vec![json!({"name":kind.as_str(),"port":kind.port(),"targetPort":kind.as_str()})];
        if kind == MiddlewareKind::Redis {
            ports.push(json!({"name":"cluster-bus","port":16379,"targetPort":"cluster-bus"}));
        } else if kind == MiddlewareKind::Emqx {
            ports.push(json!({"name":"dashboard","port":18083,"targetPort":"dashboard"}));
        }
        let mut spec = json!({
            "selector": self.labels(kind),
            "ports": ports
        });
        if kind == MiddlewareKind::Redis {
            spec["clusterIP"] = json!("None");
            spec["publishNotReadyAddresses"] = json!(true);
        }
        Ok(serde_json::from_value(json!({
            "apiVersion":"v1",
            "kind":"Service",
            "metadata":{"name":name,"namespace":runtime.namespace,"labels":self.labels(kind)},
            "spec":spec
        }))?)
    }

    fn stateful_set(&self, kind: MiddlewareKind, deployment: &MiddlewareDeployment) -> Result<StatefulSet> {
        let runtime = KubernetesController::runtime(self.target)?;
        let topology = MiddlewareTopology::new(self.target);
        let name = topology.name(kind);
        let labels = self.labels(kind);
        let mount_path = match kind {
            MiddlewareKind::Redis => "/bitnami/redis/data",
            MiddlewareKind::Pgvector => "/var/lib/postgresql/data",
            MiddlewareKind::Timescaledb => "/home/postgres/pgdata/data",
            MiddlewareKind::Emqx => "/opt/emqx/data",
        };
        let mut container = json!({
            "name":kind.as_str(),
            "image":deployment.image,
            "imagePullPolicy":"IfNotPresent",
            "ports":[{"name":kind.as_str(),"containerPort":kind.port()}],
            "resources":{
                "requests":{"cpu":format!("{}m", deployment.resources.cpu_millis.min(250)),"memory":format!("{}Mi", deployment.resources.memory_mb.min(512))},
                "limits":{"cpu":format!("{}m", deployment.resources.cpu_millis),"memory":format!("{}Mi", deployment.resources.memory_mb)}
            },
            "securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}},
            "volumeMounts":[{"name":"data","mountPath":mount_path}],
            "readinessProbe":{"tcpSocket":{"port":kind.as_str()},"initialDelaySeconds":10,"periodSeconds":5,"timeoutSeconds":3},
            "livenessProbe":{"tcpSocket":{"port":kind.as_str()},"initialDelaySeconds":30,"periodSeconds":10,"timeoutSeconds":5}
        });
        self.configure_container(kind, &name, runtime.namespace, runtime.runtime_secret, &mut container)?;
        let mut pod_spec = json!({
            "automountServiceAccountToken":false,
            "serviceAccountName":runtime.service_account,
            "terminationGracePeriodSeconds":30,
            "securityContext":{"seccompProfile":{"type":"RuntimeDefault"}},
            "containers":[container]
        });
        if matches!(kind, MiddlewareKind::Pgvector | MiddlewareKind::Timescaledb) {
            pod_spec["volumes"] =
                json!([{"name":"database-init","configMap":{"name":runtime.runtime_config_map,"optional":true}}]);
            pod_spec["containers"][0]["volumeMounts"]
                .as_array_mut()
                .context("middleware volume mounts missing")?
                .push(json!({"name":"database-init","mountPath":"/docker-entrypoint-initdb.d","readOnly":true}));
        }
        Ok(serde_json::from_value(json!({
            "apiVersion":"apps/v1",
            "kind":"StatefulSet",
            "metadata":{"name":name,"namespace":runtime.namespace,"labels":labels},
            "spec":{
                "serviceName":name,
                "replicas":Self::replicas(kind),
                "podManagementPolicy":if kind == MiddlewareKind::Redis { "Parallel" } else { "OrderedReady" },
                "selector":{"matchLabels":labels},
                "template":{"metadata":{"labels":labels},"spec":pod_spec},
                "volumeClaimTemplates":[{
                    "metadata":{"name":"data","labels":labels},
                    "spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":format!("{}Gi",deployment.resources.storage_gb)}}}
                }]
            }
        }))?)
    }

    fn configure_container(
        &self,
        kind: MiddlewareKind,
        name: &str,
        namespace: &str,
        secret: &str,
        container: &mut Value,
    ) -> Result<()> {
        let password_key = match kind {
            MiddlewareKind::Redis => Some("SIGBOT__CACHE__REDIS__PASSWORD"),
            MiddlewareKind::Pgvector => Some("SIGBOT__LLM__VECDB__PG_VECTOR__PASSWORD"),
            MiddlewareKind::Timescaledb => Some("SIGBOT__STORAGE__POSTGRES__PASSWORD"),
            MiddlewareKind::Emqx => None,
        };
        let mut environment = Vec::new();
        if let Some(key) = password_key {
            let variable = if kind == MiddlewareKind::Redis {
                "REDIS_PASSWORD"
            } else {
                "POSTGRES_PASSWORD"
            };
            environment.push(json!({"name":variable,"valueFrom":{"secretKeyRef":{"name":secret,"key":key}}}));
        }
        match kind {
            MiddlewareKind::Redis => {
                let nodes = (0..Self::replicas(kind))
                    .map(|index| format!("{name}-{index}.{name}.{namespace}.svc.cluster.local"))
                    .collect::<Vec<_>>()
                    .join(" ");
                environment.extend([
                    json!({"name":"POD_NAME","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}}),
                    json!({"name":"REDISCLI_AUTH","valueFrom":{"secretKeyRef":{"name":secret,"key":"SIGBOT__CACHE__REDIS__PASSWORD"}}}),
                    json!({"name":"REDIS_NODES","value":nodes}),
                ]);
                container["command"] = json!(["/bin/bash", "-ec"]);
                container["args"] = json!(["ordinal=\"${POD_NAME##*-}\"; if [[ \"$ordinal\" == \"0\" ]]; then export REDIS_CLUSTER_CREATOR=yes REDIS_CLUSTER_REPLICAS=1; fi; exec /opt/bitnami/scripts/redis-cluster/entrypoint.sh /opt/bitnami/scripts/redis-cluster/run.sh"]);
                container["ports"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"name":"cluster-bus","containerPort":16379}));
            }
            MiddlewareKind::Pgvector => environment.extend([
                json!({"name":"POSTGRES_USER","value":"postgres"}),
                json!({"name":"POSTGRES_DB","value":"postgres"}),
                json!({"name":"PGDATA","value":"/var/lib/postgresql/data/pgdata"}),
            ]),
            MiddlewareKind::Timescaledb => environment.extend([
                json!({"name":"POSTGRES_USER","value":"postgres"}),
                json!({"name":"POSTGRES_DB","value":"postgres"}),
            ]),
            MiddlewareKind::Emqx => {
                environment.extend([
                    json!({"name":"POD_NAME","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}}),
                    json!({"name":"EMQX_NODE__NAME","value":format!("emqx@$(POD_NAME).{name}.{namespace}.svc.cluster.local")}),
                    json!({"name":"EMQX_DURABLE_SESSIONS__ENABLE","value":"true"}),
                    json!({"name":"EMQX_DURABLE_SESSIONS__MESSAGE_RETENTION_PERIOD","value":"7d"}),
                    json!({"name":"EMQX_MQTT__SESSION_EXPIRY_INTERVAL","value":"7d"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__MECHANISM","value":"jwt"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__USE_JWKS","value":"false"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__ALGORITHM","value":"hmac-based"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__FROM","value":"password"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__VERIFY_CLAIMS","value":"{\"username\":\"${username}\"}"}),
                    json!({"name":"EMQX_AUTHENTICATION__1__SECRET","valueFrom":{"secretKeyRef":{"name":secret,"key":"SIGBOT__SECRETS__VALUES__MQTT_SIGNING_KEY"}}}),
                    json!({"name":"EMQX_AUTHORIZATION__NO_MATCH","value":"deny"}),
                ]);
                container["envFrom"] = json!([{"secretRef":{"name":secret}}]);
                container["ports"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"name":"dashboard","containerPort":18083}));
            }
        }
        container["env"] = Value::Array(environment);
        Ok(())
    }

    fn labels(&self, kind: MiddlewareKind) -> Value {
        json!({
            "app.kubernetes.io/managed-by":"sigbot-controller",
            "sigbot.io/worker-target":self.target.id.to_string(),
            "sigbot.io/middleware":kind.as_str()
        })
    }

    fn require_ownership(&self, metadata: &ObjectMeta, kind: MiddlewareKind) -> Result<()> {
        let labels = metadata.labels.as_ref();
        ensure!(
            labels
                .and_then(|value| value.get("app.kubernetes.io/managed-by"))
                .map(String::as_str)
                == Some("sigbot-controller")
                && labels.and_then(|value| value.get("sigbot.io/worker-target")) == Some(&self.target.id.to_string())
                && labels
                    .and_then(|value| value.get("sigbot.io/middleware"))
                    .map(String::as_str)
                    == Some(kind.as_str()),
            "refusing to alter middleware outside this worker target"
        );
        Ok(())
    }

    const fn replicas(kind: MiddlewareKind) -> i32 {
        if matches!(kind, MiddlewareKind::Redis) {
            6
        } else {
            1
        }
    }
}
