//! Idempotent tenant middleware provisioning on a registered Docker worker.
use crate::docker::docker::DockerController;
use crate::worker::middleware::{MiddlewareKind, MiddlewareTopology};
use crate::worker::reconciler::WorkerHealth;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, RemoveContainerOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::models::{HealthConfig, HealthStatusEnum, HostConfig, RestartPolicy, RestartPolicyNameEnum};
use bollard::volume::CreateVolumeOptions;
use bollard::Docker;
use sha2::{Digest, Sha256};
use sigbot_types::controller::{MiddlewareDeployment, WorkerInfo};
use std::collections::HashMap;

pub(super) struct DockerMiddleware<'a> {
    client: Docker,
    target: &'a WorkerInfo,
    sigbot_image: &'a str,
}

impl<'a> DockerMiddleware<'a> {
    pub(super) const fn new(client: Docker, target: &'a WorkerInfo, sigbot_image: &'a str) -> Self {
        Self {
            client,
            target,
            sigbot_image,
        }
    }

    pub(super) async fn reconcile(&self) -> Result<WorkerHealth> {
        self.require_network().await?;
        let topology = MiddlewareTopology::new(self.target);
        let mut combined = WorkerHealth {
            ready: true,
            restarted: false,
        };
        for kind in MiddlewareKind::ALL {
            let current = if let Some(deployment) = topology.deployment(kind) {
                self.apply(kind, deployment).await?
            } else {
                self.remove(kind).await?
            };
            combined.ready &= current.ready;
            combined.restarted |= current.restarted;
        }
        let migration = if topology.deployment(MiddlewareKind::Timescaledb).is_some() {
            if combined.ready {
                self.migrate().await?
            } else {
                WorkerHealth::default()
            }
        } else {
            self.remove_migration().await?
        };
        combined.ready &= migration.ready;
        combined.restarted |= migration.restarted;
        Ok(combined)
    }

    async fn migrate(&self) -> Result<WorkerHealth> {
        let base = MiddlewareTopology::new(self.target).name(MiddlewareKind::Timescaledb);
        let name = format!("{base}-migration");
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                self.sigbot_image,
                self.target.id,
                &self.target.configuration
            ))?)
        );
        match self
            .client
            .inspect_container(&name, Some(InspectContainerOptions { size: false }))
            .await
        {
            Ok(container) => {
                self.require_container(&container, MiddlewareKind::Timescaledb)?;
                let actual = container
                    .config
                    .as_ref()
                    .and_then(|value| value.labels.as_ref())
                    .and_then(|labels| labels.get("sigbot.io/specification"));
                if actual != Some(&hash) {
                    self.remove_container(&name, MiddlewareKind::Timescaledb).await?;
                    return Ok(WorkerHealth {
                        ready: false,
                        restarted: true,
                    });
                }
                let state = container.state.context("migration container state unavailable")?;
                if state.running == Some(true) {
                    return Ok(WorkerHealth {
                        ready: false,
                        restarted: false,
                    });
                }
                ensure!(state.exit_code == Some(0), "tenant database migration failed");
                return Ok(WorkerHealth {
                    ready: true,
                    restarted: false,
                });
            }
            Err(error) if Self::missing(&error) => {}
            Err(error) => return Err(error.into()),
        }
        let (network, _, _) = DockerController::settings(self.target)?;
        let mut environment = DockerController::runtime_environment(self.target)?;
        let password = Self::required(&environment, "SIGBOT__STORAGE__POSTGRES__PASSWORD")?;
        environment.extend([
            ("PGHOST".into(), base),
            ("PGPORT".into(), "5432".into()),
            ("PGUSER".into(), "postgres".into()),
            ("PGDATABASE".into(), "postgres".into()),
            ("PGPASSWORD".into(), password),
        ]);
        let mut labels = self.labels(MiddlewareKind::Timescaledb);
        labels.insert("sigbot.io/specification".into(), hash);
        labels.insert("sigbot.io/role".into(), "migration".into());
        let config = Config {
            image: Some(self.sigbot_image.to_owned()),
            entrypoint: Some(vec!["/usr/bin/tini".into(), "-s".into(), "--".into()]),
            cmd: Some(vec![
                "python3".into(),
                "/app/migration/migrate.py".into(),
                "--provider".into(),
                "postgres".into(),
            ]),
            user: Some("1000:1000".into()),
            env: Some(
                environment
                    .into_iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect(),
            ),
            labels: Some(labels),
            host_config: Some(HostConfig {
                network_mode: Some(network.to_owned()),
                readonly_rootfs: Some(true),
                cap_drop: Some(vec!["ALL".into()]),
                security_opt: Some(vec!["no-new-privileges:true".into()]),
                tmpfs: Some(HashMap::from([(
                    "/tmp".into(),
                    "rw,nosuid,nodev,size=64m,uid=1000,gid=1000".into(),
                )])),
                restart_policy: Some(RestartPolicy {
                    name: Some(RestartPolicyNameEnum::NO),
                    maximum_retry_count: Some(0),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.client
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .context("tenant database migration container creation failed")?;
        self.client
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await
            .context("tenant database migration container startup failed")?;
        Ok(WorkerHealth {
            ready: false,
            restarted: false,
        })
    }

    async fn remove_migration(&self) -> Result<WorkerHealth> {
        let name = format!(
            "{}-migration",
            MiddlewareTopology::new(self.target).name(MiddlewareKind::Timescaledb)
        );
        match self
            .client
            .inspect_container(&name, None::<InspectContainerOptions>)
            .await
        {
            Ok(container) => {
                self.require_container(&container, MiddlewareKind::Timescaledb)?;
                self.remove_container(&name, MiddlewareKind::Timescaledb).await?;
            }
            Err(error) if Self::missing(&error) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(WorkerHealth {
            ready: true,
            restarted: false,
        })
    }

    async fn require_network(&self) -> Result<()> {
        let (network, _, _) = DockerController::settings(self.target)?;
        ensure!(
            !network.is_empty() && !matches!(network, "host" | "bridge" | "none"),
            "dedicated operator-provisioned Docker network required"
        );
        let current = self
            .client
            .inspect_network(network, None::<bollard::network::InspectNetworkOptions<String>>)
            .await
            .context("dedicated worker network unavailable")?;
        ensure!(
            current.labels.as_ref().and_then(|labels| labels.get("sigbot.io/scope"))
                == Some(&format!("{}:{}", self.target.env, self.target.tenant_id)),
            "Docker network does not match worker scope"
        );
        Ok(())
    }

    async fn apply(&self, kind: MiddlewareKind, deployment: &MiddlewareDeployment) -> Result<WorkerHealth> {
        let mut combined = WorkerHealth {
            ready: true,
            restarted: false,
        };
        for index in 0..Self::replicas(kind) {
            let current = self.apply_container(kind, deployment, index).await?;
            combined.ready &= current.ready;
            combined.restarted |= current.restarted;
        }
        Ok(combined)
    }

    async fn apply_container(
        &self,
        kind: MiddlewareKind,
        deployment: &MiddlewareDeployment,
        index: usize,
    ) -> Result<WorkerHealth> {
        let name = self.container_name(kind, index);
        let volume = format!("{name}-data");
        self.ensure_volume(kind, &volume).await?;
        let config = self.container_config(kind, deployment, index, &volume)?;
        let desired_hash = config
            .labels
            .as_ref()
            .and_then(|labels| labels.get("sigbot.io/specification"));
        let mut restarted = false;
        match self
            .client
            .inspect_container(&name, Some(InspectContainerOptions { size: false }))
            .await
        {
            Ok(container) => {
                self.require_container(&container, kind)?;
                let actual_hash = container
                    .config
                    .as_ref()
                    .and_then(|value| value.labels.as_ref())
                    .and_then(|labels| labels.get("sigbot.io/specification"));
                if actual_hash == desired_hash {
                    let state = container.state.context("middleware container state unavailable")?;
                    if state.running != Some(true) {
                        self.client
                            .start_container(&name, None::<StartContainerOptions<String>>)
                            .await?;
                        return Ok(WorkerHealth {
                            ready: false,
                            restarted: true,
                        });
                    }
                    let health = state.health.and_then(|value| value.status);
                    if health == Some(HealthStatusEnum::UNHEALTHY) {
                        self.client.restart_container(&name, None).await?;
                        return Ok(WorkerHealth {
                            ready: false,
                            restarted: true,
                        });
                    }
                    return Ok(WorkerHealth {
                        ready: health == Some(HealthStatusEnum::HEALTHY),
                        restarted: false,
                    });
                }
                self.remove_container(&name, kind).await?;
                restarted = true;
            }
            Err(error) if Self::missing(&error) => {}
            Err(error) => return Err(error.into()),
        }
        self.client
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .with_context(|| format!("{} container creation failed", kind.as_str()))?;
        self.client
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await
            .with_context(|| format!("{} container startup failed", kind.as_str()))?;
        Ok(WorkerHealth {
            ready: false,
            restarted,
        })
    }

    async fn remove(&self, kind: MiddlewareKind) -> Result<WorkerHealth> {
        let mut removed = false;
        for index in 0..Self::replicas(kind) {
            let name = self.container_name(kind, index);
            match self
                .client
                .inspect_container(&name, None::<InspectContainerOptions>)
                .await
            {
                Ok(container) => {
                    self.require_container(&container, kind)?;
                    self.remove_container(&name, kind).await?;
                    removed = true;
                }
                Err(error) if Self::missing(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // Named volumes remain available for a recoverable re-registration.
        Ok(WorkerHealth {
            ready: true,
            restarted: removed,
        })
    }

    async fn remove_container(&self, name: &str, kind: MiddlewareKind) -> Result<()> {
        match self
            .client
            .stop_container(name, Some(StopContainerOptions { t: 20 }))
            .await
        {
            Ok(_) => {}
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 304 | 404, ..
            }) => {}
            Err(error) => return Err(error.into()),
        }
        self.client
            .remove_container(
                name,
                Some(RemoveContainerOptions {
                    force: false,
                    v: false,
                    link: false,
                }),
            )
            .await
            .with_context(|| format!("{} container removal failed", kind.as_str()))?;
        Ok(())
    }

    async fn ensure_volume(&self, kind: MiddlewareKind, name: &str) -> Result<()> {
        match self.client.inspect_volume(name).await {
            Ok(volume) => {
                ensure!(
                    volume.labels.get("sigbot.io/worker-target") == Some(&self.target.id.to_string())
                        && volume.labels.get("sigbot.io/middleware").map(String::as_str) == Some(kind.as_str()),
                    "middleware volume belongs to another worker target"
                );
            }
            Err(error) if Self::missing(&error) => {
                self.client
                    .create_volume(CreateVolumeOptions {
                        name: name.to_owned(),
                        driver: "local".to_owned(),
                        driver_opts: HashMap::new(),
                        labels: self.labels(kind),
                    })
                    .await?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn container_config(
        &self,
        kind: MiddlewareKind,
        deployment: &MiddlewareDeployment,
        index: usize,
        volume: &str,
    ) -> Result<Config<String>> {
        let (network, _, _) = DockerController::settings(self.target)?;
        let name = self.container_name(kind, index);
        let mut environment = DockerController::runtime_environment(self.target)?;
        self.configure_environment(kind, index, &name, &mut environment)?;
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                self.target.id,
                kind.as_str(),
                index,
                deployment,
                &self.target.configuration
            ))?)
        );
        let mut labels = self.labels(kind);
        labels.insert("sigbot.io/specification".into(), hash);
        let command = self.command(kind);
        Ok(Config {
            image: Some(deployment.image.clone()),
            hostname: Some(name),
            entrypoint: command.as_ref().map(|_| vec!["/bin/bash".into(), "-ec".into()]),
            cmd: command.map(|value| vec![value]),
            env: Some(
                environment
                    .into_iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect(),
            ),
            labels: Some(labels),
            healthcheck: Some(HealthConfig {
                test: Some(vec!["CMD-SHELL".into(), Self::healthcheck(kind).into()]),
                interval: Some(10_000_000_000),
                timeout: Some(5_000_000_000),
                retries: Some(6),
                start_period: Some(30_000_000_000),
                ..Default::default()
            }),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{volume}:{}:rw", Self::mount_path(kind))]),
                network_mode: Some(network.to_owned()),
                memory: Some(i64::from(deployment.resources.memory_mb) * 1024 * 1024),
                memory_swap: Some(i64::from(deployment.resources.memory_mb) * 1024 * 1024),
                nano_cpus: Some(i64::from(deployment.resources.cpu_millis) * 1_000_000),
                readonly_rootfs: Some(false),
                cap_drop: Some(vec!["ALL".into()]),
                security_opt: Some(vec!["no-new-privileges:true".into()]),
                restart_policy: Some(RestartPolicy {
                    name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
                    maximum_retry_count: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn configure_environment(
        &self,
        kind: MiddlewareKind,
        index: usize,
        name: &str,
        environment: &mut HashMap<String, String>,
    ) -> Result<()> {
        match kind {
            MiddlewareKind::Redis => {
                let password = Self::required(environment, "SIGBOT__CACHE__REDIS__PASSWORD")?;
                let base = MiddlewareTopology::new(self.target).name(kind);
                let nodes = (0..Self::replicas(kind))
                    .map(|item| format!("{base}-{item}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                environment.extend([
                    ("REDIS_PASSWORD".into(), password.clone()),
                    ("REDISCLI_AUTH".into(), password),
                    ("REDIS_NODES".into(), nodes),
                ]);
                if index == 0 {
                    environment.insert("REDIS_CLUSTER_CREATOR".into(), "yes".into());
                    environment.insert("REDIS_CLUSTER_REPLICAS".into(), "1".into());
                }
            }
            MiddlewareKind::Pgvector => {
                let password = Self::required(environment, "SIGBOT__LLM__VECDB__PG_VECTOR__PASSWORD")?;
                environment.extend([
                    ("POSTGRES_PASSWORD".into(), password),
                    ("POSTGRES_USER".into(), "postgres".into()),
                    ("POSTGRES_DB".into(), "postgres".into()),
                    ("PGDATA".into(), "/var/lib/postgresql/data/pgdata".into()),
                ]);
            }
            MiddlewareKind::Timescaledb => {
                let password = Self::required(environment, "SIGBOT__STORAGE__POSTGRES__PASSWORD")?;
                environment.extend([
                    ("POSTGRES_PASSWORD".into(), password),
                    ("POSTGRES_USER".into(), "postgres".into()),
                    ("POSTGRES_DB".into(), "postgres".into()),
                ]);
            }
            MiddlewareKind::Emqx => {
                let signing_key = Self::required(environment, "SIGBOT__SECRETS__VALUES__MQTT_SIGNING_KEY")?;
                environment.extend([
                    ("EMQX_NODE__NAME".into(), format!("emqx@{name}")),
                    ("EMQX_DURABLE_SESSIONS__ENABLE".into(), "true".into()),
                    ("EMQX_DURABLE_SESSIONS__MESSAGE_RETENTION_PERIOD".into(), "7d".into()),
                    ("EMQX_MQTT__SESSION_EXPIRY_INTERVAL".into(), "7d".into()),
                    ("EMQX_AUTHENTICATION__1__MECHANISM".into(), "jwt".into()),
                    ("EMQX_AUTHENTICATION__1__USE_JWKS".into(), "false".into()),
                    ("EMQX_AUTHENTICATION__1__ALGORITHM".into(), "hmac-based".into()),
                    ("EMQX_AUTHENTICATION__1__FROM".into(), "password".into()),
                    (
                        "EMQX_AUTHENTICATION__1__VERIFY_CLAIMS".into(),
                        "{\"username\":\"${username}\"}".into(),
                    ),
                    ("EMQX_AUTHENTICATION__1__SECRET".into(), signing_key),
                    ("EMQX_AUTHORIZATION__NO_MATCH".into(), "deny".into()),
                ]);
            }
        }
        Ok(())
    }

    fn command(&self, kind: MiddlewareKind) -> Option<String> {
        (kind == MiddlewareKind::Redis).then(|| {
            "exec /opt/bitnami/scripts/redis-cluster/entrypoint.sh /opt/bitnami/scripts/redis-cluster/run.sh".into()
        })
    }

    fn labels(&self, kind: MiddlewareKind) -> HashMap<String, String> {
        HashMap::from([
            ("sigbot.io/managed-by".into(), "sigbot-controller".into()),
            ("sigbot.io/worker-target".into(), self.target.id.to_string()),
            ("sigbot.io/middleware".into(), kind.as_str().into()),
        ])
    }

    fn require_container(
        &self,
        container: &bollard::models::ContainerInspectResponse,
        kind: MiddlewareKind,
    ) -> Result<()> {
        let labels = container.config.as_ref().and_then(|value| value.labels.as_ref());
        ensure!(
            labels
                .and_then(|value| value.get("sigbot.io/managed-by"))
                .map(String::as_str)
                == Some("sigbot-controller")
                && labels.and_then(|value| value.get("sigbot.io/worker-target")) == Some(&self.target.id.to_string())
                && labels
                    .and_then(|value| value.get("sigbot.io/middleware"))
                    .map(String::as_str)
                    == Some(kind.as_str()),
            "middleware container belongs to another worker target"
        );
        Ok(())
    }

    fn container_name(&self, kind: MiddlewareKind, index: usize) -> String {
        let base = MiddlewareTopology::new(self.target).name(kind);
        if kind == MiddlewareKind::Redis {
            format!("{base}-{index}")
        } else {
            base
        }
    }

    fn required(environment: &HashMap<String, String>, key: &str) -> Result<String> {
        environment
            .get(key)
            .filter(|value| !value.is_empty())
            .cloned()
            .with_context(|| format!("runtime environment requires {key}"))
    }

    const fn replicas(kind: MiddlewareKind) -> usize {
        if matches!(kind, MiddlewareKind::Redis) {
            6
        } else {
            1
        }
    }

    const fn mount_path(kind: MiddlewareKind) -> &'static str {
        match kind {
            MiddlewareKind::Redis => "/bitnami/redis/data",
            MiddlewareKind::Pgvector => "/var/lib/postgresql/data",
            MiddlewareKind::Timescaledb => "/home/postgres/pgdata/data",
            MiddlewareKind::Emqx => "/opt/emqx/data",
        }
    }

    const fn healthcheck(kind: MiddlewareKind) -> &'static str {
        match kind {
            MiddlewareKind::Redis => "redis-cli -h 127.0.0.1 ping | grep -q PONG",
            MiddlewareKind::Pgvector | MiddlewareKind::Timescaledb => "pg_isready -U postgres -d postgres",
            MiddlewareKind::Emqx => "emqx ctl status",
        }
    }

    fn missing(error: &bollard::errors::Error) -> bool {
        matches!(
            error,
            bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }
        )
    }
}
