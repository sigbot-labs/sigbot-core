//! Docker uses deterministic worker names and replaces changed specifications atomically
//! with respect to the controller's PostgreSQL workflow lock.
use crate::{
    deployer_factory::ISigbotDeployer,
    worker_plan::WorkerPlan,
    worker_reconciler::{WorkerHealth, WorkerProvider, WorkerReconciler, WorkerScheduler},
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use bollard::{
    container::{
        Config, CreateContainerOptions, InspectContainerOptions, RemoveContainerOptions, StartContainerOptions,
        StopContainerOptions,
    },
    models::{
        ContainerInspectResponse, HealthConfig, HealthStatusEnum, HostConfig, RestartPolicy, RestartPolicyNameEnum,
    },
    Docker,
};
use sigbot_core::context::state::SigbotState;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct SigbotDockerDeployer {
    state: Arc<SigbotState>,
    client: Docker,
    scheduler: WorkerScheduler,
}
impl SigbotDockerDeployer {
    pub const NAME: &'static str = "DOCKER";
    pub async fn new(cron: Option<String>, channels: Option<usize>, state: Option<Arc<SigbotState>>) -> Arc<Self> {
        Arc::new(Self {
            state: state.expect("controller state required"),
            client: Docker::connect_with_defaults().expect("Docker connection configuration invalid"),
            scheduler: WorkerScheduler::new(cron, channels),
        })
    }
    fn environment(&self, plan: &WorkerPlan) -> Result<Vec<String>> {
        let path = &self.state.config.services.deployer.workers.docker_env_file;
        ensure!(!path.is_empty(), "worker runtime environment file required");
        let metadata = std::fs::metadata(path).context("runtime environment file unavailable")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "runtime environment file must be owner-only"
            );
        }
        ensure!(metadata.len() <= 1024 * 1024, "runtime environment file exceeds limit");
        let contents = std::fs::read_to_string(path).context("runtime environment file cannot be read")?;
        let mut values = HashMap::new();
        for line in contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let (key, value) = line
                .split_once('=')
                .context("runtime environment must use KEY=value records")?;
            ensure!(
                !key.is_empty()
                    && key
                        .bytes()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_'),
                "invalid runtime environment name"
            );
            values.insert(key.to_owned(), value.to_owned());
        }
        values.extend(plan.environment.clone());
        Ok(values
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect())
    }
    async fn config(&self, plan: &WorkerPlan) -> Result<Config<String>> {
        let config = &self.state.config.services.deployer;
        let settings = &config.workers;
        ensure!(
            !settings.docker_network.is_empty()
                && !matches!(settings.docker_network.as_str(), "host" | "bridge" | "none"),
            "dedicated operator-provisioned Docker network required"
        );
        // Network provisioning/firewall policy is an operator responsibility; no host networking.
        let network = self
            .client
            .inspect_network(
                &settings.docker_network,
                None::<bollard::network::InspectNetworkOptions<String>>,
            )
            .await
            .context("dedicated worker network unavailable")?;
        ensure!(
            network.labels.as_ref().and_then(|labels| labels.get("sigbot.io/scope"))
                == Some(&format!(
                    "{}:{}:{}",
                    self.state.config.authguard.service_env.as_deref().unwrap_or_default(),
                    self.state.config.authguard.service_tenant_id.unwrap_or_default(),
                    self.state.config.authguard.service_space_id.unwrap_or_default()
                )),
            "Docker network does not match controller scope"
        );
        let hash = plan.specification_hash(&config.images.sigbot, settings)?;
        let binds = if plan.persistent_logs {
            let name = plan.log_volume_name(&settings.docker_log_volume_prefix)?;
            let volume = self
                .client
                .inspect_volume(&name)
                .await
                .context("pre-provisioned durable worker log volume missing")?;
            ensure!(
                volume.labels.get("sigbot.io/worker") == Some(&plan.name),
                "log volume belongs to a different worker"
            );
            Some(vec![format!("{name}:/var/lib/sigbot/logs:rw")])
        } else {
            None
        };
        Ok(Config {
            image: Some(config.images.sigbot.clone()),
            entrypoint: Some(vec!["/usr/bin/tini".into(), "-s".into(), "--".into()]),
            cmd: Some(plan.command.clone()),
            user: Some("1000:1000".into()),
            env: Some(self.environment(plan)?),
            labels: Some(HashMap::from([
                ("sigbot.io/specification".into(), hash),
                ("sigbot.io/managed-by".into(), "sigbot-deployer".into()),
                ("sigbot.io/worker".into(), plan.name.clone()),
            ])),
            healthcheck: Some(HealthConfig {
                test: Some(vec![
                    "CMD".into(), "python3".into(), "-c".into(),
                    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:19090/_/healthz',timeout=2).read()".into(),
                ]),
                interval: Some(5_000_000_000),
                timeout: Some(3_000_000_000),
                retries: Some(3),
                start_period: Some(30_000_000_000),
                ..Default::default()
            }),
            host_config: Some(HostConfig {
                binds,
                network_mode: Some(settings.docker_network.clone()),
                memory: Some(i64::from(settings.memory_mb) * 1024 * 1024),
                memory_swap: Some(i64::from(settings.memory_mb) * 1024 * 1024),
                nano_cpus: Some(i64::from(settings.cpu_millis) * 1_000_000),
                pids_limit: Some(settings.pids_limit),
                readonly_rootfs: Some(true),
                cap_drop: Some(vec!["ALL".into()]),
                security_opt: Some(vec!["no-new-privileges:true".into()]),
                tmpfs: Some(HashMap::from([
                    ("/tmp".into(), "rw,nosuid,nodev,size=64m,uid=1000,gid=1000".into()),
                    ("/var/lib/sigbot/storage".into(), "rw,nosuid,nodev,size=128m,uid=1000,gid=1000".into()),
                ])),
                restart_policy: Some(RestartPolicy {
                    name: Some(RestartPolicyNameEnum::NO),
                    maximum_retry_count: Some(0),
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn missing(error: &bollard::errors::Error) -> bool {
        matches!(
            error,
            bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }
        )
    }
    fn require_ownership(&self, container: &ContainerInspectResponse, plan: &WorkerPlan) -> Result<()> {
        let labels = container.config.as_ref().and_then(|config| config.labels.as_ref());
        let managed = labels
            .and_then(|labels| labels.get("sigbot.io/managed-by"))
            .is_some_and(|value| value == "sigbot-deployer");
        let worker = labels.and_then(|labels| labels.get("sigbot.io/worker"));
        // Pre-label worker releases remain updatable only on this controller's
        // dedicated network. A label from another controller is never enough.
        let network = container
            .host_config
            .as_ref()
            .and_then(|host| host.network_mode.as_deref());
        ensure!(
            managed
                && worker.is_none_or(|name| name == &plan.name)
                && network == Some(self.state.config.services.deployer.workers.docker_network.as_str()),
            "worker container does not belong to this scoped controller"
        );
        Ok(())
    }
}
#[async_trait]
impl WorkerProvider for SigbotDockerDeployer {
    async fn apply(&self, plan: &WorkerPlan) -> Result<WorkerHealth> {
        let config = self.config(plan).await?;
        let hash = config
            .labels
            .as_ref()
            .and_then(|labels| labels.get("sigbot.io/specification"));
        let mut restarted = false;
        match self
            .client
            .inspect_container(&plan.name, Some(InspectContainerOptions { size: false }))
            .await
        {
            Ok(container) => {
                self.require_ownership(&container, plan)?;
                let same = container
                    .config
                    .as_ref()
                    .and_then(|config| config.labels.as_ref())
                    .and_then(|labels| labels.get("sigbot.io/specification"))
                    == hash;
                if same {
                    let state = container.state.context("container state unavailable")?;
                    if state.running != Some(true) {
                        self.client
                            .start_container(&plan.name, None::<StartContainerOptions<String>>)
                            .await?;
                        return Ok(WorkerHealth {
                            ready: false,
                            restarted: true,
                        });
                    }
                    let health = state.health.and_then(|health| health.status);
                    if health == Some(HealthStatusEnum::UNHEALTHY) {
                        self.client.restart_container(&plan.name, None).await?;
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
                self.remove(plan).await?;
                restarted = true;
            }
            Err(error) if Self::missing(&error) => {}
            Err(error) => return Err(error.into()),
        }
        self.client
            .create_container(
                Some(CreateContainerOptions {
                    name: plan.name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .context("worker container creation failed")?;
        self.client
            .start_container(&plan.name, None::<StartContainerOptions<String>>)
            .await
            .context("worker container startup failed")?;
        Ok(WorkerHealth {
            ready: false,
            restarted,
        })
    }
    async fn remove(&self, plan: &WorkerPlan) -> Result<bool> {
        match self
            .client
            .inspect_container(&plan.name, None::<InspectContainerOptions>)
            .await
        {
            Err(error) if Self::missing(&error) => return Ok(true),
            Err(error) => return Err(error.into()),
            Ok(container) => self.require_ownership(&container, plan)?,
        }
        match self
            .client
            .stop_container(&plan.name, Some(StopContainerOptions { t: 15 }))
            .await
        {
            Ok(_) => {}
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 304 | 404, ..
            }) => {}
            Err(error) => return Err(error.into()),
        }
        match self
            .client
            .remove_container(
                &plan.name,
                Some(RemoveContainerOptions {
                    force: false,
                    v: false,
                    link: false,
                }),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if Self::missing(&error) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }
}
#[async_trait]
impl ISigbotDeployer for SigbotDockerDeployer {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    async fn startup(&self) {
        let provider = Arc::new(self.clone());
        let reconciler = Arc::new(
            WorkerReconciler::new(self.state.clone(), provider).expect("invalid Docker controller configuration"),
        );
        self.scheduler
            .start(reconciler)
            .await
            .expect("Docker controller startup failed");
    }
    async fn shutdown(&self) {
        self.scheduler.stop().await;
    }
}
