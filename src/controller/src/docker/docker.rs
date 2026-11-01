//! Docker uses deterministic worker names and replaces changed specifications atomically
//! with respect to the controller's PostgreSQL workflow lock.
use crate::worker::plan::WorkerPlan;
use crate::worker::reconciler::WorkerHealth;
use crate::worker::reconciler::WorkerProvider;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use bollard::container::Config;
use bollard::container::CreateContainerOptions;
use bollard::container::InspectContainerOptions;
use bollard::container::RemoveContainerOptions;
use bollard::container::StartContainerOptions;
use bollard::container::StopContainerOptions;
use bollard::models::ContainerInspectResponse;
use bollard::models::HealthConfig;
use bollard::models::HealthStatusEnum;
use bollard::models::HostConfig;
use bollard::models::RestartPolicy;
use bollard::models::RestartPolicyNameEnum;
use bollard::Docker;
use bollard::{BollardRequest, API_DEFAULT_VERSION};
use hyper::header::{HeaderValue, AUTHORIZATION};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use sha2::{Digest, Sha256};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_types::controller::{WorkerAuthConfig, WorkerConfiguration, WorkerInfo};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct DockerController {
    state: Arc<SigbotState>,
    pub(super) client: Docker,
}
impl DockerController {
    const RUNTIME_ENV_DIRECTORY: &'static str = "/run/sigbot/workers";

    pub async fn for_worker(state: Arc<SigbotState>, target: &WorkerInfo) -> Result<Self> {
        let client = match (&target.auth_config, target.conn_url.as_str()) {
            (WorkerAuthConfig::DockerAnonymous, endpoint) if endpoint.starts_with("unix://") => {
                Docker::connect_with_unix(endpoint.trim_start_matches("unix://"), 120, API_DEFAULT_VERSION)?
            }
            (WorkerAuthConfig::DockerAnonymous, endpoint) => Self::remote_client(endpoint, None)?,
            (WorkerAuthConfig::DockerJwt { token_secret }, endpoint) => {
                let token = get_config().secrets.require(token_secret)?.to_owned();
                Self::remote_client(endpoint, Some(token))?
            }
            _ => anyhow::bail!("invalid Docker worker authentication"),
        };
        client.ping().await.context("Docker worker is unreachable")?;
        Ok(Self { state, client })
    }

    fn remote_client(endpoint: &str, token: Option<String>) -> Result<Docker> {
        let authorization = token
            .map(|value| HeaderValue::from_str(&format!("Bearer {value}")))
            .transpose()
            .context("invalid Docker JWT")?;
        let connector = HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http1()
            .build();
        let client = Arc::new(Client::builder(TokioExecutor::new()).build(connector));
        let endpoint = endpoint.to_owned();
        Ok(Docker::connect_with_custom_transport(
            move |mut request: BollardRequest| {
                let client = client.clone();
                let authorization = authorization.clone();
                Box::pin(async move {
                    if let Some(value) = authorization {
                        request.headers_mut().insert(AUTHORIZATION, value);
                    }
                    client.request(request).await.map_err(bollard::errors::Error::from)
                })
            },
            Some(endpoint),
            120,
            API_DEFAULT_VERSION,
        )?)
    }
    pub(super) fn settings(target: &WorkerInfo) -> Result<(&str, &str, &str)> {
        match &target.configuration {
            WorkerConfiguration::Docker {
                network,
                environment_file,
                log_volume_prefix,
            } => Ok((network, environment_file, log_volume_prefix)),
            _ => anyhow::bail!("Docker worker requires Docker runtime configuration"),
        }
    }

    fn specification_hash(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<String> {
        let base = plan.specification_hash(
            &self.state.config.services.controller.images.sigbot,
            &self.state.config.services.controller.workers,
        )?;
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(base, target.id, &target.configuration))?)
        ))
    }

    pub(super) fn runtime_environment(target: &WorkerInfo) -> Result<HashMap<String, String>> {
        let (_, environment_file, _) = Self::settings(target)?;
        let path = std::path::Path::new(Self::RUNTIME_ENV_DIRECTORY).join(format!("{environment_file}.env"));
        let metadata = std::fs::metadata(&path).context("runtime environment file unavailable")?;
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
        Ok(values)
    }

    fn environment(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<Vec<String>> {
        let mut values = Self::runtime_environment(target)?;
        values.extend(plan.environment.clone());
        Ok(values
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect())
    }
    async fn config(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<Config<String>> {
        let config = &self.state.config.services.controller;
        let settings = &config.workers;
        let (network_name, _, log_volume_prefix) = Self::settings(target)?;
        ensure!(
            !network_name.is_empty() && !matches!(network_name, "host" | "bridge" | "none"),
            "dedicated operator-provisioned Docker network required"
        );
        // Network provisioning/firewall policy is an operator responsibility; no host networking.
        let network = self
            .client
            .inspect_network(network_name, None::<bollard::network::InspectNetworkOptions<String>>)
            .await
            .context("dedicated worker network unavailable")?;
        ensure!(
            network.labels.as_ref().and_then(|labels| labels.get("sigbot.io/scope"))
                == Some(&format!("{}:{}", target.env, target.tenant_id)),
            "Docker network does not match worker scope"
        );
        let hash = self.specification_hash(target, plan)?;
        let binds = if plan.persistent_logs {
            let name = plan.log_volume_name(log_volume_prefix)?;
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
            env: Some(self.environment(target, plan)?),
            labels: Some(HashMap::from([
                ("sigbot.io/specification".into(), hash),
                ("sigbot.io/managed-by".into(), "sigbot-controller".into()),
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
                network_mode: Some(network_name.to_owned()),
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
    fn require_ownership(
        &self,
        target: &WorkerInfo,
        container: &ContainerInspectResponse,
        plan: &WorkerPlan,
    ) -> Result<()> {
        let labels = container.config.as_ref().and_then(|config| config.labels.as_ref());
        let managed = labels
            .and_then(|labels| labels.get("sigbot.io/managed-by"))
            .is_some_and(|value| value == "sigbot-controller");
        let worker = labels.and_then(|labels| labels.get("sigbot.io/worker"));
        // Pre-label worker releases remain updatable only on this controller's
        // dedicated network. A label from another controller is never enough.
        let network = container
            .host_config
            .as_ref()
            .and_then(|host| host.network_mode.as_deref());
        let (network_name, _, _) = Self::settings(target)?;
        ensure!(
            managed && worker.is_none_or(|name| name == &plan.name) && network == Some(network_name),
            "worker container does not belong to this scoped controller"
        );
        Ok(())
    }
}
#[async_trait]
impl WorkerProvider for DockerController {
    async fn prepare(&self, target: &WorkerInfo) -> Result<WorkerHealth> {
        crate::docker::middleware::DockerMiddleware::new(
            self.client.clone(),
            target,
            &self.state.config.services.controller.images.sigbot,
        )
        .reconcile()
        .await
    }

    async fn apply(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<WorkerHealth> {
        let config = self.config(target, plan).await?;
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
                self.require_ownership(target, &container, plan)?;
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
                WorkerProvider::remove(self, target, plan).await?;
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
    async fn remove(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<bool> {
        match self
            .client
            .inspect_container(&plan.name, None::<InspectContainerOptions>)
            .await
        {
            Err(error) if Self::missing(&error) => return Ok(true),
            Err(error) => return Err(error.into()),
            Ok(container) => self.require_ownership(target, &container, plan)?,
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
