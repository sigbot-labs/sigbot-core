// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.

//! Container Runtime Abstraction
//!
//! Provides unified container management across different runtimes:
//! - Docker daemon (dockerd)
//! - containerd (with CRI plugin)
//! - CRI-O
//!
//! Priority order:
//! 1. Docker daemon socket (primary for dockerd)
//! 2. containerd socket (primary for containerd)
//! 3. CRI-O socket (primary for CRI-O)
//! 4. Docker CLI fallback (when no socket available)

use anyhow::{Result, Context, anyhow};
use std::path::Path;
use std::process::Command;

// ============================================================================
// Default Socket Paths
// ============================================================================

/// Docker daemon socket path
const DOCKER_SOCKET: &str = "/var/run/docker.sock";

/// containerd socket path
const CONTAINERD_SOCKET: &str = "/run/containerd/containerd.sock";

/// CRI-O socket path
const CRIO_SOCKET: &str = "/var/run/crio/crio.sock";

/// Alternative CRI-O socket path
const CRIO_SOCKET_ALT: &str = "/run/crio/crio.sock";

// ============================================================================
// Container Runtime Types
// ============================================================================

/// Supported container runtimes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerRuntime {
    /// Docker daemon (dockerd)
    Docker,
    /// containerd with CRI plugin
    Containerd,
    /// CRI-O
    Crio,
    /// CLI fallback (docker command)
    Cli,
}

impl std::fmt::Display for ContainerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContainerRuntime::Docker => write!(f, "docker"),
            ContainerRuntime::Containerd => write!(f, "containerd"),
            ContainerRuntime::Crio => write!(f, "crio"),
            ContainerRuntime::Cli => write!(f, "cli"),
        }
    }
}

// ============================================================================
// Container Runtime Manager
// ============================================================================

/// Manages container operations across different runtimes
pub struct ContainerRuntimeManager {
    runtime: ContainerRuntime,
    docker_client: Option<DockerClient>,
    #[allow(dead_code)]
    containerd_client: Option<ContainerdClient>,
    #[allow(dead_code)]
    crio_client: Option<CrioClient>,
}

impl ContainerRuntimeManager {
    /// Detect and initialize the best available container runtime
    pub fn detect() -> Result<Self> {
        log::info!("Detecting container runtime...");

        // Try Docker daemon first (most common)
        if Path::new(DOCKER_SOCKET).exists() {
            log::info!("Found Docker socket at {}", DOCKER_SOCKET);
            if let Ok(client) = DockerClient::new(DOCKER_SOCKET) {
                if client.ping().is_ok() {
                    log::info!("Using Docker daemon runtime");
                    return Ok(Self {
                        runtime: ContainerRuntime::Docker,
                        docker_client: Some(client),
                        containerd_client: None,
                        crio_client: None,
                    });
                }
            }
        }

        // Try containerd
        if Path::new(CONTAINERD_SOCKET).exists() {
            log::info!("Found containerd socket at {}", CONTAINERD_SOCKET);
            if let Ok(client) = ContainerdClient::new(CONTAINERD_SOCKET) {
                if client.ping().is_ok() {
                    log::info!("Using containerd runtime");
                    return Ok(Self {
                        runtime: ContainerRuntime::Containerd,
                        docker_client: None,
                        containerd_client: Some(client),
                        crio_client: None,
                    });
                }
            }
        }

        // Try CRI-O (primary path)
        if Path::new(CRIO_SOCKET).exists() {
            log::info!("Found CRI-O socket at {}", CRIO_SOCKET);
            if let Ok(client) = CrioClient::new(CRIO_SOCKET) {
                if client.ping().is_ok() {
                    log::info!("Using CRI-O runtime");
                    return Ok(Self {
                        runtime: ContainerRuntime::Crio,
                        docker_client: None,
                        containerd_client: None,
                        crio_client: Some(client),
                    });
                }
            }
        }

        // Try CRI-O (alternative path)
        if Path::new(CRIO_SOCKET_ALT).exists() {
            log::info!("Found CRI-O socket at {}", CRIO_SOCKET_ALT);
            if let Ok(client) = CrioClient::new(CRIO_SOCKET_ALT) {
                if client.ping().is_ok() {
                    log::info!("Using CRI-O runtime (alt path)");
                    return Ok(Self {
                        runtime: ContainerRuntime::Crio,
                        docker_client: None,
                        containerd_client: None,
                        crio_client: Some(client),
                    });
                }
            }
        }

        // Fallback to CLI if docker command is available
        if Self::check_docker_cli() {
            log::info!("Falling back to Docker CLI");
            return Ok(Self {
                runtime: ContainerRuntime::Cli,
                docker_client: None,
                containerd_client: None,
                crio_client: None,
            });
        }

        Err(anyhow!("No container runtime found. Please ensure Docker, containerd, or CRI-O is installed and running."))
    }

    /// Get the detected runtime
    pub fn runtime(&self) -> ContainerRuntime {
        self.runtime
    }

    /// Check if docker CLI is available
    fn check_docker_cli() -> bool {
        Command::new("docker")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Run a container
    pub fn run_container(&self, config: &ContainerConfig) -> Result<String> {
        match self.runtime {
            ContainerRuntime::Docker => {
                self.docker_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("Docker client not initialized"))?
                    .run_container(config)
            }
            ContainerRuntime::Containerd => {
                self.containerd_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("containerd client not initialized"))?
                    .run_container(config)
            }
            ContainerRuntime::Crio => {
                self.crio_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("CRI-O client not initialized"))?
                    .run_container(config)
            }
            ContainerRuntime::Cli => Self::run_container_cli(config),
        }
    }

    /// Stop and remove a container
    pub fn remove_container(&self, container_id: &str) -> Result<()> {
        match self.runtime {
            ContainerRuntime::Docker => {
                self.docker_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("Docker client not initialized"))?
                    .remove_container(container_id)
            }
            ContainerRuntime::Containerd => {
                self.containerd_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("containerd client not initialized"))?
                    .remove_container(container_id)
            }
            ContainerRuntime::Crio => {
                self.crio_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("CRI-O client not initialized"))?
                    .remove_container(container_id)
            }
            ContainerRuntime::Cli => Self::remove_container_cli(container_id),
        }
    }

    /// Create a network
    pub fn create_network(&self, network_name: &str) -> Result<()> {
        match self.runtime {
            ContainerRuntime::Docker => {
                self.docker_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("Docker client not initialized"))?
                    .create_network(network_name)
            }
            ContainerRuntime::Containerd => {
                self.containerd_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("containerd client not initialized"))?
                    .create_network(network_name)
            }
            ContainerRuntime::Crio => {
                self.crio_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("CRI-O client not initialized"))?
                    .create_network(network_name)
            }
            ContainerRuntime::Cli => Self::create_network_cli(network_name),
        }
    }

    /// Remove a network
    pub fn remove_network(&self, network_name: &str) -> Result<()> {
        match self.runtime {
            ContainerRuntime::Docker => {
                self.docker_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("Docker client not initialized"))?
                    .remove_network(network_name)
            }
            ContainerRuntime::Containerd => {
                self.containerd_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("containerd client not initialized"))?
                    .remove_network(network_name)
            }
            ContainerRuntime::Crio => {
                self.crio_client
                    .as_ref()
                    .ok_or_else(|| anyhow!("CRI-O client not initialized"))?
                    .remove_network(network_name)
            }
            ContainerRuntime::Cli => Self::remove_network_cli(network_name),
        }
    }

    /// CLI fallback: Run container
    fn run_container_cli(config: &ContainerConfig) -> Result<String> {
        let mut cmd = Command::new("docker");
        cmd.arg("run").arg("-d").arg("--name").arg(&config.name);

        // Network
        if let Some(network) = &config.network {
            cmd.arg("--network").arg(network);
        }

        // Environment variables
        for (key, value) in &config.env {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }

        // Port mappings
        for (host_port, container_port) in &config.ports {
            cmd.arg("-p").arg(format!("{}:{}", host_port, container_port));
        }

        // Image
        cmd.arg(&config.image);

        // Command args
        if !config.command.is_empty() {
            cmd.args(&config.command);
        }

        let output = cmd.output().context("Failed to run docker command")?;

        if !output.status.success() {
            return Err(anyhow!(
                "Docker CLI failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// CLI fallback: Remove container
    fn remove_container_cli(container_id: &str) -> Result<()> {
        Command::new("docker")
            .args(["rm", "-f", container_id])
            .output()
            .context("Failed to remove container")?;
        Ok(())
    }

    /// CLI fallback: Create network
    fn create_network_cli(network_name: &str) -> Result<()> {
        let output = Command::new("docker")
            .args(["network", "create", network_name])
            .output()
            .context("Failed to create network")?;

        if !output.status.success() {
            // Network might already exist, which is fine
            log::debug!("Network creation output: {:?}", output);
        }
        Ok(())
    }

    /// CLI fallback: Remove network
    fn remove_network_cli(network_name: &str) -> Result<()> {
        Command::new("docker")
            .args(["network", "rm", network_name])
            .output()
            .context("Failed to remove network")?;
        Ok(())
    }
}

// ============================================================================
// Container Configuration
// ============================================================================

/// Container run configuration
#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub name: String,
    pub image: String,
    pub network: Option<String>,
    pub env: Vec<(String, String)>,
    pub ports: Vec<(u16, u16)>,
    pub command: Vec<String>,
    pub volumes: Vec<(String, String)>,
}

impl ContainerConfig {
    pub fn new(name: &str, image: &str) -> Self {
        Self {
            name: name.to_string(),
            image: image.to_string(),
            network: None,
            env: Vec::new(),
            ports: Vec::new(),
            command: Vec::new(),
            volumes: Vec::new(),
        }
    }

    pub fn with_network(mut self, network: &str) -> Self {
        self.network = Some(network.to_string());
        self
    }

    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.to_string(), value.to_string()));
        self
    }

    pub fn with_port(mut self, host: u16, container: u16) -> Self {
        self.ports.push((host, container));
        self
    }

    pub fn with_command(mut self, cmd: &[&str]) -> Self {
        self.command = cmd.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_volume(mut self, host: &str, container: &str) -> Self {
        self.volumes.push((host.to_string(), container.to_string()));
        self
    }
}

// ============================================================================
// Docker Client (REST API via hyper local connector or CLI fallback)
// ============================================================================

/// Docker daemon REST API client (simplified - uses CLI internally)
struct DockerClient {
    #[allow(dead_code)]
    socket_path: String,
}

impl DockerClient {
    fn new(socket_path: &str) -> Result<Self> {
        // Verify socket exists and is accessible
        if !Path::new(socket_path).exists() {
            return Err(anyhow!("Docker socket not found at {}", socket_path));
        }

        Ok(Self {
            socket_path: socket_path.to_string(),
        })
    }

    fn ping(&self) -> Result<()> {
        // Use docker CLI for ping as a simple check
        let output = Command::new("docker")
            .args(["info"])
            .output()
            .context("Failed to run docker info")?;

        if output.status.success() {
            Ok(())
        } else {
            Err(anyhow!("Docker daemon ping failed"))
        }
    }

    fn run_container(&self, config: &ContainerConfig) -> Result<String> {
        let mut cmd = Command::new("docker");
        cmd.arg("run").arg("-d").arg("--name").arg(&config.name);

        // Network
        if let Some(network) = &config.network {
            cmd.arg("--network").arg(network);
        }

        // Environment variables
        for (key, value) in &config.env {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }

        // Port mappings
        for (host_port, container_port) in &config.ports {
            cmd.arg("-p").arg(format!("{}:{}", host_port, container_port));
        }

        // Image
        cmd.arg(&config.image);

        // Command args
        if !config.command.is_empty() {
            cmd.args(&config.command);
        }

        let output = cmd.output().context("Failed to run docker command")?;

        if !output.status.success() {
            return Err(anyhow!(
                "Docker failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        Command::new("docker")
            .args(["rm", "-f", container_id])
            .output()
            .context("Failed to remove container")?;
        Ok(())
    }

    fn create_network(&self, network_name: &str) -> Result<()> {
        let output = Command::new("docker")
            .args(["network", "create", network_name])
            .output()
            .context("Failed to create network")?;

        if !output.status.success() {
            log::debug!("Network creation output: {:?}", output);
        }
        Ok(())
    }

    fn remove_network(&self, network_name: &str) -> Result<()> {
        Command::new("docker")
            .args(["network", "rm", network_name])
            .output()
            .context("Failed to remove network")?;
        Ok(())
    }
}

// ============================================================================
// containerd Client (CRI API)
// ============================================================================

/// containerd CRI client
#[allow(dead_code)]
struct ContainerdClient {
    socket_path: String,
}

#[allow(dead_code)]
impl ContainerdClient {
    fn new(socket_path: &str) -> Result<Self> {
        Ok(Self {
            socket_path: socket_path.to_string(),
        })
    }

    fn ping(&self) -> Result<()> {
        // containerd CRI doesn't have a simple ping endpoint
        // We'll use crictl as a proxy for checking connectivity
        let output = Command::new("crictl")
            .args(["info"])
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .output();

        match output {
            Ok(o) if o.status.success() => Ok(()),
            _ => Err(anyhow!("containerd not responding")),
        }
    }

    fn run_container(&self, config: &ContainerConfig) -> Result<String> {
        // Use crictl to run container (simpler than raw CRI API)
        let pod_config = self.build_pod_config(config);
        let pod_id = self.create_pod(&pod_config)?;
        Ok(pod_id)
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        Command::new("crictl")
            .args(["rmp", "-f", container_id])
            .output()
            .context("Failed to remove pod")?;
        Ok(())
    }

    fn create_network(&self, _network_name: &str) -> Result<()> {
        // containerd CRI uses CNI for networking, networks are pre-configured
        log::debug!("containerd: Network creation skipped (CNI managed)");
        Ok(())
    }

    fn remove_network(&self, _network_name: &str) -> Result<()> {
        log::debug!("containerd: Network removal skipped (CNI managed)");
        Ok(())
    }

    fn create_pod(&self, config: &serde_json::Value) -> Result<String> {
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new()
            .context("Failed to create temp file")?;
        temp_file
            .write_all(config.to_string().as_bytes())
            .context("Failed to write config")?;

        let output = Command::new("crictl")
            .args(["runp", temp_file.path().to_str().unwrap()])
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .output()
            .context("Failed to run crictl")?;

        if !output.status.success() {
            return Err(anyhow!(
                "crictl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn build_pod_config(&self, config: &ContainerConfig) -> serde_json::Value {
        serde_json::json!({
            "metadata": {
                "name": config.name,
                "namespace": "itest"
            },
            "linux": {},
            "log_directory": "/var/log/pods"
        })
    }
}

// ============================================================================
// CRI-O Client
// ============================================================================

/// CRI-O client
#[allow(dead_code)]
struct CrioClient {
    socket_path: String,
}

#[allow(dead_code)]
impl CrioClient {
    fn new(socket_path: &str) -> Result<Self> {
        Ok(Self {
            socket_path: socket_path.to_string(),
        })
    }

    fn ping(&self) -> Result<()> {
        // Use crictl to check CRI-O connectivity
        let output = Command::new("crictl")
            .args(["info"])
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .output();

        match output {
            Ok(o) if o.status.success() => Ok(()),
            _ => Err(anyhow!("CRI-O not responding")),
        }
    }

    fn run_container(&self, config: &ContainerConfig) -> Result<String> {
        // CRI-O also uses crictl
        let pod_config = self.build_pod_config(config);
        let pod_id = self.create_pod(&pod_config)?;
        Ok(pod_id)
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        Command::new("crictl")
            .args(["rmp", "-f", container_id])
            .output()
            .context("Failed to remove pod")?;
        Ok(())
    }

    fn create_network(&self, _network_name: &str) -> Result<()> {
        log::debug!("CRI-O: Network creation skipped (CNI managed)");
        Ok(())
    }

    fn remove_network(&self, _network_name: &str) -> Result<()> {
        log::debug!("CRI-O: Network removal skipped (CNI managed)");
        Ok(())
    }

    fn create_pod(&self, config: &serde_json::Value) -> Result<String> {
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new()
            .context("Failed to create temp file")?;
        temp_file
            .write_all(config.to_string().as_bytes())
            .context("Failed to write config")?;

        let output = Command::new("crictl")
            .args(["runp", temp_file.path().to_str().unwrap()])
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .output()
            .context("Failed to run crictl")?;

        if !output.status.success() {
            return Err(anyhow!(
                "crictl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn build_pod_config(&self, config: &ContainerConfig) -> serde_json::Value {
        serde_json::json!({
            "metadata": {
                "name": config.name,
                "namespace": "itest"
            },
            "linux": {},
            "log_directory": "/var/log/pods"
        })
    }
}

// ============================================================================
// Global Runtime Manager
// ============================================================================

/// Global container runtime manager (lazy initialized)
static mut RUNTIME_MANAGER: Option<ContainerRuntimeManager> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// Get or initialize the global runtime manager
#[allow(static_mut_refs)]
pub fn get_runtime_manager() -> Result<&'static ContainerRuntimeManager> {
    unsafe {
        INIT.call_once(|| {
            RUNTIME_MANAGER = Some(ContainerRuntimeManager::detect().expect("Failed to detect container runtime"));
        });
        Ok(RUNTIME_MANAGER.as_ref().unwrap())
    }
}

/// Initialize the global runtime manager
pub fn init_runtime_manager() -> Result<()> {
    let _ = get_runtime_manager()?;
    Ok(())
}
