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

//! Middleware Test Containers with Aliyun Mirror
//!
//! Provides Docker container management for E2E tests using:
//! - PostgreSQL (registry.aliyuncs.com/bitnami/postgresql:15)
//! - TimescaleDB (timescale/timescaledb:latest-pg15) - for time-series kline data
//! - Redis Cluster (registry.aliyuncs.com/bitnami/redis-cluster:7.0)
//! - Kafka (registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:3.5)
//! - MinIO (bitnami/minio:latest)
//! - EMQX MQTT Broker (emqx/emqx:5)
//!
//! Supports multiple container runtimes:
//! - Docker daemon (dockerd) - via REST API or CLI
//! - containerd (with CRI plugin) - via crictl
//! - CRI-O - via crictl

use std::time::Duration;
use tokio::time::sleep;
use sqlx::Row;
use super::container_runtime::{ContainerRuntimeManager, ContainerConfig};

// ============================================================================
// Container Image Configuration
// ============================================================================

/// PostgreSQL image
const POSTGRES_IMAGE: &str = "registry.cn-shenzhen.aliyuncs.com/wl4g/bitnami_postgresql:18.3";

/// TimescaleDB image (for time-series kline data)
const TIMESCALEDB_IMAGE: &str = "timescale/timescaledb:latest-pg15";

/// Redis Cluster image
const REDIS_CLUSTER_IMAGE: &str = "registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_redis_cluster:7.0";

/// Kafka image
const KAFKA_IMAGE: &str = "registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:3.5";

/// MinIO image
const MINIO_IMAGE: &str = "bitnami/minio:latest";

/// EMQX MQTT Broker image
const EMQX_IMAGE: &str = "emqx/emqx:5";

/// Default Docker network name prefix
const NETWORK_PREFIX: &str = "itest_network";

/// Container name prefix
const CONTAINER_PREFIX: &str = "sigbot_itest";

// ============================================================================
// Container Configuration Structures
// ============================================================================

/// PostgreSQL container configuration
#[derive(Debug, Clone)]
pub struct PostgresContainerConfig {
    pub image: &'static str,
    pub database: String,
    pub username: String,
    pub password: String,
    pub port: u16,
    pub host_port: u16,
}

impl Default for PostgresContainerConfig {
    fn default() -> Self {
        Self {
            image: POSTGRES_IMAGE,
            database: "testdb".to_string(),
            username: "test".to_string(),
            password: "test".to_string(),
            port: 5432,
            host_port: 5432,
        }
    }
}

/// TimescaleDB container configuration (for time-series kline data)
#[derive(Debug, Clone)]
pub struct TimescaleDBContainerConfig {
    pub image: &'static str,
    pub database: String,
    pub username: String,
    pub password: String,
    pub port: u16,
    pub host_port: u16,
}

impl Default for TimescaleDBContainerConfig {
    fn default() -> Self {
        Self {
            image: TIMESCALEDB_IMAGE,
            database: "klinedb".to_string(),
            username: "test".to_string(),
            password: "test".to_string(),
            port: 5433,
            host_port: 5433,
        }
    }
}

/// Redis Cluster container configuration
#[derive(Debug, Clone)]
pub struct RedisClusterContainerConfig {
    pub image: &'static str,
    pub password: String,
    pub port: u16,
    pub host_port: u16,
    pub cluster_nodes: usize,
}

impl Default for RedisClusterContainerConfig {
    fn default() -> Self {
        Self {
            image: REDIS_CLUSTER_IMAGE,
            password: "test".to_string(),
            port: 6379,
            host_port: 6379,
            cluster_nodes: 6, // 3 masters + 3 replicas
        }
    }
}

/// Kafka container configuration
#[derive(Debug, Clone)]
pub struct KafkaContainerConfig {
    pub image: &'static str,
    pub port: u16,
    pub host_port: u16,
    pub controller_port: u16,
}

impl Default for KafkaContainerConfig {
    fn default() -> Self {
        Self {
            image: KAFKA_IMAGE,
            port: 9092,
            host_port: 9092,
            controller_port: 9093,
        }
    }
}

/// MinIO container configuration
#[derive(Debug, Clone)]
pub struct MinioContainerConfig {
    pub image: &'static str,
    pub root_user: String,
    pub root_password: String,
    pub port: u16,
    pub console_port: u16,
    pub host_port: u16,
    pub host_console_port: u16,
}

impl Default for MinioContainerConfig {
    fn default() -> Self {
        Self {
            image: MINIO_IMAGE,
            root_user: "minioadmin".to_string(),
            root_password: "minioadmin".to_string(),
            port: 9000,
            console_port: 9001,
            host_port: 9000,
            host_console_port: 9001,
        }
    }
}

/// EMQX MQTT Broker container configuration
#[derive(Debug, Clone)]
pub struct EmqxContainerConfig {
    pub image: &'static str,
    pub port: u16,
    pub host_port: u16,
    pub dashboard_port: u16,
    pub host_dashboard_port: u16,
}

impl Default for EmqxContainerConfig {
    fn default() -> Self {
        Self {
            image: EMQX_IMAGE,
            port: 1883,
            host_port: 1883,
            dashboard_port: 18083,
            host_dashboard_port: 18083,
        }
    }
}

// ============================================================================
// Middleware Container Manager
// ============================================================================

/// Manages all middleware containers for E2E testing
///
/// Includes:
/// - PostgreSQL: wallet, ledger, balance, position, log storage
/// - TimescaleDB: time-series kline data for backtest
/// - Redis Cluster: caching layer
/// - Kafka: event streaming for export results
/// - MinIO: object storage
/// - EMQX: MQTT broker for inter-service messaging
pub struct MiddlewareContainerManager {
    pub postgres_config: PostgresContainerConfig,
    pub timescaledb_config: TimescaleDBContainerConfig,
    pub redis_config: RedisClusterContainerConfig,
    pub kafka_config: KafkaContainerConfig,
    pub minio_config: MinioContainerConfig,
    pub emqx_config: EmqxContainerConfig,
    pub network_name: String,
    pub container_prefix: String,
    runtime_manager: ContainerRuntimeManager,
}

impl MiddlewareContainerManager {
    /// Get the Kafka container name
    pub fn get_kafka_container_name(&self) -> String {
        format!("{}_kafka", self.container_prefix)
    }

    /// Get the PostgreSQL container name
    pub fn get_postgres_container_name(&self) -> String {
        format!("{}_postgres", self.container_prefix)
    }

    /// Get the TimescaleDB container name
    pub fn get_timescaledb_container_name(&self) -> String {
        format!("{}_timescaledb", self.container_prefix)
    }

    /// Get the Redis container name
    pub fn get_redis_container_name(&self) -> String {
        format!("{}_redis", self.container_prefix)
    }

    /// Get the MinIO container name
    pub fn get_minio_container_name(&self) -> String {
        format!("{}_minio", self.container_prefix)
    }

    /// Get the EMQX container name
    pub fn get_emqx_container_name(&self) -> String {
        format!("{}_emqx", self.container_prefix)
    }

    /// Create a new container manager with default configurations
    pub fn new() -> Self {
        let pid = std::process::id();
        let runtime_manager = ContainerRuntimeManager::detect().expect("Failed to detect container runtime");
        Self {
            postgres_config: PostgresContainerConfig::default(),
            timescaledb_config: TimescaleDBContainerConfig::default(),
            redis_config: RedisClusterContainerConfig::default(),
            kafka_config: KafkaContainerConfig::default(),
            minio_config: MinioContainerConfig::default(),
            emqx_config: EmqxContainerConfig::default(),
            network_name: format!("{}_{}", NETWORK_PREFIX, pid),
            container_prefix: format!("{}_{}", CONTAINER_PREFIX, pid),
            runtime_manager,
        }
    }

    /// Create container manager with custom configurations
    pub fn with_configs(
        postgres: PostgresContainerConfig,
        timescaledb: TimescaleDBContainerConfig,
        redis: RedisClusterContainerConfig,
        kafka: KafkaContainerConfig,
        minio: MinioContainerConfig,
        emqx: EmqxContainerConfig,
    ) -> Self {
        let pid = std::process::id();
        let runtime_manager = ContainerRuntimeManager::detect().expect("Failed to detect container runtime");
        Self {
            postgres_config: postgres,
            timescaledb_config: timescaledb,
            redis_config: redis,
            kafka_config: kafka,
            minio_config: minio,
            emqx_config: emqx,
            network_name: format!("{}_{}", NETWORK_PREFIX, pid),
            container_prefix: format!("{}_{}", CONTAINER_PREFIX, pid),
            runtime_manager,
        }
    }

    /// Start all containers and wait for services to be ready
    pub async fn start(&self) -> Result<(), String> {
        log::info!("Starting middleware containers...");
        log::info!("Network: {}", self.network_name);
        log::info!("Container prefix: {}", self.container_prefix);

        // Create Docker network
        self.create_network()?;

        // Start containers
        self.start_postgres()?;
        self.start_timescaledb()?;
        self.start_redis_cluster()?;
        self.start_kafka()?;
        self.start_minio()?;
        self.start_emqx()?;

        // Wait for services
        self.wait_for_services().await?;

        log::info!("All middleware containers started successfully");
        Ok(())
    }

    /// Stop and remove all containers
    pub fn stop(&self) {
        log::info!("Stopping middleware containers...");
        self.stop_containers();
        self.stop_redis();
        self.stop_kafka();
        self.stop_minio();
        self.stop_emqx();
        self.remove_network();
        log::info!("Middleware containers stopped");
    }

    /// Get PostgreSQL connection string
    pub fn postgres_url(&self) -> String {
        format!(
            "postgres://{}:{}@localhost:{}/{}",
            self.postgres_config.username,
            self.postgres_config.password,
            self.postgres_config.host_port,
            self.postgres_config.database
        )
    }

    /// Get TimescaleDB connection string
    pub fn timescaledb_url(&self) -> String {
        format!(
            "postgres://{}:{}@localhost:{}/{}",
            self.timescaledb_config.username,
            self.timescaledb_config.password,
            self.timescaledb_config.host_port,
            self.timescaledb_config.database
        )
    }

    /// Get Redis connection string (single node for cluster)
    pub fn redis_url(&self) -> String {
        format!(
            "redis://:{}@localhost:{}/",
            self.redis_config.password, self.redis_config.host_port
        )
    }

    /// Get MinIO endpoint
    pub fn minio_endpoint(&self) -> String {
        format!("http://localhost:{}/", self.minio_config.host_port)
    }

    /// Get Kafka bootstrap servers
    pub fn kafka_bootstrap_servers(&self) -> String {
        format!("localhost:{}", self.kafka_config.host_port)
    }

    /// Get Kafka topic for export results
    pub fn kafka_export_topic(&self) -> String {
        "sigbot-export-results".to_string()
    }

    /// Get EMQX MQTT broker URL
    pub fn mqtt_broker_url(&self) -> String {
        format!("tcp://localhost:{}", self.emqx_config.host_port)
    }

    /// Get EMQX MQTT WebSocket URL
    pub fn mqtt_ws_url(&self) -> String {
        format!("ws://localhost:{}/mqtt", self.emqx_config.host_port)
    }
}

// ============================================================================
// Docker Network Management
// ============================================================================

impl MiddlewareContainerManager {
    fn create_network(&self) -> Result<(), String> {
        log::debug!("Creating network: {}", self.network_name);

        self.runtime_manager
            .create_network(&self.network_name)
            .map_err(|e| format!("Failed to create network: {}", e))
    }

    fn remove_network(&self) {
        log::debug!("Removing network: {}", self.network_name);
        let _ = self.runtime_manager.remove_network(&self.network_name);
    }
}

// ============================================================================
// Container Lifecycle Management
// ============================================================================

impl MiddlewareContainerManager {
    fn start_postgres(&self) -> Result<(), String> {
        let name = format!("{}_postgres", self.container_prefix);
        log::debug!("Starting PostgreSQL container: {}", name);

        let config = ContainerConfig::new(&name, self.postgres_config.image)
            .with_network(&self.network_name)
            .with_env("POSTGRESQL_DATABASE", &self.postgres_config.database)
            .with_env("POSTGRESQL_USERNAME", &self.postgres_config.username)
            .with_env("POSTGRESQL_PASSWORD", &self.postgres_config.password)
            .with_env("POSTGRESQL_PORT_NUMBER", &self.postgres_config.port.to_string())
            .with_port(self.postgres_config.host_port, self.postgres_config.port);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start PostgreSQL: {}", e))?;

        Ok(())
    }

    fn start_timescaledb(&self) -> Result<(), String> {
        let name = format!("{}_timescaledb", self.container_prefix);
        log::debug!("Starting TimescaleDB container: {}", name);

        let config = ContainerConfig::new(&name, self.timescaledb_config.image)
            .with_network(&self.network_name)
            .with_env("POSTGRES_DB", &self.timescaledb_config.database)
            .with_env("POSTGRES_USER", &self.timescaledb_config.username)
            .with_env("POSTGRES_PASSWORD", &self.timescaledb_config.password)
            .with_port(self.timescaledb_config.host_port, self.timescaledb_config.port);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start TimescaleDB: {}", e))?;

        Ok(())
    }

    fn start_redis_cluster(&self) -> Result<(), String> {
        let name = format!("{}_redis", self.container_prefix);
        log::debug!("Starting Redis Cluster container: {}", name);

        let config = ContainerConfig::new(&name, self.redis_config.image)
            .with_network(&self.network_name)
            .with_env("REDIS_PASSWORD", &self.redis_config.password)
            .with_env("ALLOW_EMPTY_PASSWORD", "no")
            .with_env("REDIS_CLUSTER_REPLICAS", "1")
            .with_env("REDIS_CLUSTER_CREATOR", "yes")
            .with_env("REDIS_CLUSTER_NODES", &self.redis_config.cluster_nodes.to_string())
            .with_port(self.redis_config.host_port, self.redis_config.port);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start Redis Cluster: {}", e))?;

        Ok(())
    }

    fn start_kafka(&self) -> Result<(), String> {
        let name = format!("{}_kafka", self.container_prefix);
        log::debug!("Starting Kafka container: {}", name);

        let controller_port = self.kafka_config.controller_port;

        let config = ContainerConfig::new(&name, self.kafka_config.image)
            .with_network(&self.network_name)
            .with_env("ALLOW_PLAINTEXT_LISTENER", "yes")
            .with_env("KAFKA_CFG_NODE_ID", "0")
            .with_env("KAFKA_CFG_PROCESS_ROLES", "controller,broker")
            .with_env("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", &format!("0@localhost:{}", controller_port))
            .with_env("KAFKA_CFG_LISTENERS", &format!("PLAINTEXT://:{},CONTROLLER://:{}", self.kafka_config.port, controller_port))
            .with_env("KAFKA_CFG_ADVERTISED_LISTENERS", &format!("PLAINTEXT://localhost:{}", self.kafka_config.host_port))
            .with_env("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .with_env("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .with_env("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "true")
            .with_port(self.kafka_config.host_port, self.kafka_config.port);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start Kafka: {}", e))?;

        Ok(())
    }

    fn start_minio(&self) -> Result<(), String> {
        let name = format!("{}_minio", self.container_prefix);
        log::debug!("Starting MinIO container: {}", name);

        let config = ContainerConfig::new(&name, self.minio_config.image)
            .with_network(&self.network_name)
            .with_env("MINIO_ROOT_USER", &self.minio_config.root_user)
            .with_env("MINIO_ROOT_PASSWORD", &self.minio_config.root_password)
            .with_port(self.minio_config.host_port, self.minio_config.port)
            .with_port(self.minio_config.host_console_port, self.minio_config.console_port)
            .with_command(&["server", "/data", &format!("--console-address :{}", self.minio_config.console_port)]);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start MinIO: {}", e))?;

        Ok(())
    }

    fn start_emqx(&self) -> Result<(), String> {
        let name = format!("{}_emqx", self.container_prefix);
        log::debug!("Starting EMQX container: {}", name);

        let config = ContainerConfig::new(&name, self.emqx_config.image)
            .with_network(&self.network_name)
            .with_env("EMQX_LISTENER__TCP__DEFAULT", "1883")
            .with_env("EMQX_DASHBOARD__DEFAULT_LISTENER__HTTP", "18083")
            .with_port(self.emqx_config.host_port, self.emqx_config.port)
            .with_port(self.emqx_config.host_dashboard_port, self.emqx_config.dashboard_port);

        self.runtime_manager
            .run_container(&config)
            .map_err(|e| format!("Failed to start EMQX: {}", e))?;

        Ok(())
    }

    fn stop_containers(&self) {
        let containers = [
            format!("{}_postgres", self.container_prefix),
            format!("{}_timescaledb", self.container_prefix),
        ];

        for container in &containers {
            log::debug!("Removing container: {}", container);
            let _ = self.runtime_manager.remove_container(&container);
        }
    }

    fn stop_redis(&self) {
        let name = format!("{}_redis", self.container_prefix);
        log::debug!("Removing Redis container: {}", name);
        let _ = self.runtime_manager.remove_container(&name);
    }

    fn stop_kafka(&self) {
        let name = format!("{}_kafka", self.container_prefix);
        log::debug!("Removing Kafka container: {}", name);
        let _ = self.runtime_manager.remove_container(&name);
    }

    fn stop_minio(&self) {
        let name = format!("{}_minio", self.container_prefix);
        log::debug!("Removing MinIO container: {}", name);
        let _ = self.runtime_manager.remove_container(&name);
    }

    fn stop_emqx(&self) {
        let name = format!("{}_emqx", self.container_prefix);
        log::debug!("Removing EMQX container: {}", name);
        let _ = self.runtime_manager.remove_container(&name);
    }
}

// ============================================================================
// Service Health Check
// ============================================================================

impl MiddlewareContainerManager {
    async fn wait_for_services(&self) -> Result<(), String> {
        log::info!("Waiting for services to be ready...");

        self.wait_for_postgres().await?;
        self.wait_for_timescaledb().await?;
        self.wait_for_redis().await?;
        self.wait_for_kafka().await?;
        self.wait_for_minio().await?;
        self.wait_for_emqx().await?;

        log::info!("All services are ready");
        Ok(())
    }

    async fn wait_for_postgres(&self) -> Result<(), String> {
        let url = self.postgres_url();
        log::debug!("Waiting for PostgreSQL at {}", url);

        for i in 0..30 {
            match sqlx::PgPool::connect(&url).await {
                Ok(pool) => {
                    match sqlx::query("SELECT 1").fetch_one(&pool).await {
                        Ok(_) => {
                            log::info!("PostgreSQL is ready");
                            drop(pool);
                            return Ok(());
                        }
                        Err(e) => log::warn!("PostgreSQL query failed: {}", e),
                    }
                }
                Err(e) => log::warn!("PostgreSQL connection failed: {}", e),
            }

            if i == 29 {
                return Err("PostgreSQL startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn wait_for_timescaledb(&self) -> Result<(), String> {
        let url = self.timescaledb_url();
        log::debug!("Waiting for TimescaleDB at {}", url);

        for i in 0..30 {
            match sqlx::PgPool::connect(&url).await {
                Ok(pool) => {
                    match sqlx::query("SELECT version()").fetch_one(&pool).await {
                        Ok(row) => {
                            let version: String = row.get(0);
                            log::info!("TimescaleDB is ready: {}", version);
                            drop(pool);
                            return Ok(());
                        }
                        Err(e) => log::warn!("TimescaleDB query failed: {}", e),
                    }
                }
                Err(e) => log::warn!("TimescaleDB connection failed: {}", e),
            }

            if i == 29 {
                return Err("TimescaleDB startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn wait_for_redis(&self) -> Result<(), String> {
        let url = self.redis_url();
        log::debug!("Waiting for Redis Cluster at {}", url);

        for i in 0..30 {
            match redis::Client::open(url.as_str()) {
                Ok(client) => {
                    match client.get_connection() {
                        Ok(mut conn) => {
                            let result: redis::RedisResult<String> = redis::cmd("PING").query(&mut conn);
                            match result {
                                Ok(pong) => {
                                    log::info!("Redis Cluster is ready: {}", pong);
                                    return Ok(());
                                }
                                Err(e) => log::warn!("Redis PING failed: {}", e),
                            }
                        }
                        Err(e) => log::warn!("Redis connection failed: {}", e),
                    }
                }
                Err(e) => log::warn!("Redis client creation failed: {}", e),
            }

            if i == 29 {
                return Err("Redis Cluster startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn wait_for_kafka(&self) -> Result<(), String> {
        use std::process::Command;

        log::debug!("Waiting for Kafka to be ready...");

        for i in 0..30 {
            let kafka_container = format!("{}_kafka", self.container_prefix);
            let output = Command::new("docker")
                .args([
                    "exec",
                    &kafka_container,
                    "/opt/bitnami/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server", "localhost:9092",
                    "--list",
                ])
                .output();

            match output {
                Ok(result) => {
                    if result.status.success() {
                        log::info!("Kafka is ready");

                        // Create export topic
                        let _ = Command::new("docker")
                            .args([
                                "exec",
                                &kafka_container,
                                "/opt/bitnami/kafka/bin/kafka-topics.sh",
                                "--bootstrap-server", "localhost:9092",
                                "--create",
                                "--if-not-exists",
                                "--topic", &self.kafka_export_topic(),
                                "--partitions", "3",
                                "--replication-factor", "1",
                            ])
                            .output();

                        return Ok(());
                    } else {
                        log::warn!("Kafka not ready: {:?}", result);
                    }
                }
                Err(e) => log::warn!("Kafka command failed: {}", e),
            }

            if i == 29 {
                return Err("Kafka startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn wait_for_minio(&self) -> Result<(), String> {
        let health_url = format!("{}/minio/health/live", self.minio_endpoint());
        log::debug!("Waiting for MinIO at {}", health_url);

        for i in 0..30 {
            let client = reqwest::Client::new();
            match client.get(&health_url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        log::info!("MinIO is ready");
                        return Ok(());
                    } else {
                        log::warn!("MinIO health check returned: {}", response.status());
                    }
                }
                Err(e) => log::warn!("MinIO health check failed: {}", e),
            }

            if i == 29 {
                return Err("MinIO startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn wait_for_emqx(&self) -> Result<(), String> {
        let health_url = format!("http://localhost:{}/status", self.emqx_config.host_dashboard_port);
        log::debug!("Waiting for EMQX at {}", health_url);

        for i in 0..30 {
            let client = reqwest::Client::new();
            match client.get(&health_url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        log::info!("EMQX is ready");
                        return Ok(());
                    } else {
                        log::warn!("EMQX health check returned: {}", response.status());
                    }
                }
                Err(e) => log::warn!("EMQX health check failed: {}", e),
            }

            if i == 29 {
                return Err("EMQX startup timeout after 30 seconds".to_string());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

// ============================================================================
// E2E Context Integration
// ============================================================================

/// E2E Test Context with middleware containers
pub struct MiddlewareE2EContext {
    pub manager: MiddlewareContainerManager,
    pub postgres_url: String,
    pub timescaledb_url: String,
    pub redis_url: String,
    pub kafka_bootstrap_servers: String,
    pub kafka_export_topic: String,
    pub minio_endpoint: String,
    pub mqtt_broker_url: String,
    pub mqtt_ws_url: String,
}

impl Drop for MiddlewareE2EContext {
    fn drop(&mut self) {
        log::info!("Auto-cleaning up middleware containers for context");
        self.manager.stop();
    }
}

impl MiddlewareE2EContext {
    /// Setup complete test environment with middleware containers
    pub async fn setup() -> Self {
        let manager = MiddlewareContainerManager::new();

        manager.start().await.expect("Failed to start containers");

        Self {
            postgres_url: manager.postgres_url(),
            timescaledb_url: manager.timescaledb_url(),
            redis_url: manager.redis_url(),
            kafka_bootstrap_servers: manager.kafka_bootstrap_servers(),
            kafka_export_topic: manager.kafka_export_topic(),
            minio_endpoint: manager.minio_endpoint(),
            mqtt_broker_url: manager.mqtt_broker_url(),
            mqtt_ws_url: manager.mqtt_ws_url(),
            manager,
        }
    }

    /// Setup with custom configurations
    pub async fn setup_with_configs(
        postgres: PostgresContainerConfig,
        timescaledb: TimescaleDBContainerConfig,
        redis: RedisClusterContainerConfig,
        kafka: KafkaContainerConfig,
        minio: MinioContainerConfig,
        emqx: EmqxContainerConfig,
    ) -> Self {
        let manager = MiddlewareContainerManager::with_configs(postgres, timescaledb, redis, kafka, minio, emqx);

        manager.start().await.expect("Failed to start containers");

        Self {
            postgres_url: manager.postgres_url(),
            timescaledb_url: manager.timescaledb_url(),
            redis_url: manager.redis_url(),
            kafka_bootstrap_servers: manager.kafka_bootstrap_servers(),
            kafka_export_topic: manager.kafka_export_topic(),
            minio_endpoint: manager.minio_endpoint(),
            mqtt_broker_url: manager.mqtt_broker_url(),
            mqtt_ws_url: manager.mqtt_ws_url(),
            manager,
        }
    }

    /// Teardown test environment
    pub fn teardown(self) {
        self.manager.stop();
    }
}

// ============================================================================
// Default Configurations
// ============================================================================

impl Default for MiddlewareContainerManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_middleware_containers_lifecycle() {
        let ctx = MiddlewareE2EContext::setup().await;

        assert!(!ctx.postgres_url.is_empty());
        assert!(!ctx.timescaledb_url.is_empty());
        assert!(!ctx.redis_url.is_empty());
        assert!(!ctx.minio_endpoint.is_empty());
        assert!(!ctx.mqtt_broker_url.is_empty());

        assert!(ctx.postgres_url.contains("test:test"));
        assert!(ctx.redis_url.contains(":test@"));

        ctx.teardown();
    }

    #[test]
    fn test_default_configs() {
        let postgres = PostgresContainerConfig::default();
        assert!(postgres.image.contains("postgresql"));
        assert_eq!(postgres.database, "testdb");
        assert_eq!(postgres.port, 5432);

        let timescaledb = TimescaleDBContainerConfig::default();
        assert!(timescaledb.image.contains("timescaledb"));
        assert_eq!(timescaledb.database, "klinedb");
        assert_eq!(timescaledb.port, 5433);

        let redis = RedisClusterContainerConfig::default();
        assert!(redis.image.contains("redis"));
        assert_eq!(redis.cluster_nodes, 6);

        let emqx = EmqxContainerConfig::default();
        assert_eq!(emqx.port, 1883);
        assert_eq!(emqx.dashboard_port, 18083);
    }
}
