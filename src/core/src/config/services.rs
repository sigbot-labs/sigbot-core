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
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use serde::{Deserialize, Serialize};

// Services Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServicesProperties {
    #[serde(default = "DeployerProperties::default")]
    pub deployer: DeployerProperties,
    #[serde(rename = "exchanges")]
    pub exchanges: ExchangeProperties,
    #[serde(rename = "evaluator", default = "EvaluatorProperties::default")]
    pub evaluator: EvaluatorProperties,
    #[serde(rename = "backtest")]
    pub backtest: BacktestProperties,
    #[serde(rename = "api", default = "ApiProperties::default")]
    pub api: ApiProperties,
    #[serde(rename = "a2a", default = "A2AProperties::default")]
    pub a2a: A2AProperties,
}

// Deploy Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeployerProperties {
    #[serde(default)]
    pub workers: WorkerDeployProperties,
    #[serde(flatten)]
    pub inner: ScheduledPropertiesBase,
    #[serde(default = "DeployImagesProperties::default")]
    pub images: DeployImagesProperties,
    #[serde(default = "DeployNetworkProperties::default")]
    pub network: DeployNetworkProperties,
    #[serde(default = "DeployMiddlewareProperties::default")]
    pub middleware: DeployMiddlewareProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScheduledPropertiesBase {
    #[serde(rename = "cron")]
    pub cron: String,
    #[serde(alias = "channel-size")]
    pub channel_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeployImagesProperties {
    #[serde(rename = "postgresql")]
    pub postgresql: String,
    #[serde(rename = "emqx")]
    pub emqx: String,
    #[serde(rename = "redis")]
    pub redis: String,
    #[serde(rename = "timescaledb")]
    pub timescaledb: String,
    #[serde(rename = "sigbot")]
    pub sigbot: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeployNetworkProperties {
    #[serde(alias = "tenant-network-prefix")]
    pub tenant_network_prefix: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeployMiddlewareProperties {
    #[serde(default = "PostgresqlDeployProperties::default")]
    pub postgresql: PostgresqlDeployProperties,
    #[serde(default = "EmqxDeployProperties::default")]
    pub emqx: EmqxDeployProperties,
    #[serde(default = "RedisDeployProperties::default")]
    pub redis: RedisDeployProperties,
    #[serde(default = "TimescaledbDeployProperties::default")]
    pub timescaledb: TimescaledbDeployProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct EmqxDeployProperties {
    #[serde(rename = "mode")]
    pub mode: DeployMode,
    #[serde(rename = "replicas")]
    pub replicas: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PostgresqlDeployProperties {
    #[serde(rename = "mode")]
    pub mode: DeployMode,
    #[serde(rename = "replicas")]
    pub replicas: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TimescaledbDeployProperties {
    #[serde(rename = "mode")]
    pub mode: DeployMode,
    #[serde(rename = "replicas")]
    pub replicas: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedisDeployProperties {
    #[serde(rename = "mode")]
    pub mode: DeployMode,
    #[serde(rename = "replicas")]
    pub replicas: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum DeployMode {
    Standalone,
    Cluster,
}

// Exchange Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExchangeProperties {
    #[serde(rename = "binance", default = "BinanceProperties::default")]
    pub binance: BinanceProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct BinanceProperties {
    // Spot
    #[serde(alias = "spot-api-mainnet-endpoint")]
    pub spot_api_mainnet_endpoint: String,
    #[serde(alias = "spot-api-testnet-endpoint")]
    pub spot_api_testnet_endpoint: String,
    #[serde(alias = "spot-ws-mainnet-endpoint")]
    pub spot_ws_mainnet_endpoint: String,
    #[serde(alias = "spot-ws-testnet-endpoint")]
    pub spot_ws_testnet_endpoint: String,
    // Spot-market
    #[serde(alias = "spot-market-api-mainnet-endpoint")]
    pub spot_market_api_mainnet_endpoint: String,
    #[serde(alias = "spot-market-ws-mainnet-endpoint")]
    pub spot_market_ws_mainnet_endpoint: String,
    // USDS-derivatives
    #[serde(alias = "usds-api-mainnet-endpoint")]
    pub usds_api_mainnet_endpoint: String,
    #[serde(alias = "usds-ws-mainnet-endpoint")]
    pub usds_api_testnet_endpoint: String,
    #[serde(alias = "usds-ws-testnet-endpoint")]
    pub usds_ws_mainnet_endpoint: String,
    #[serde(alias = "usds-api-testnet-endpoint")]
    pub usds_ws_testnet_endpoint: String,
    // USDS-derivatives-market
    #[serde(alias = "usds-market-api-mainnet-endpoint")]
    pub usds_market_api_mainnet_endpoint: String,
    #[serde(alias = "usds-market-ws-mainnet-endpoint")]
    pub usds_market_ws_mainnet_endpoint: String,
    // Coin-derivatives
    #[serde(alias = "coin-api-mainnet-endpoint")]
    pub coin_api_mainnet_endpoint: String,
    #[serde(alias = "coin-api-testnet-endpoint")]
    pub coin_api_testnet_endpoint: String,
    #[serde(alias = "coin-ws-mainnet-endpoint")]
    pub coin_ws_mainnet_endpoint: String,
    #[serde(alias = "coin-ws-testnet-endpoint")]
    pub coin_ws_testnet_endpoint: String,
    // Coin-derivatives-market
    #[serde(alias = "coin-market-api-mainnet-endpoint")]
    pub coin_market_api_mainnet_endpoint: String,
    #[serde(alias = "coin-market-ws-mainnet-endpoint")]
    pub coin_market_ws_mainnet_endpoint: String,
}

// Evaluator Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvaluatorProperties {
    #[serde(flatten)]
    pub inner: ScheduledPropertiesBase,
    /// Data fetch window for time-driven scans (in hours)
    #[serde(alias = "data-window-hours")]
    pub data_window_hours: i64,
    /// Execution timeout (in seconds)
    #[serde(alias = "execution-timeout-secs")]
    pub execution_timeout_secs: u64,
    /// MCP servers configuration for evaluator to call external services
    #[serde(alias = "mcp-servers", default)]
    pub mcp_servers: Vec<EvaluatorMcpServerConfig>,
}

/// External MCP server configuration for Evaluator
/// Used when evaluator needs to call external MCP services (e.g., Binance, Bitget)
/// to fetch market data for dynamic parameter adjustment
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluatorMcpServerConfig {
    /// Service name (e.g., "binance-mcp", "bitget-mcp")
    #[serde(rename = "name")]
    pub name: String,
    /// Service URL (SSE endpoint or STDIO command)
    #[serde(rename = "url")]
    pub url: String,
    /// Transport type: "sse", "stdio"
    #[serde(rename = "transport")]
    pub transport: String,
    /// Authentication token (if required)
    #[serde(alias = "auth-token")]
    pub auth_token: Option<String>,
    /// API key (for exchange MCP services)
    #[serde(alias = "api-key")]
    pub api_key: Option<String>,
    /// API secret (for exchange MCP services)
    #[serde(alias = "api-secret")]
    pub api_secret: Option<String>,
    /// Enabled tools from this service
    #[serde(alias = "enabled-tools")]
    pub enabled_tools: Vec<String>,
    /// Timeout in seconds for MCP calls
    #[serde(alias = "timeout-secs", default = "EvaluatorMcpServerConfig::default_timeout")]
    pub timeout_secs: u64,
}

impl EvaluatorMcpServerConfig {
    fn default_timeout() -> u64 {
        30
    }
}

impl Default for EvaluatorMcpServerConfig {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            url: "".to_string(),
            transport: "sse".to_string(),
            auth_token: None,
            api_key: None,
            api_secret: None,
            enabled_tools: vec![],
            timeout_secs: 30,
        }
    }
}

// Backtest Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BacktestProperties {
    #[serde(flatten)]
    pub inner: ScheduledPropertiesBase,
}

// API Properties with MCP integration.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiProperties {
    /// Enable MCP server wrapping RESTful API
    #[serde(rename = "mcp", default = "ApiMcpProperties::default")]
    pub mcp: ApiMcpProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiMcpProperties {
    /// Enable MCP server for API exposure
    #[serde(rename = "enabled")]
    pub enabled: bool,
    /// MCP server name
    #[serde(rename = "name")]
    pub name: String,
    /// Transport type: "sse", "stdio", "streamable-http"
    #[serde(rename = "transport")]
    pub transport: String,
    /// Bind address for SSE transport
    #[serde(alias = "bind-address")]
    pub bind_address: String,
    /// Exposed API capabilities via MCP
    #[serde(rename = "capabilities")]
    pub capabilities: Vec<String>,
    /// Authentication enabled
    #[serde(alias = "auth-enabled")]
    pub auth_enabled: bool,
}

impl Default for ApiMcpProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "sigbot-api-mcp-server".to_string(),
            transport: "sse".to_string(),
            bind_address: "0.0.0.0:8080".to_string(),
            capabilities: vec![
                "trading".to_string(),
                "market-data".to_string(),
                "backtest".to_string(),
                "strategy".to_string(),
            ],
            auth_enabled: true,
        }
    }
}

impl Default for ApiProperties {
    fn default() -> Self {
        Self {
            mcp: ApiMcpProperties::default(),
        }
    }
}

// A2A (Agent-to-Agent) Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct A2AProperties {
    /// Enable A2A server for agent collaboration
    #[serde(rename = "enabled")]
    pub enabled: bool,
    /// Gateway bind address
    #[serde(alias = "bind-address")]
    pub bind_address: String,
    /// Authentication enabled
    #[serde(alias = "auth-enabled")]
    pub auth_enabled: bool,
    /// Rate limiting (requests per second)
    #[serde(alias = "rate-limit")]
    pub rate_limit: Option<u32>,
    /// Registered local agents
    #[serde(rename = "agents")]
    pub agents: Vec<AgentConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// Agent unique identifier
    #[serde(alias = "agent-id")]
    pub agent_id: String,
    /// Agent display name
    #[serde(rename = "name")]
    pub name: String,
    /// Agent description
    #[serde(rename = "description")]
    pub description: String,
    /// Agent version
    #[serde(rename = "version")]
    pub version: String,
    /// Agent capabilities
    #[serde(rename = "capabilities")]
    pub capabilities: Vec<AgentCapabilityConfig>,
    /// Agent endpoint (for remote agents)
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    /// Agent tags for discovery
    #[serde(rename = "tags")]
    pub tags: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilityConfig {
    /// Capability name
    #[serde(rename = "name")]
    pub name: String,
    /// Capability description
    #[serde(rename = "description")]
    pub description: String,
    /// Capability input schema (JSON Schema)
    #[serde(alias = "input-schema")]
    pub input_schema: Option<serde_json::Value>,
}

impl Default for A2AProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:8081".to_string(),
            auth_enabled: true,
            rate_limit: Some(100),
            agents: vec![],
        }
    }
}

// Services Properties impls.

impl Default for ServicesProperties {
    fn default() -> Self {
        ServicesProperties {
            exchanges: ExchangeProperties::default(),
            evaluator: EvaluatorProperties::default(),
            backtest: BacktestProperties::default(),
            deployer: DeployerProperties::default(),
            api: ApiProperties::default(),
            a2a: A2AProperties::default(),
        }
    }
}

impl Default for ScheduledPropertiesBase {
    fn default() -> Self {
        ScheduledPropertiesBase {
            cron: String::from("0/30 * * * * * *"), // Every half minute
            channel_size: 200,
        }
    }
}

impl Default for EvaluatorProperties {
    fn default() -> Self {
        EvaluatorProperties {
            inner: ScheduledPropertiesBase {
                cron: String::from("0/30 * * * * *"), // Every 30 seconds
                channel_size: 5,
            },
            data_window_hours: 4,        // Last 4 hours of data
            execution_timeout_secs: 300, // 5 minutes
            mcp_servers: vec![],
        }
    }
}

impl Default for ExchangeProperties {
    fn default() -> Self {
        ExchangeProperties {
            binance: BinanceProperties::default(),
        }
    }
}

impl Default for BinanceProperties {
    fn default() -> Self {
        BinanceProperties {
            // Spot, see:https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/rest-api/general-api-information
            spot_api_mainnet_endpoint: String::from("https://api.binance.com/api/v3"), // https://api-gcp.binance.com,https://api2.binance.com,https://api3.binance.com,https://api4.binance.com
            spot_api_testnet_endpoint: String::from("wss://stream.testnet.binance.vision/ws"),
            // see:https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/websocket-api/general-api-information
            spot_ws_mainnet_endpoint: String::from("wss://ws-api.binance.com:443/ws-api/v3"),
            spot_ws_testnet_endpoint: String::from("wss://ws-api.testnet.binance.vision/ws-api/v3"),
            // Spot-market
            spot_market_api_mainnet_endpoint: String::from("https://api.binance.com/api/v3"),
            // see:https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/web-socket-streams
            spot_market_ws_mainnet_endpoint: String::from("wss://stream.binance.com:443"),
            // USDS-derivatives, see:https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/general-info
            usds_api_mainnet_endpoint: String::from("https://fapi.binance.com/fapi/v1"),
            usds_api_testnet_endpoint: String::from("https://demo-fapi.binance.com"),
            // see:https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/websocket-api-general-info
            usds_ws_mainnet_endpoint: String::from("wss://ws-fapi.binance.com/ws-fapi/v1"),
            usds_ws_testnet_endpoint: String::from("wss://fstream.binancefuture.com"), // wss://testnet.binancefuture.com/ws-fapi/v1
            // USDS-derivatives-market, see:https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/websocket-market-streams
            usds_market_api_mainnet_endpoint: String::from("https://fapi.binance.com"),
            usds_market_ws_mainnet_endpoint: String::from("wss://fstream.binance.com"),
            // Coin-derivatives, see:https://developers.binance.com/docs/zh-CN/derivatives/coin-margined-futures/general-info
            coin_api_mainnet_endpoint: String::from("https://dapi.binance.com/dapi/v1"),
            coin_api_testnet_endpoint: String::from("https://testnet.binancefuture.com"),
            // see:https://developers.binance.com/docs/zh-CN/derivatives/coin-margined-futures/websocket-api-general-info
            coin_ws_mainnet_endpoint: String::from("wss://ws-dapi.binance.com/ws-dapi/v1"),
            coin_ws_testnet_endpoint: String::from("wss://dstream.binancefuture.com"), // wss://testnet.binancefuture.com/ws-dapi/v1
            // Coin-derivatives-market
            coin_market_api_mainnet_endpoint: String::from("https://dapi.binance.com/dapi/v1"),
            coin_market_ws_mainnet_endpoint: String::from("wss://ws-dapi.binance.com/ws-dapi/v1"),
        }
    }
}

impl Default for BacktestProperties {
    fn default() -> Self {
        BacktestProperties {
            inner: ScheduledPropertiesBase::default(),
        }
    }
}

// Deploy Properties impls.

impl Default for DeployerProperties {
    fn default() -> Self {
        DeployerProperties {
            workers: WorkerDeployProperties::default(),
            inner: ScheduledPropertiesBase::default(),
            images: DeployImagesProperties::default(),
            network: DeployNetworkProperties::default(),
            middleware: DeployMiddlewareProperties::default(),
        }
    }
}

impl Default for DeployImagesProperties {
    fn default() -> Self {
        DeployImagesProperties {
            sigbot: "sigbot:latest".to_string(),
            emqx: "emqx/emqx:latest".to_string(),
            postgresql: "postgres:16".to_string(),
            timescaledb: "timescale/timescaledb:latest-pg16".to_string(),
            redis: "redis:7-alpine".to_string(),
        }
    }
}

impl Default for DeployNetworkProperties {
    fn default() -> Self {
        DeployNetworkProperties {
            tenant_network_prefix: "sigbot-t".to_string(),
        }
    }
}

impl Default for DeployMiddlewareProperties {
    fn default() -> Self {
        DeployMiddlewareProperties {
            emqx: EmqxDeployProperties::default(),
            postgresql: PostgresqlDeployProperties::default(),
            timescaledb: TimescaledbDeployProperties::default(),
            redis: RedisDeployProperties::default(),
        }
    }
}

impl Default for EmqxDeployProperties {
    fn default() -> Self {
        EmqxDeployProperties {
            mode: DeployMode::Standalone,
            replicas: Some(1),
        }
    }
}

impl Default for PostgresqlDeployProperties {
    fn default() -> Self {
        PostgresqlDeployProperties {
            mode: DeployMode::Standalone,
            replicas: Some(1),
        }
    }
}

impl Default for TimescaledbDeployProperties {
    fn default() -> Self {
        TimescaledbDeployProperties {
            mode: DeployMode::Standalone,
            replicas: Some(1),
        }
    }
}

impl Default for RedisDeployProperties {
    fn default() -> Self {
        RedisDeployProperties {
            mode: DeployMode::Standalone,
            replicas: Some(1),
        }
    }
}

/// Runtime hosts only. Middleware is provisioned once through Helm.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerDeployProperties {
    pub enabled: bool,
    pub provider: WorkerDeployProvider,
    pub namespace: String,
    pub runtime_secret: String,
    /// Increment when rotating the referenced Secret or Docker env file.
    pub secret_revision: String,
    pub runtime_config_map: String,
    pub service_account: String,
    pub docker_network: String,
    pub docker_env_file: String,
    /// Existing per-worker volumes; the controller never creates or deletes them.
    pub log_pvc_prefix: String,
    pub docker_log_volume_prefix: String,
    pub cpu_millis: u32,
    pub memory_mb: u32,
    pub pids_limit: i64,
    pub reconciliation_timeout_seconds: u64,
    pub egress_cidrs: Vec<String>,
    pub egress_ports: Vec<u16>,
}
impl Default for WorkerDeployProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: WorkerDeployProvider::Kubernetes,
            namespace: "sigbot".into(),
            runtime_secret: "sigbot-runtime".into(),
            secret_revision: "1".into(),
            runtime_config_map: "sigbot-runtime".into(),
            service_account: "default".into(),
            docker_network: String::new(),
            docker_env_file: String::new(),
            log_pvc_prefix: String::new(),
            docker_log_volume_prefix: String::new(),
            cpu_millis: 1000,
            memory_mb: 512,
            pids_limit: 64,
            reconciliation_timeout_seconds: 45,
            egress_cidrs: Vec::new(),
            egress_ports: vec![1883, 5432, 6379, 443],
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerDeployProvider {
    Kubernetes,
    Docker,
}
