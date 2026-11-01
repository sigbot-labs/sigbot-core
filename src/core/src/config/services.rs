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

use serde::Deserialize;

use serde::Serialize;
use sigbot_types::controller::{MiddlewareDeployment, MiddlewareResources, TenantMiddlewareConfiguration};

// Services Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct ServicesProperties {
    #[serde(default = "ControllerProperties::default")]
    pub controller: ControllerProperties,
    #[serde(rename = "exchanges")]
    pub exchanges: ExchangeProperties,
    #[serde(rename = "backtest")]
    pub backtest: BacktestProperties,
    #[serde(rename = "api", default = "ApiProperties::default")]
    pub api: ApiProperties,
}

// Deploy Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ControllerProperties {
    #[serde(default)]
    pub workers: WorkerDeployProperties,
    #[serde(flatten)]
    pub inner: ScheduledPropertiesBase,
    #[serde(default = "DeployImagesProperties::default")]
    pub images: DeployImagesProperties,
    #[serde(default = "ControllerProperties::default_middleware")]
    pub middleware: TenantMiddlewareConfiguration,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ScheduledPropertiesBase {
    pub cron: String,
    pub channel_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeployImagesProperties {
    pub sigbot: String,
}

// Exchange Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct ExchangeProperties {
    #[serde(rename = "binance", default = "BinanceProperties::default")]
    pub binance: BinanceProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct BinanceProperties {
    // Spot
    pub spot_api_mainnet_endpoint: String,
    pub spot_api_testnet_endpoint: String,
    pub spot_ws_mainnet_endpoint: String,
    pub spot_ws_testnet_endpoint: String,
    // Spot-market
    pub spot_market_api_mainnet_endpoint: String,
    pub spot_market_ws_mainnet_endpoint: String,
    // USDS-derivatives
    pub usds_api_mainnet_endpoint: String,
    pub usds_api_testnet_endpoint: String,
    pub usds_ws_mainnet_endpoint: String,
    pub usds_ws_testnet_endpoint: String,
    // USDS-derivatives-market
    pub usds_market_api_mainnet_endpoint: String,
    pub usds_market_ws_mainnet_endpoint: String,
    // Coin-derivatives
    pub coin_api_mainnet_endpoint: String,
    pub coin_api_testnet_endpoint: String,
    pub coin_ws_mainnet_endpoint: String,
    pub coin_ws_testnet_endpoint: String,
    // Coin-derivatives-market
    pub coin_market_api_mainnet_endpoint: String,
    pub coin_market_ws_mainnet_endpoint: String,
}

// Backtest Properties.

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct BacktestProperties {
    #[serde(flatten)]
    pub inner: ScheduledPropertiesBase,
}

// API Properties with MCP integration.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiProperties {
    #[serde(rename = "mcp", default = "ApiMcpProperties::default")]
    pub mcp: ApiMcpProperties,
    #[serde(rename = "a2a", default = "A2AProperties::default")]
    pub a2a: A2AProperties,
    pub upload_temp_dir: String,
}

impl Default for ApiProperties {
    fn default() -> Self {
        Self {
            mcp: ApiMcpProperties::default(),
            a2a: A2AProperties::default(),
            upload_temp_dir: "/tmp/sigbot/knowledge_upload".to_owned(),
        }
    }
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
    /// Protocol transport. This process supports authenticated streamable HTTP.
    #[serde(rename = "transport")]
    pub transport: String,
    /// Bind address for SSE transport
    pub bind_address: String,
    /// Authentication enabled
    pub auth_enabled: bool,
}

impl Default for ApiMcpProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "sigbot-api-mcp-server".to_string(),
            transport: "streamable_http".to_string(),
            bind_address: "0.0.0.0:8080".to_string(),
            auth_enabled: true,
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
    pub bind_address: String,
    /// Externally reachable JSON-RPC endpoint advertised by the Agent Card.
    pub public_url: String,
    /// Authentication enabled
    pub auth_enabled: bool,
    /// Registered local agents
    #[serde(rename = "agents")]
    pub agents: Vec<AgentConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: String,
    pub description: String,
    pub version: String,
    pub capabilities: Vec<AgentCapabilityConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilityConfig {
    pub name: String,
    pub description: String,
}

impl Default for A2AProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:8081".to_string(),
            public_url: "http://127.0.0.1:8081/a2a".to_string(),
            auth_enabled: true,
            agents: vec![],
        }
    }
}

// Services Properties impls.

impl Default for ScheduledPropertiesBase {
    fn default() -> Self {
        ScheduledPropertiesBase {
            cron: String::from("0/30 * * * * * *"), // Every half minute
            channel_size: 200,
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

// Deploy Properties impls.

impl Default for DeployImagesProperties {
    fn default() -> Self {
        DeployImagesProperties {
            sigbot: "sigbot:latest".to_string(),
        }
    }
}

impl ControllerProperties {
    fn deployment(image: &str, cpu_millis: u32, memory_mb: u32, storage_gb: u32) -> MiddlewareDeployment {
        MiddlewareDeployment {
            image: image.into(),
            resources: MiddlewareResources {
                cpu_millis,
                memory_mb,
                storage_gb,
            },
        }
    }

    pub fn default_middleware() -> TenantMiddlewareConfiguration {
        TenantMiddlewareConfiguration {
            redis: Some(Self::deployment("docker.io/bitnami/redis-cluster:8.10", 500, 512, 8)),
            pgvector: Some(Self::deployment("docker.io/pgvector/pgvector:pg18", 1000, 2048, 20)),
            timescaledb: Some(Self::deployment(
                "docker.io/timescale/timescaledb-ha:pg18",
                1000,
                2048,
                50,
            )),
            emqx: Some(Self::deployment("docker.io/emqx/emqx:6.3.2", 1000, 1024, 20)),
        }
    }
}

impl Default for ControllerProperties {
    fn default() -> Self {
        Self {
            workers: WorkerDeployProperties::default(),
            inner: ScheduledPropertiesBase::default(),
            images: DeployImagesProperties::default(),
            middleware: Self::default_middleware(),
        }
    }
}

/// Runtime hosts are reconciled after their tenant middleware is ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerDeployProperties {
    pub enabled: bool,
    pub cpu_millis: u32,
    pub memory_mb: u32,
    pub pids_limit: i64,
    pub reconciliation_timeout_seconds: u64,
}
impl Default for WorkerDeployProperties {
    fn default() -> Self {
        Self {
            enabled: false,
            cpu_millis: 1000,
            memory_mb: 512,
            pids_limit: 64,
            reconciliation_timeout_seconds: 45,
        }
    }
}
