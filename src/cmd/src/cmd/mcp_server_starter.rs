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

//! MCP Server Starter
//!
//! Provides the command-line interface and startup logic for the MCP Server

use clap::{Arg, ArgMatches, Command};
use common_telemetry::{error, info};
use sigbot_mcp::config::{McpConfig, McpServerConfig};
use sigbot_mcp::server::{McPServer, SigbotResource, SigbotTool, ToolHandler};
use sigbot_mcp::client::{McpClient, McpClientManager};
use serde_json::Value;
use std::sync::Arc;

pub struct SigbotMCPServer;

impl SigbotMCPServer {
    pub const COMMAND_NAME: &'static str = "mcp-server";

    /// Build the command-line interface for the MCP Server
    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Start the MCP Server for Sigbot")
            .arg(
                Arg::new("BIND_ADDRESS")
                    .long("bind-address")
                    .value_name("ADDRESS")
                    .help("MCP Server bind address")
                    .default_value("0.0.0.0:8080"),
            )
            .arg(
                Arg::new("TRANSPORT")
                    .long("transport")
                    .value_name("TYPE")
                    .help("Transport type: sse, stdio, or both")
                    .default_value("sse"),
            )
            .arg(
                Arg::new("AUTH_ENABLED")
                    .long("auth-enabled")
                    .value_name("BOOL")
                    .help("Enable authentication")
                    .default_value("true"),
            )
            .arg(
                Arg::new("EXTERNAL_MCP_SERVICES")
                    .long("external-mcp-services")
                    .value_name("CONFIG")
                    .help("Path to external MCP services configuration file"),
            )
    }

    /// Run the MCP Server
    pub fn run(matches: &ArgMatches, verbose: bool) {
        if verbose {
            println!("Starting Sigbot MCP Server with verbose output...");
        }

        info!("Initializing Sigbot MCP Server...");

        // Get command line arguments
        let bind_address = matches.get_one::<String>("BIND_ADDRESS").unwrap();
        let transport = matches.get_one::<String>("TRANSPORT").unwrap();
        let auth_enabled = matches.get_one::<String>("AUTH_ENABLED").map(|s| s == "true").unwrap_or(true);

        // Create server configuration
        let config = McpServerConfig {
            name: "sigbot-mcp-server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            transport: transport.clone(),
            sse_bind_address: bind_address.clone(),
            auth_enabled,
            jwt_secret: std::env::var("SIGBOT_MCP_JWT_SECRET").ok(),
            rate_limit: Some(100),
            cors_enabled: true,
        };

        info!("MCP Server configuration: {:?}", config);

        // Create and configure the MCP server
        let mut server = McPServer::new(config.clone()).expect("Failed to create MCP server");

        // Register default Sigbot resources
        register_default_resources(&server);

        // Register default Sigbot tools
        register_default_tools(&server);

        // Initialize external MCP clients if configured
        if let Some(config_path) = matches.get_one::<String>("EXTERNAL_MCP_SERVICES") {
            info!("Loading external MCP services from: {}", config_path);
            // TODO: Load and register external MCP services
        }

        // Start the server
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

        rt.block_on(async {
            match server.start().await {
                Ok(_) => {
                    info!("MCP Server started successfully on {}", config.sse_bind_address);
                    info!("Transport: {}", config.transport);

                    // Keep the server running
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                        info!("MCP Server is running...");
                    }
                }
                Err(e) => {
                    error!("Failed to start MCP Server: {}", e);
                    std::process::exit(1);
                }
            }
        });
    }
}

/// Register default Sigbot resources
fn register_default_resources(server: &McPServer) {
    let registry = server.resource_registry();

    // Strategy resources
    registry.register(SigbotResource::new(
        "sigbot://strategies",
        "All Strategies",
        "List all available trading strategies",
    ));

    // Backtest resources
    registry.register(SigbotResource::new(
        "sigbot://backtests",
        "All Backtests",
        "List all backtest results",
    ));

    // Evaluator resources
    registry.register(SigbotResource::new(
        "sigbot://evaluators",
        "All Evaluators",
        "List all evaluator states",
    ));

    // Market data resources
    registry.register(SigbotResource::new(
        "sigbot://market-data",
        "Market Data",
        "Access real-time market data",
    ));

    // Position resources
    registry.register(SigbotResource::new(
        "sigbot://positions",
        "Positions",
        "View current trading positions",
    ));

    info!("Registered default MCP resources");
}

/// Register default Sigbot tools
fn register_default_tools(server: &McPServer) {
    let registry = server.tool_registry();

    // Tool: get_strategies
    registry.register(
        SigbotTool::new(
            "get_strategies",
            "Get list of all available trading strategies",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "tenant_id": {"type": "string", "description": "Tenant ID (optional)"}
                }
            }),
        ),
        GetStrategiesHandler,
    );

    // Tool: run_backtest
    registry.register(
        SigbotTool::new(
            "run_backtest",
            "Execute a backtest for a strategy",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "strategy_id": {"type": "string", "description": "Strategy ID"},
                    "start_time": {"type": "string", "description": "Start time (ISO 8601)"},
                    "end_time": {"type": "string", "description": "End time (ISO 8601)"},
                    "initial_capital": {"type": "number", "description": "Initial capital"}
                },
                "required": ["strategy_id", "start_time", "end_time"]
            }),
        ),
        RunBacktestHandler,
    );

    // Tool: get_market_data
    registry.register(
        SigbotTool::new(
            "get_market_data",
            "Get market data for a symbol",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Trading symbol"},
                    "data_type": {"type": "string", "description": "Data type: price, kline, orderbook"},
                    "interval": {"type": "string", "description": "Interval (for kline)"}
                },
                "required": ["symbol", "data_type"]
            }),
        ),
        GetMarketDataHandler,
    );

    // Tool: get_position
    registry.register(
        SigbotTool::new(
            "get_position",
            "Get current trading position",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "account_id": {"type": "string", "description": "Account ID"},
                    "symbol": {"type": "string", "description": "Trading symbol (optional)"}
                },
                "required": ["account_id"]
            }),
        ),
        GetPositionHandler,
    );

    info!("Registered default MCP tools");
}

// Tool Handlers

struct GetStrategiesHandler;

#[async_trait::async_trait]
impl ToolHandler for GetStrategiesHandler {
    async fn execute(&self, _args: Value) -> sigbot_mcp::error::Result<Value> {
        // TODO: Implement actual strategy fetching from sigbot-core
        Ok(serde_json::json!({
            "strategies": [
                {"id": "strategy_1", "name": "Moving Average Crossover", "status": "active"},
                {"id": "strategy_2", "name": "RSI Mean Reversion", "status": "active"}
            ]
        }))
    }
}

struct RunBacktestHandler;

#[async_trait::async_trait]
impl ToolHandler for RunBacktestHandler {
    async fn execute(&self, args: Value) -> sigbot_mcp::error::Result<Value> {
        let strategy_id = args.get("strategy_id").and_then(|s| s.as_str()).unwrap_or("default");
        let start_time = args.get("start_time").and_then(|s| s.as_str()).unwrap_or("");
        let end_time = args.get("end_time").and_then(|s| s.as_str()).unwrap_or("");

        info!("Running backtest: strategy={}, from={}, to={}", strategy_id, start_time, end_time);

        // TODO: Implement actual backtest execution via sigbot-core
        Ok(serde_json::json!({
            "backtest_id": "bt_123456",
            "strategy_id": strategy_id,
            "status": "running",
            "message": "Backtest started successfully"
        }))
    }
}

struct GetMarketDataHandler;

#[async_trait::async_trait]
impl ToolHandler for GetMarketDataHandler {
    async fn execute(&self, args: Value) -> sigbot_mcp::error::Result<Value> {
        let symbol = args.get("symbol").and_then(|s| s.as_str()).unwrap_or("BTCUSDT");
        let data_type = args.get("data_type").and_then(|s| s.as_str()).unwrap_or("price");

        info!("Fetching market data: symbol={}, type={}", symbol, data_type);

        // TODO: Implement actual market data fetching
        Ok(serde_json::json!({
            "symbol": symbol,
            "data_type": data_type,
            "data": {
                "price": 50000.0,
                "timestamp": chrono::Utc::now().timestamp_millis()
            }
        }))
    }
}

struct GetPositionHandler;

#[async_trait::async_trait]
impl ToolHandler for GetPositionHandler {
    async fn execute(&self, args: Value) -> sigbot_mcp::error::Result<Value> {
        let account_id = args.get("account_id").and_then(|s| s.as_str()).unwrap_or("default");
        let symbol = args.get("symbol").and_then(|s| s.as_str());

        info!("Fetching position: account={}, symbol={:?}", account_id, symbol);

        // TODO: Implement actual position fetching
        Ok(serde_json::json!({
            "account_id": account_id,
            "positions": [
                {
                    "symbol": symbol.unwrap_or("BTCUSDT"),
                    "quantity": 1.5,
                    "entry_price": 45000.0,
                    "current_price": 50000.0,
                    "pnl": 7500.0
                }
            ]
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_build() {
        let cmd = SigbotMCPServer::build();
        assert_eq!(cmd.get_name(), "mcp-server");
    }
}
