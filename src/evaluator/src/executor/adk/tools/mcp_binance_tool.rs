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

//! MCP Binance Tool - Fetch market data via MCP protocol
//!
//! This tool wraps external Binance MCP service calls
//! and implements the ADK Tool trait for integration.

use adk_core::{Tool, ToolContext, Result as AdkResult};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use serde_json::{json, Value};
use std::sync::Arc;

/// MCP Binance Market Data Tool
///
/// Fetches market data from external Binance MCP service
pub struct McpBinanceMarketTool {
    mcp_client: Option<Arc<sigbot_mcp::client::McpClient>>,
}

impl McpBinanceMarketTool {
    pub fn new(mcp_client: Option<Arc<sigbot_mcp::client::McpClient>>) -> Self {
        Self { mcp_client }
    }

    /// Execute the MCP tool call
    async fn call_mcp_tool(&self, tool_name: &str, args: Value) -> AdkResult<Value> {
        if let Some(client) = &self.mcp_client {
            client
                .call_tool(tool_name, args)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        } else {
            // Return mock data if no MCP client configured
            Ok(self.get_mock_data())
        }
    }

    fn get_mock_data(&self) -> Value {
        json!({
            "symbol": "BTCUSDT",
            "price": 50000.0,
            "timestamp": chrono::Utc::now().timestamp_millis(),
            "source": "binance-mcp-mock"
        })
    }
}

#[async_trait]
impl Tool for McpBinanceMarketTool {
    fn name(&self) -> &str {
        "mcp_binance_market"
    }

    fn description(&self) -> &str {
        "Get current market price from Binance via MCP protocol. \
         Use this tool to fetch real-time price data from external Binance MCP service."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol (e.g., 'BTCUSDT', 'ETHUSDT')"
                }
            },
            "required": ["symbol"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let symbol = args
            .get("symbol")
            .and_then(|s| s.as_str())
            .unwrap_or("BTCUSDT");

        info!("McpBinanceMarketTool: Fetching price for symbol={}", symbol);

        let result = self
            .call_mcp_tool(
                "get_price",
                json!({
                    "symbol": symbol
                }),
            )
            .await?;

        debug!("McpBinanceMarketTool: Got result: {:?}", result);

        Ok(json!({
            "symbol": symbol,
            "data": result,
            "source": "binance-mcp"
        }))
    }
}

/// MCP Binance Kline Tool
pub struct McpBinanceKlineTool {
    mcp_client: Option<Arc<sigbot_mcp::client::McpClient>>,
}

impl McpBinanceKlineTool {
    pub fn new(mcp_client: Option<Arc<sigbot_mcp::client::McpClient>>) -> Self {
        Self { mcp_client }
    }

    async fn call_mcp_tool(&self, tool_name: &str, args: Value) -> AdkResult<Value> {
        if let Some(client) = &self.mcp_client {
            client
                .call_tool(tool_name, args)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        } else {
            Ok(json!({
                "symbol": args.get("symbol").and_then(|s| s.as_str()).unwrap_or("BTCUSDT"),
                "interval": args.get("interval").and_then(|s| s.as_str()).unwrap_or("1h"),
                "klines": [],
                "note": "Mock data - MCP client not configured"
            }))
        }
    }
}

#[async_trait]
impl Tool for McpBinanceKlineTool {
    fn name(&self) -> &str {
        "mcp_binance_kline"
    }

    fn description(&self) -> &str {
        "Get historical kline/candlestick data from Binance via MCP protocol. \
         Returns OHLCV data for the specified interval."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol (e.g., 'BTCUSDT')"
                },
                "interval": {
                    "type": "string",
                    "description": "Kline interval (e.g., '1m', '5m', '15m', '30m', '1h', '4h', '1d')",
                    "enum": ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of klines to fetch (default: 100)",
                    "default": 100
                }
            },
            "required": ["symbol", "interval"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let symbol = args.get("symbol").and_then(|s| s.as_str()).unwrap_or("BTCUSDT");
        let interval = args.get("interval").and_then(|s| s.as_str()).unwrap_or("1h");

        info!(
            "McpBinanceKlineTool: Fetching klines for symbol={}, interval={}",
            symbol, interval
        );

        let result = self
            .call_mcp_tool(
                "get_klines",
                json!({
                    "symbol": symbol,
                    "interval": interval,
                    "limit": args.get("limit").and_then(|l| l.as_u64()).unwrap_or(100)
                }),
            )
            .await?;

        Ok(json!({
            "symbol": symbol,
            "interval": interval,
            "data": result,
            "source": "binance-mcp"
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_market_tool() {
        let tool = McpBinanceMarketTool::new(None);
        let result = tool
            .execute(Arc::new(()), json!({"symbol": "BTCUSDT"}))
            .await;
        assert!(result.is_ok());
    }
}
