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
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

//! Evaluator Module Configuration
//!
//! ## Architecture
//!
//! The evaluator can call external MCP services (e.g., Binance, Bitget) to fetch
//! real-time market data for dynamic parameter adjustment:
//! - Moving average confirmation
//! - Support/resistance level adjustment
//! - Oscillating vs trending market detection
//! - Hyperparameter switching
//!
//! ## Configuration
//!
//! ```yaml
//! services:
//!   evaluator:
//!     cron: "0/30 * * * * *"  # Every 30 seconds
//!     channel-size: 5
//!     data-window-hours: 4
//!     execution-timeout-secs: 300
//!     mcp-servers:
//!       - name: "binance-mcp"
//!         url: "https://binance-mcp.example.com/sse"
//!         transport: "sse"
//!         api-key: "${BINANCE_API_KEY}"
//!         api-secret: "${BINANCE_API_SECRET}"
//!         enabled-tools:
//!           - "get_kline"
//!           - "get_ticker"
//!           - "get_orderbook"
//!         timeout-secs: 30
//!       - name: "bitget-mcp"
//!         url: "https://bitget-mcp.example.com/sse"
//!         transport: "sse"
//!         api-key: "${BITGET_API_KEY}"
//!         api-secret: "${BITGET_API_SECRET}"
//!         enabled-tools:
//!           - "get_market_data"
//!         timeout-secs: 30
//! ```
//!
//! ## Usage
//!
//! Access configuration via:
//! ```rust
//! use sigbot_core::config::config::get_config;
//! let config = get_config();
//! let mcp_servers = &config.services.evaluator.mcp_servers;
//! ```

#[cfg(test)]
mod tests {}
