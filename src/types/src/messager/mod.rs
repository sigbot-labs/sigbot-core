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

pub mod envelope;
pub mod messager;
pub mod topic;

/// This topic for strategy runner to receive workflow's configuration push.
/// phy-data-flow: from api-server to strategy-runner.
pub const TOPIC_CONFIG_WORKFLOW: &str = "/internal/v1/{TENANT_ID}/config/workflow";

/// This topic for strategy runner to receive workflow's market kline/tick data streams.
/// phy-data-flow: from datafeed-ingestor or backtest to strategy-runner.
pub const TOPIC_WF_MARKET_STREAM: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/market/stream";
