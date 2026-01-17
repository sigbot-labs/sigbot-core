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

pub mod messager;

/// This topic for strategy runner to receive workflow's configuration push.
/// phy-data-flow: from api-server to strategy-runner.
pub const TOPIC_CONFIG_WORKFLOW: &str = "/internal/v1/{TENANT_ID}/config/workflow";

/// This topic for strategy runner to receive workflow's market kline/tick data streams.
/// phy-data-flow: from datafeed-ingestor or backtest to strategy-runner.
pub const TOPIC_WF_MARKET_STREAM: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/market/stream";

/// This topic for order-manager to receive workflow's trading signals.
/// phy-data-flow: from strategy-runner to order-manager.
pub const TOPIC_WF_TRADING_SIGNAL: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/trading/signal";

/// This topic for wallet-manager to receive workflow's trade order placed results.
/// phy-data-flow: from order-manager to wallet-manager.
pub const TOPIC_WF_TRADING_PLACED: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/trading/placed";

/// This topic for notification forwarder to receive workflow's notification messages.
/// phy-data-flow: from any components(api-server, strategy-runner, order-manager, wallet-manager, etc.) to notification-forwarder.
pub const TOPIC_WF_NOTIFY_MESSAGE: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/notify";

/// This topic for log service to receive workflow's log messages.
/// phy-data-flow: from any components(api-server, strategy-runner, order-manager, wallet-manager, etc.) to log-service.
pub const TOPIC_WF_LOG: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/{NODE_ID}/log";

/// This topic for strategy runner to receive hyperparameter updates from evaluator.
/// phy-data-flow: from evaluator to strategy-runner.
pub const TOPIC_WF_HYPERPARAMETER_UPDATE: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/hyperparameter/update";

/// This topic for evaluator to receive market data trigger events.
/// phy-data-flow: from datafeed-ingestor or market data rules to evaluator.
pub const TOPIC_EVALUATOR_TRIGGER: &str = "/internal/v1/{TENANT_ID}/evaluator/trigger";
