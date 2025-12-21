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

/// This topic for strategy runner to receive strategies configuration push.
/// data-flow: from api-server to strategy-runner.
pub const TOPIC_CONFIG_STRATEGY: &str = "sigbot/internal/v1/{tenant_id}/config/strategy";

/// This topic for strategy runner to receive market kline/tick data streams.
/// data-flow: from datafeed-ingestor or backtest to strategy-runner.
pub const TOPIC_MARKET_STREAMS: &str = "sigbot/internal/v1/{tenant_id}/market/streams";

/// This topic for order-manager to receive trading signals.
/// data-flow: from strategy-runner to order-manager.
pub const TOPIC_TRADING_SIGNALS: &str = "sigbot/internal/v1/{tenant_id}/trading/signals";

/// This topic for wallet-manager to receive trade results.
/// data-flow: from order-manager to wallet-manager.
pub const TOPIC_TRADING_RESULTS: &str = "sigbot/internal/v1/{tenant_id}/trading/results";

/// This topic for notification forwarder to receive notification messages.
/// data-flow: from any components(api-server, strategy-runner, order-manager, wallet-manager, etc.) to notification-forwarder.
pub const TOPIC_NOTIFICATION_MESSAGES: &str = "sigbot/internal/v1/{tenant_id}/notifications";
