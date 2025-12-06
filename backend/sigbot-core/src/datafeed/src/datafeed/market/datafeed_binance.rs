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

use crate::datafeed::datafeed_factory::ISigbotDatafeedExecutor;
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::datafeed::datafeed::DatafeedInfo;
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotBinanceDatafeedExecutor {
    config: Arc<DatafeedInfo>,
}

impl SigbotBinanceDatafeedExecutor {
    pub const KIND: &'static str = "BINANCE_DATAFEED";

    pub async fn new(config: Arc<DatafeedInfo>) -> Arc<Self> {
        Arc::new(Self { config })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Binance Datafeed ...");
        // TODO: Implement the logic to execute Binance market gateway.
        // TODO: 1. Start the Binance market websocket subscription and pushing to EMQx(hot data cache).
        // TODO: 2. Start the consumer to market data to database(cold data persist) from EMQx.
        info!("Executed Binance Datafeed.");
    }
}

#[async_trait]
impl ISigbotDatafeedExecutor for SigbotBinanceDatafeedExecutor {
    async fn init(&self) {
        info!("Started Binance Datafeed.");
    }

    async fn shutdown(&self) {
        info!("Shut down Binance Datafeed.");
    }
}

#[cfg(test)]
mod tests {}
