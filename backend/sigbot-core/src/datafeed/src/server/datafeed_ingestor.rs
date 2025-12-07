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

use common_telemetry::info;
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use std::sync::Arc;

use crate::client::datafeed_factory::SigbotDatafeedClientFactory;

pub struct SigbotDatafeedIngestorServer {
    // TODO: binance client.
    // TODO: twitter client.
    // TODO: trush social client.
}

impl SigbotDatafeedIngestorServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Datafeed clients.");
        SigbotDatafeedClientFactory::init(matches, verbose).await;
        info!("Initialized Datafeed clients.");

        info!("Initializing Messaging client.");
        SigbotMessagingClientFactory::init(matches, verbose).await;
        info!("Initialized Messaging client.");
    }

    pub async fn shutdown() {
        info!("Shutting down Datafeed clients.");
        SigbotDatafeedClientFactory::close().await;
        info!("Shutting down Datafeed clients.");

        info!("Shutting down Messaging client.");
        SigbotMessagingClientFactory::close().await;
        info!("Shutting down Messaging client.");
    }
}

#[cfg(test)]
mod tests {}
