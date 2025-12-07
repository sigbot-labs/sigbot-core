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

use crate::client::datafeed_factory::ISigbotDatafeedClient;
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::datafeed::datafeed::DatafeedInfo;
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotTwitterDatafeedClient {
    config: Arc<DatafeedInfo>,
}

impl SigbotTwitterDatafeedClient {
    pub const KIND: &'static str = "TWITTER_DATAFEED";

    pub async fn new(config: Arc<DatafeedInfo>) -> Arc<Self> {
        Arc::new(Self { config })
    }

    pub(super) async fn process(&self) {
        info!("Processing Twitter data feed ...");
        // TODO: Implement the logic to process Twitter data feed.
        // TODO: 1. Start the Twitter websocket subscription and pushing to EMQx(hot data cache).
        // TODO: 2. Start the consumer to Twitter data to database(cold data persist) from EMQx.
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotDatafeedClient for SigbotTwitterDatafeedClient {
    async fn init(&self) {
        info!("Started Twitter data feed.");
    }

    async fn shutdown(&self) {
        info!("Shut down Twitter data feed.");
    }
}

#[cfg(test)]
mod tests {}
