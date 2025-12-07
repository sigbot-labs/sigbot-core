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
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotTrushSocialDatafeedClient {
    // config: Arc<DatafeedInfo>,
}

impl SigbotTrushSocialDatafeedClient {
    pub const NAME: &'static str = "TRUSHSOCIAL";

    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub(super) async fn process(&self) {
        info!("Processing Trush Social data feed ...");
        // TODO: Implement the logic to process Trush Social data feed.
        // TODO: 1. Start the Trush Social websocket subscription and pushing to EMQx(hot data cache).
        // TODO: 2. Start the consumer to Trush Social data to database(cold data persist) from EMQx.
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotDatafeedClient for SigbotTrushSocialDatafeedClient {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn init(&self) {
        info!("Started Trush Social Datafeed.");
    }

    async fn close(&self) {
        info!("Shutting down Trush Social Datafeed.");
    }
}

#[cfg(test)]
mod tests {}
