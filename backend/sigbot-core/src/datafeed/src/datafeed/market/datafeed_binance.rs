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
use sigbot_core::config::config::ExecutorProperties;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotBinanceDatafeedExecutor {
    config: ExecutorProperties,
    scheduler: Arc<JobScheduler>,
}

impl SigbotBinanceDatafeedExecutor {
    pub const KIND: &'static str = "BINANCE_DATAFEED";

    pub async fn new(config: &ExecutorProperties) -> Arc<Self> {
        Arc::new(Self {
            config: config.to_owned(),
            scheduler: Arc::new(JobScheduler::new_with_channel_size(config.channel_size).await.unwrap()),
        })
    }

    pub(super) async fn process(&self) {
        info!("Processing Binance market gateway ...");
        // TODO: Implement the logic to process Binance market gateway.
        // TODO: 1. Start the Binance market websocket subscription and pushing to EMQx(hot data cache).
        // TODO: 2. Start the consumer to market data to database(cold data persist) from EMQx.
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotDatafeedExecutor for SigbotBinanceDatafeedExecutor {
    async fn init(&self) {
        let this = self.clone();

        // Pre-check the cron expression is valid.
        let cron = match Job::new_async(self.config.cron.as_str(), |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => self.config.cron.as_str(),
            Err(e) => {
                tracing::warn!(
                    "Invalid cron expression '{}': {}. Using default '0/30 * * * * *'",
                    self.config.cron,
                    e
                );
                "0/30 * * * * *" // every half minute
            }
        };

        info!("Starting Binance market gateway with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Binance market gateway ...", chrono::Utc::now());
                that.process().await;
            })
        })
        .unwrap();

        self.scheduler.add(job).await.unwrap();
        self.scheduler.start().await.unwrap();

        info!("Started Binance market gateway.");
    }
}

#[cfg(test)]
mod tests {}
