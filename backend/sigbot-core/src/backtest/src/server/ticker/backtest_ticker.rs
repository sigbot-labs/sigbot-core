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

use crate::server::backtest_factory::ISigbotBacktestRunner;
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_core::config::config::BacktestProperties;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotTickerBacktestRunner {
    config: BacktestProperties,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
}

impl SigbotTickerBacktestRunner {
    pub const NAME: &'static str = "TICKER_BASED";

    pub async fn new(config: &BacktestProperties) -> Arc<Self> {
        Arc::new(Self {
            config: config.to_owned(),
            scheduler: Arc::new(Mutex::new(None)),
        })
    }

    pub(super) async fn process(&self) {
        info!("Processing ticker based backtest ...");
        // TODO: Implement the logic to process ticker based backtest.
        // TODO: 1. Start the mock exchange APIs for receiving from strategy runner trade signals (via EMQx pub/sub event-driven).
        // TODO: 2. Start the ticker data extractor for pushing to strategy runner (via EMQx pub/sub event-driven).
        // TODO: 3. Start the summarizer for calculating the loss/profit and updating to balances (via EMQx pub/sub event-driven).
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotBacktestRunner for SigbotTickerBacktestRunner {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn startup(&self) {
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

        info!("Starting Ticker based backtest handler with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Hi I ran", chrono::Utc::now());
                that.process().await;
            })
        })
        .unwrap();

        let scheduler = JobScheduler::new_with_channel_size(self.config.channel_size)
            .await
            .unwrap();
        scheduler.add(job).await.unwrap();
        scheduler.start().await.unwrap();
        *self.scheduler.lock().await = Some(scheduler);

        info!("Started Ticker based backtest handler.");
    }

    async fn shutdown(&self) {
        info!("Shutting down Ticker based backtest handler.");
        let mut guard = self.scheduler.lock().await;
        if let Some(scheduler) = guard.as_mut() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown Ticker based backtest handler.");
        }
        info!("Ticker based backtest handler shutdown gracefully.");
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused)]
    use super::*;
    use sigbot_core::config::config::AppConfigProperties;

    #[tokio::test]
    async fn test_verify() {
        #[allow(unused)]
        let mut config = AppConfigProperties::default();
        todo!()
    }
}
