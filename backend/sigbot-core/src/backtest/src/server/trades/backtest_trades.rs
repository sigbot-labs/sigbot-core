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

use crate::server::backtest_factory::{BacktestProvider, ISigbotBacktestRunner};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_core::sys::handler::dlock_handler::IDLockHandler;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotTradesBacktestRunner {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    dlock_handler: Option<Arc<dyn IDLockHandler>>,
}

impl SigbotTradesBacktestRunner {
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(schedule_cron: Option<String>, schedule_channels: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            dlock_handler: None, // TODO: Inject the dlock handler.
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing trades based backtest runner ...");

        // Acquire to distrbuted lock.
        let dlock_name = "TRADES_BACKTEST";
        let acquired = self
            .dlock_handler
            .clone()
            .expect("Dlock handler is not injected.")
            .acquire(dlock_name.to_string(), Duration::from_secs(10))
            .await;
        match acquired {
            Ok(true) => {
                self.process().await;
            }
            Ok(false) => {
                info!("Unable to acquire dlock with {}", dlock_name);
            }
            Err(e) => {
                info!("Failed to acquire dlock: {:?}", e.to_string());
            }
        }

        info!("Executed trades based backtest runner process ...");
    }

    pub(super) async fn process(&self) {
        info!("Processing trades based backtest runner ...");
        // TODO: Implement the logic to process trades based backtest.
        // TODO: 1. Start the mock exchange APIs for receiving from strategy runner trade signals (via EMQx pub/sub event-driven).
        // TODO: 2. Start the ticker data extractor for pushing to strategy runner (via EMQx pub/sub event-driven).
        // TODO: 3. Start the summarizer for calculating the loss/profit and updating to balances (via EMQx pub/sub event-driven).
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotBacktestRunner for SigbotTradesBacktestRunner {
    fn provider(&self) -> BacktestProvider {
        BacktestProvider::TRADES
    }

    async fn startup(&self) {
        let this = self.clone();
        let cron_expression = self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION);
        let channel_size = self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS);

        // Validate the cron expression.
        let cron = match Job::new_async(cron_expression, |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => cron_expression,
            Err(e) => {
                tracing::warn!(
                    "Invalid cron expression '{}': {}. Using default '{}'",
                    cron_expression,
                    e,
                    Self::DEFAULT_CRON_EXPRESSION
                );
                Self::DEFAULT_CRON_EXPRESSION
            }
        };

        info!("Starting trades based backtest runner with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running trades based backtest runner ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create trades based backtest runner job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create scheduler");
        scheduler
            .add(job)
            .await
            .expect("Failed to add trades based backtest runner job");
        scheduler
            .start()
            .await
            .expect("Failed to start trades based backtest runner scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started trades based backtest runner with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing trades based backtest runner with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown trades based backtest runner scheduler");
        }
        info!(
            "Closed trades based backtest runner with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
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
