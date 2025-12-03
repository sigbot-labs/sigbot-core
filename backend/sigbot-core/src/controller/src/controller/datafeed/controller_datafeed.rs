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

use crate::controller::controller_factory::ISigbotController;
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_core::config::config::ExecutorProperties;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotDatafeedRunnerController {
    config: ExecutorProperties,
    scheduler: Arc<JobScheduler>,
}

impl SigbotDatafeedRunnerController {
    pub const KIND: &'static str = "DATAFEED_RUNNER";

    pub async fn new(config: &ExecutorProperties) -> Arc<Self> {
        Arc::new(Self {
            config: config.to_owned(),
            scheduler: Arc::new(JobScheduler::new_with_channel_size(config.channel_size).await.unwrap()),
        })
    }

    pub(super) async fn process(&self) {
        info!("Scanning to activate datafeed ...");
        // TODO: Implement the logic to scan to activate datafeed sources.
        // TODO: 1. Scan the datafeed source from the database.
        // TODO: 2. If the datafeed source is activated, then to start the datafeed runner instance(pod/container).
        // TODO: 3. If the datafeed source is not activated, then to shutdown the datafeed runner instance(pod/container).
        unimplemented!()
    }
}

#[async_trait]
impl ISigbotController for SigbotDatafeedRunnerController {
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

        info!("Starting Datafeed Runner Controller with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Datafeed Runner Controller ...", chrono::Utc::now());
                that.process().await;
            })
        })
        .unwrap();

        self.scheduler.add(job).await.unwrap();
        self.scheduler.start().await.unwrap();

        info!("Started Datafeed Runner Controller.");
    }

    async fn close(&self) {
        info!(
            "Closing Datafeed Runner Controller with cron '{}'",
            self.config.cron.as_str()
        );
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {}
