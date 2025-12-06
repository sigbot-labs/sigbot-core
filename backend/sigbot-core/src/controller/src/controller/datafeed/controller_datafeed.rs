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
use sigbot_core::{
    modules::datafeed::handler::datafeed_handler::IDatafeedInfoHandler, sys::handler::dlock_handler::IDLockHandler,
};
use sigbot_datafeed::datafeed::datafeed_factory::SigbotDatafeedFactory;
use sigbot_types::{modules::datafeed::datafeed::QueryDatafeedRequest, PageRequest, PageResponse};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotDatafeedRunnerController {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    datafeed_handler: Option<Arc<dyn IDatafeedInfoHandler>>,
    dlock_handler: Option<Arc<dyn IDLockHandler>>,
}

impl SigbotDatafeedRunnerController {
    pub const NAME: &'static str = "DATAFEED_CONTROLLER";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(schedule_cron: Option<String>, schedule_channels: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            datafeed_handler: None, // TODO: Inject the datafeed handler.
            dlock_handler: None,    // TODO: Inject the dlock handler.
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Datafeed controller ...");

        // Acquire to distrbuted lock.
        let dlock_name = "DATAFEED_RUNNER_CONTROLLER";
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
                info!("Unable to acquire distributed lock with {}", dlock_name);
            }
            Err(e) => {
                info!("Failed to acquire distributed lock: {:?}", e.to_string());
            }
        }

        info!("Executed Datafeed controller.");
    }

    // TODO: Implement the logic to scan to activate datafeed sources.
    // TODO: 1. Scan the datafeed source from the database.
    // TODO: 2. If the datafeed source is activated, then to start the datafeed runner instance(pod/container).
    // TODO: 3. If the datafeed source is not activated, then to shutdown the datafeed runner instance(pod/container).
    async fn process(&self) {
        info!("Scanning Datafeed ...");

        let mut gatekeeper_counter = 0 as u16;
        let mut last_page = PageResponse::new(None, None, None);
        while gatekeeper_counter > Self::DEFAULT_SAFETY_THRESHOLD
            && (last_page.total.is_none() || last_page.total.unwrap_or(0) > 0)
        {
            gatekeeper_counter += 1;
            info!("Loading Datafeed : {}", last_page.num.unwrap_or(1));

            let (current_page, datafeeds) = self
                .datafeed_handler
                .clone()
                .expect("Datafeed handler is not injected.")
                .find(
                    QueryDatafeedRequest {
                        name: None,
                        active: None,
                        provider: None,
                    },
                    PageRequest::new(last_page.num.unwrap_or(1) as u32, last_page.limit.unwrap_or(10) as u32),
                )
                .await
                .expect("Failed to find Datafeed.");
            last_page = current_page;

            info!("Loaded {} Datafeed : {}", datafeeds.len(), last_page.num.unwrap_or(1));

            for datafeed in datafeeds {
                let datafeed0 = Arc::new(datafeed);
                if datafeed0.base.status.unwrap_or(0) == 1 {
                    info!(
                        "Initializing Datafeed Runner : {:?}/{:?}",
                        datafeed0.base.id, datafeed0.name
                    );
                    let result = SigbotDatafeedFactory::register(datafeed0.to_owned()).await;
                    match result {
                        Ok(handler) => {
                            handler.init().await;
                            info!(
                                "Initialized Datafeed Runner : {:?}/{:?}",
                                datafeed0.base.id, datafeed0.name
                            );
                        }
                        Err(e) => {
                            info!(
                                "Failed to initialize Datafeed Runner : {:?}/{:?}",
                                datafeed0.base.id, datafeed0.name
                            );
                        }
                    }
                } else {
                    info!(
                        "Shutting down Datafeed Runner : {:?}/{:?}",
                        datafeed0.base.id, datafeed0.name
                    );
                    let result =
                        SigbotDatafeedFactory::get_implementation(datafeed0.name.to_owned().unwrap_or_default()).await;
                    match result {
                        Ok(handler) => {
                            handler.shutdown().await;
                            info!(
                                "Shutdown Datafeed Runner : {:?}/{:?}",
                                datafeed0.base.id, datafeed0.name
                            );
                        }
                        Err(e) => {
                            info!(
                                "Failed to shutdown Datafeed Runner : {:?}/{:?}",
                                datafeed0.base.id, datafeed0.name
                            );
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ISigbotController for SigbotDatafeedRunnerController {
    async fn init(&self) {
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

        info!("Starting Datafeed controller with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Datafeed controller ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create Datafeed controller job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create Datafeed controller scheduler");
        scheduler.add(job).await.expect("Failed to add Datafeed controller job");
        scheduler
            .start()
            .await
            .expect("Failed to start Datafeed controller scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Datafeed controller with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn close(&self) {
        info!(
            "Closing Datafeed controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown datafeed controller scheduler");
        }
        info!(
            "Closed Datafeed controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
