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
    modules::strategy::handler::strategy_handler::IStrategyInfoHandler, sys::handler::dlock_handler::IDLockHandler,
};
use sigbot_types::{modules::strategy::strategy::QueryStrategyRequest, PageRequest, PageResponse};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotStrategyRunnerController {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    strategy_handler: Option<Arc<dyn IStrategyInfoHandler>>,
    dlock_handler: Option<Arc<dyn IDLockHandler>>,
}

impl SigbotStrategyRunnerController {
    pub const NAME: &'static str = "STRATEGY_CONTROLLER";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(schedule_cron: Option<String>, schedule_channels: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            strategy_handler: None, // TODO: Inject the strategy handler.
            dlock_handler: None,    // TODO: Inject the dlock handler.
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing strategy controller ...");

        // Acquire to distrbuted lock.
        let dlock_name = "STRATEGY_RUNNER_CONTROLLER";
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

        info!("Executed strategy controller process ...");
    }

    /// Implement the logical flow to scan to activate or deactivate strategies.
    /// 0. Acquire to distrbuted lock.
    /// 1. Scan the strategies from the database.
    /// 2. If the strategies is activated, then to start the strategy runner instance(pod/container).
    /// 3. If the strategies is not activated, then to shutdown the strategy runner instance(pod/container).
    async fn process(&self) {
        info!("Scanning strategies ...");

        let mut gatekeeper_counter = 0 as u16;
        let mut last_page = PageResponse::new(None, None, None);
        while gatekeeper_counter > Self::DEFAULT_SAFETY_THRESHOLD
            && (last_page.total.is_none() || last_page.total.unwrap_or(0) > 0)
        {
            gatekeeper_counter += 1;
            info!("Loading strategies : {}", last_page.num.unwrap_or(1));

            let (current_page, strategies) = self
                .strategy_handler
                .clone()
                .expect("Strategy handler is not injected.")
                .find(
                    QueryStrategyRequest {
                        name: None,
                        active: None,
                        provider: None,
                    },
                    PageRequest::new(last_page.num.unwrap_or(1) as u32, last_page.limit.unwrap_or(10) as u32),
                )
                .await
                .expect("Failed to find active strategies.");
            last_page = current_page;

            info!(
                "Loaded {} strategies : {}",
                strategies.len(),
                last_page.num.unwrap_or(1)
            );

            for strategy in strategies {
                info!("Starting strategy : {:?}/{:?}", strategy.base.id, strategy.name);
                if strategy.base.status.unwrap_or(0) == 1 {
                    info!("Starting Strategy Runner : {:?}/{:?}", strategy.base.id, strategy.name);
                    unimplemented!()
                } else {
                    info!("Stopping Strategy Runner : {:?}/{:?}", strategy.base.id, strategy.name);
                    unimplemented!()
                }
            }
        }
    }
}

#[async_trait]
impl ISigbotController for SigbotStrategyRunnerController {
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

        info!("Starting Strategy Runner Controller with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Strategy Runner Controller ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create strategy controller job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create scheduler");
        scheduler.add(job).await.expect("Failed to add strategy controller job");
        scheduler
            .start()
            .await
            .expect("Failed to start strategy controller scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Strategy Runner Controller with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing Strategy Runner Controller with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown strategy controller scheduler");
        }
        info!(
            "Closed Strategy controller with cron '{}', channels '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION),
            self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS)
        );
    }
}

#[cfg(test)]
mod tests {}
