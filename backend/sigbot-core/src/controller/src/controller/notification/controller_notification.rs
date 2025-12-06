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
    modules::notification::handler::notification_handler::INotificationInfoHandler,
    sys::handler::dlock_handler::IDLockHandler,
};
use sigbot_types::{modules::notification::notification::QueryNotificationRequest, PageRequest, PageResponse};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotNotificationController {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    notification_handler: Option<Arc<dyn INotificationInfoHandler>>,
    dlock_handler: Option<Arc<dyn IDLockHandler>>,
}

impl SigbotNotificationController {
    pub const NAME: &'static str = "NOTIFICATION_CONTROLLER";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(schedule_cron: Option<String>, schedule_channels: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            notification_handler: None, // TODO: Inject the notification handler.
            dlock_handler: None,        // TODO: Inject the dlock handler.
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing notification controller ...");

        // Acquire to distrbuted lock.
        let dlock_name = "NOTIFICATION_CONTROLLER";
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

        info!("Executed notification controller.");
    }

    // TODO: Implement the logic.
    // TODO: 1. Consume the notification from the EMQx messaging topics.
    // TODO: 2. Sending the messing to the notification channel.
    async fn process(&self) {
        info!("Scanning Messaging ...");

        let mut gatekeeper_counter = 0 as u16;
        let mut last_page = PageResponse::new(None, None, None);
        while gatekeeper_counter > Self::DEFAULT_SAFETY_THRESHOLD
            && (last_page.total.is_none() || last_page.total.unwrap_or(0) > 0)
        {
            gatekeeper_counter += 1;
            info!("Loading Notification : {}", last_page.num.unwrap_or(1));

            let (current_page, notifications) = self
                .notification_handler
                .clone()
                .expect("Notification handler is not injected.")
                .find(
                    QueryNotificationRequest {
                        name: None,
                        active: None,
                        provider: None,
                    },
                    PageRequest::new(last_page.num.unwrap_or(1) as u32, last_page.limit.unwrap_or(10) as u32),
                )
                .await
                .expect("Failed to find notification.");
            last_page = current_page;

            info!(
                "Loaded {} Notification : {}",
                notifications.len(),
                last_page.num.unwrap_or(1)
            );

            for notification in notifications {
                info!(
                    "Starting Notification : {:?}/{:?}",
                    notification.base.id, notification.name
                );
                if notification.base.status.unwrap_or(0) == 1 {
                    info!(
                        "Starting Notification Runner : {:?}/{:?}",
                        notification.base.id, notification.name
                    );
                    unimplemented!()
                } else {
                    info!(
                        "Stopping Notification Runner : {:?}/{:?}",
                        notification.base.id, notification.name
                    );
                    unimplemented!()
                }
            }
        }
    }
}

#[async_trait]
impl ISigbotController for SigbotNotificationController {
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

        info!("Starting notification controller with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running notification controller ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create notification controller job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create scheduler");
        scheduler
            .add(job)
            .await
            .expect("Failed to add notification controller job");
        scheduler
            .start()
            .await
            .expect("Failed to start notification controller scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started notification controller with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn close(&self) {
        info!(
            "Closing notification controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown notification controller scheduler");
        }
        info!(
            "Closed notification controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
