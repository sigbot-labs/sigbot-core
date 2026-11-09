// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

use async_trait::async_trait;
use common_context::IProvider;
use sigbot_core::context::state::SigbotState;
use std::sync::Arc;

use crate::worker::reconciler::{WorkerReconciler, WorkerScheduler};
use crate::worker::router::WorkerRouter;

#[async_trait]
pub trait IController: IProvider {
    fn name(&self) -> &'static str;
}

pub struct CentralController {
    state: Arc<SigbotState>,
    scheduler: WorkerScheduler,
}

impl CentralController {
    pub fn new(cron: Option<String>, channels: Option<usize>, state: Arc<SigbotState>) -> Arc<Self> {
        Arc::new(Self {
            state,
            scheduler: WorkerScheduler::new(cron, channels),
        })
    }
}

#[async_trait]
impl IController for CentralController {
    fn name(&self) -> &'static str {
        "CENTRAL"
    }
}

#[async_trait]
impl IProvider for CentralController {
    async fn init(&self) -> anyhow::Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("controller", false);
        let router = Arc::new(WorkerRouter::new(self.state.clone()));
        let reconciler = Arc::new(WorkerReconciler::new(self.state.clone(), router)?);
        self.scheduler.start(reconciler).await?;
        common_telemetry::readiness::RuntimeReadiness::business("controller", true);
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("controller", false);
        self.scheduler.stop().await;
        Ok(())
    }
}
