// SPDX-License-Identifier: GPL-3.0-only

use crate::controller::{CentralController, IController};
use common_context::ApplicationContext;
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use std::sync::Arc;

pub struct ControllerServer;

impl ControllerServer {
    pub async fn startup() -> Arc<dyn IController> {
        let config = get_config();
        let state = Arc::new(SigbotState::new(&config).await);
        let context = ApplicationContext::global();
        context
            .register_if_absent::<dyn IController, _>("controller", move || {
                let state = state.clone();
                async move {
                    let settings = &state.config.services.platform.controller;
                    let cron = Some(settings.inner.cron.clone());
                    let channels = Some(settings.inner.channel_size);
                    let controller: Arc<dyn IController> = CentralController::new(cron, channels, state);
                    Ok(controller)
                }
            })
            .expect("controller provider registration failed");
        context
            .get::<dyn IController>("controller")
            .await
            .expect("controller provider initialization failed")
    }

    pub async fn shutdown() -> anyhow::Result<()> {
        ApplicationContext::global().close_all::<dyn IController>().await
    }
}
