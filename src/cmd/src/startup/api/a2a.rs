// SPDX-License-Identifier: GPL-3.0-only
use crate::startup::internal::management::SigbotManagementStarter;
use clap::Command;
use common_context::ApplicationContext;
use sigbot_a2a::{IA2aApi, ResourceA2aApi};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use std::sync::Arc;

#[derive(Default)]
pub struct SigbotA2aStarter {
    management: SigbotManagementStarter,
}
impl SigbotA2aStarter {
    pub const COMMAND_NAME: &'static str = "a2a";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Platformization A2A API for Operations.")
    }

    #[tokio::main]
    pub async fn run(_matches: &clap::ArgMatches, _verbose: bool) {
        let service = Self::default();
        let management = service.management.start().await;
        Self::startup().await;
        service
            .management
            .wait(management)
            .await
            .expect("service lifecycle failed");
    }

    pub async fn startup() {
        let config = get_config();
        let state = SigbotState::new(&config).await;
        let settings = config.services.platform.api.a2a.clone();
        let context = ApplicationContext::global();
        context
            .register_if_absent::<dyn IA2aApi, _>("a2a-main", move || {
                let api = ResourceA2aApi::new(settings.clone(), state.clone());
                async move { Ok(Arc::new(api) as Arc<dyn IA2aApi>) }
            })
            .expect("a2a registration failed");
        context
            .get::<dyn IA2aApi>("a2a-main")
            .await
            .expect("a2a initialization failed");
        common_telemetry::readiness::RuntimeReadiness::business("a2a", true);
    }
}
