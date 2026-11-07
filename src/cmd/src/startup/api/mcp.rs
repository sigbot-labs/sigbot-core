// SPDX-License-Identifier: GPL-3.0-only
use crate::startup::internal::management::SigbotManagementStarter;
use clap::Command;
use common_context::ApplicationContext;
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_mcp::{HttpMcpApi, IMcpApi};
use std::sync::Arc;

#[derive(Default)]
pub struct SigbotMcpStarter {
    management: SigbotManagementStarter,
}
impl SigbotMcpStarter {
    pub const COMMAND_NAME: &'static str = "mcp";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Platformization MCP API for Operations.")
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
        let settings = config.services.platform.api.mcp.clone();
        let context = ApplicationContext::global();
        context
            .register_if_absent::<dyn IMcpApi, _>("mcp-main", move || {
                let api = HttpMcpApi::new(settings.clone(), state.clone());
                async move { Ok(Arc::new(api) as Arc<dyn IMcpApi>) }
            })
            .expect("mcp registration failed");
        context
            .get::<dyn IMcpApi>("mcp-main")
            .await
            .expect("mcp initialization failed");
        common_telemetry::readiness::RuntimeReadiness::business("mcp", true);
    }
}
