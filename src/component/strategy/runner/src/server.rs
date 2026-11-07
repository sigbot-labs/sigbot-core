// SPDX-License-Identifier: GPL-3.0-only

use crate::strategy::StrategyService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use std::sync::Arc;

pub struct SigbotStrategyRunner;
impl SigbotStrategyRunner {
    pub async fn startup() {
        let argument = sigbot_core::config::config::get_config()
            .services
            .component
            .strategy
            .clone()
            .expect("services.component.strategy required");
        let argument = Arc::new(argument);
        argument.properties.as_ref().expect("strategy properties required");
        let key = sigbot_utils::providers::ProviderInstanceId::configured("strategy", &*argument)
            .expect("invalid service identity");
        let messager = MessagerServer::connect(
            sigbot_core::config::config::get_config()
                .services
                .component
                .messager
                .clone(),
        )
        .await
        .expect("strategy messager failed");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<StrategyService, _>(&key, 20, move || {
                let argument = argument.clone();
                let messager = messager.clone();
                async move { StrategyService::start(&argument, messager.clone()).await }
            })
            .expect("strategy registration failed");
        context
            .get::<StrategyService>(&key)
            .await
            .expect("strategy startup failed");
    }
    pub async fn shutdown() {
        ApplicationContext::global()
            .close_all::<StrategyService>()
            .await
            .expect("strategy close failed");
    }
}
