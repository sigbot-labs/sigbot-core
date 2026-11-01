// SPDX-License-Identifier: GPL-3.0-only

use crate::strategy::StrategyService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::strategy::StrategyArgument;
use sigbot_types::ArgumentConfig;
use std::sync::Arc;

pub struct SigbotStrategyRunner;
impl SigbotStrategyRunner {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("STRATEGY_RUNNER_CONFIGURATION")
            .expect("strategy configuration required");
        let argument = Arc::new(
            StrategyArgument::from_json(&ArgumentConfig::decode(encoded).expect("invalid configuration encoding"))
                .expect("invalid strategy configuration"),
        );
        argument.properties.as_ref().expect("strategy properties required");
        let key = sigbot_utils::providers::ProviderInstanceId::configured("strategy", &*argument)
            .expect("invalid service identity");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
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
