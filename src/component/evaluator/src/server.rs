// SPDX-License-Identifier: GPL-3.0-only

use crate::evaluator::IEvaluator;
use crate::llm::llm::LlmEvaluator;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::evaluator::EvaluatorArgument;
use sigbot_types::ArgumentConfig;
use std::sync::Arc;

pub struct SigbotEvaluatorRunner;
impl SigbotEvaluatorRunner {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("EVALUATOR_RUNNER_CONFIGURATION")
            .expect("evaluator configuration required");
        let argument = Arc::new(
            EvaluatorArgument::from_json(&ArgumentConfig::decode(encoded).expect("invalid configuration encoding"))
                .expect("invalid evaluator configuration"),
        );
        let properties = argument
            .properties
            .as_ref()
            .expect("evaluator properties required")
            .clone();
        let key = sigbot_utils::providers::ProviderInstanceId::configured("evaluator", &*argument)
            .expect("invalid service identity");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("evaluator messager failed");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<LlmEvaluator, _>(&key, 20, move || {
                let properties = properties.clone();
                let argument = argument.clone();
                let messager = messager.clone();
                async move {
                    let instance = LlmEvaluator::from_properties(&properties).await?;
                    if let Err(error) = instance.clone().startup(argument, messager.clone()).await {
                        let _ = instance.close().await;
                        return Err(error);
                    }
                    Ok(instance)
                }
            })
            .expect("evaluator registration failed");
        context
            .register_with::<dyn IEvaluator, LlmEvaluator>(&key, |instance| Ok(instance as Arc<dyn IEvaluator>))
            .expect("evaluator interface registration failed");
        context
            .get::<dyn IEvaluator>(&key)
            .await
            .expect("evaluator startup failed");
    }
    pub async fn shutdown() {
        ApplicationContext::global()
            .close_all::<LlmEvaluator>()
            .await
            .expect("evaluator close failed");
    }
}
