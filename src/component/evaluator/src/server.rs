// SPDX-License-Identifier: GPL-3.0-only

use crate::evaluator::IEvaluator;
use crate::llm::llm::LlmEvaluator;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use std::sync::Arc;

pub struct SigbotEvaluatorRunner;
impl SigbotEvaluatorRunner {
    pub async fn startup() {
        let argument = sigbot_core::config::config::get_config()
            .services
            .component
            .evaluator
            .clone()
            .expect("services.component.evaluator required");
        let argument = Arc::new(argument);
        let properties = argument
            .properties
            .as_ref()
            .expect("evaluator properties required")
            .clone();
        let key = sigbot_utils::providers::ProviderInstanceId::configured("evaluator", &*argument)
            .expect("invalid service identity");
        let messager = MessagerServer::connect(
            sigbot_core::config::config::get_config()
                .services
                .component
                .messager
                .clone(),
        )
        .await
        .expect("evaluator messager failed");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<LlmEvaluator, _>(&key, 20, move || {
                let properties = properties.clone();
                let messager = messager.clone();
                async move {
                    let instance = LlmEvaluator::from_properties(&properties).await?;
                    if let Err(error) = instance.clone().startup(messager.clone()).await {
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
