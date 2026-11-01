// SPDX-License-Identifier: GPL-3.0-only

use crate::processing::workflow_order::WorkflowOrderProcessor;
use anyhow::Context;
use common_context::ApplicationContext;
use sigbot_core::config::config::get_config;
use sigbot_messager::server::MessagerServer;
use sigbot_types::order::OrderArgument;
use sigbot_types::ArgumentConfig;

pub struct SigbotOrderServer;

impl SigbotOrderServer {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("ORDER_MANAGER_CONFIGURATION")
            .expect("order host configuration required");
        let argument =
            OrderArgument::from_json(&ArgumentConfig::decode(encoded).expect("invalid order configuration encoding"))
                .expect("invalid order configuration");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("order host messager initialization failed");
        let config = get_config();
        config
            .authguard
            .service_principal_id
            .as_ref()
            .filter(|principal| !principal.is_empty())
            .expect("order host service principal required");
        let properties = argument
            .properties
            .context("order runtime properties required")
            .unwrap();
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<WorkflowOrderProcessor, _>("order", 30, move || {
                let properties = properties.clone();
                let messager = messager.clone();
                async move {
                    let processor = WorkflowOrderProcessor::from_properties(&properties).await?;
                    processor.start(messager).await?;
                    Ok(processor)
                }
            })
            .expect("order registration failed");
        context
            .get::<WorkflowOrderProcessor>("order")
            .await
            .expect("order initialization failed");
    }
}
