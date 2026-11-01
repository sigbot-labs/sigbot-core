// SPDX-License-Identifier: GPL-3.0-only

use crate::notifier::NotifierService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::notifier::NotifierArgument;
use sigbot_types::ArgumentConfig;

pub struct SigbotNotifier;

impl SigbotNotifier {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("NOTIFIER_CONFIGURATION")
            .expect("notifier host configuration required");
        let argument = NotifierArgument::from_json(
            &ArgumentConfig::decode(encoded).expect("invalid notifier configuration encoding"),
        )
        .expect("invalid notifier configuration");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("notifier messager initialization failed");
        let properties = argument.properties.expect("notifier runtime properties required");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<NotifierService, _>("notifier", 30, move || {
                let properties = properties.clone();
                let messager = messager.clone();
                async move {
                    let instance = NotifierService::from_properties(&properties).await?;
                    instance.start(messager).await?;
                    Ok(instance)
                }
            })
            .expect("notifier registration failed");
        context
            .get::<NotifierService>("notifier")
            .await
            .expect("notifier initialization failed");
    }
}
