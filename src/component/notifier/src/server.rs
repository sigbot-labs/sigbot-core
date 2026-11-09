// SPDX-License-Identifier: GPL-3.0-only

use crate::notifier::NotifierService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;

pub struct SigbotNotifier;

impl SigbotNotifier {
    pub async fn startup() {
        let argument = sigbot_core::config::config::get_config()
            .services
            .component
            .notifier
            .clone()
            .expect("services.component.notifier required");
        let messager = MessagerServer::connect(
            sigbot_core::config::config::get_config()
                .services
                .component
                .messager
                .clone(),
        )
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
