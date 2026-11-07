// SPDX-License-Identifier: GPL-3.0-only

use crate::exporter::ExportService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;

pub struct SigbotExporterServer;

impl SigbotExporterServer {
    pub async fn startup() {
        let argument = sigbot_core::config::config::get_config()
            .services
            .component
            .exporter
            .clone()
            .expect("services.component.exporter required");
        let messager = MessagerServer::connect(
            sigbot_core::config::config::get_config()
                .services
                .component
                .messager
                .clone(),
        )
        .await
        .expect("exporter messager initialization failed");
        let properties = argument.properties.expect("exporter runtime properties required");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<ExportService, _>("exporter", 30, move || {
                let properties = properties.clone();
                let messager = messager.clone();
                async move {
                    let instance = ExportService::from_properties(&properties).await?;
                    instance.start(messager).await?;
                    Ok(instance)
                }
            })
            .expect("exporter registration failed");
        context
            .get::<ExportService>("exporter")
            .await
            .expect("exporter initialization failed");
    }
}
