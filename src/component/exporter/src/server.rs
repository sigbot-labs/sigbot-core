// SPDX-License-Identifier: GPL-3.0-only

use crate::exporter::ExportService;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::exporter::ExporterArgument;
use sigbot_types::ArgumentConfig;

pub struct SigbotExporterServer;

impl SigbotExporterServer {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("EXPORTER_MANAGER_CONFIGURATION")
            .expect("exporter host configuration required");
        let argument = ExporterArgument::from_json(
            &ArgumentConfig::decode(encoded).expect("invalid exporter configuration encoding"),
        )
        .expect("invalid exporter configuration");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
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
