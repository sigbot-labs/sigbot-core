// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::manager::exporter_factory::SigbotExporterManagerFactory;
use common_telemetry::{debug, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use std::sync::Arc;

pub struct SigbotExporterServer {}

impl SigbotExporterServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        if let Some(encoded) = matches.get_one::<String>("EXPORTER_MANAGER_CONFIGURATION") {
            let decoded =
                sigbot_types::modules::decode_arg_config(encoded).expect("invalid exporter configuration encoding");
            let argument = sigbot_types::modules::exporter::SigbotExporterManagerArgument::from_json(&decoded)
                .expect("invalid exporter configuration");
            if let Some(properties) = argument
                .properties
                .as_ref()
                .filter(|p| p.get("runtime_v2").is_some_and(|value| value == "true"))
            {
                let runtime = super::export_runtime::ExportRuntime::from_properties(properties)
                    .await
                    .expect("invalid v2 exporter configuration");
                let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config)
                    .await
                    .expect("exporter broker connection failed");
                runtime.start(messager).await.expect("exporter subscription failed");
                return;
            }
        }
        debug!("Initializing Exporter manager.");
        let (manager, argument) = SigbotExporterManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Exporter manager.");
        info!("Initialized Exporter manager. {}", manager.provider().as_str());

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        // Each exporter manager handles its own subscription to appropriate topics
        manager
            .subscribe(messager)
            .await
            .expect("Failed to subscribe exporter manager to messager topics");

        info!("Exporter manager subscribed to messager topics.");
    }

    pub async fn shutdown() {
        info!("Shutting down Exporter manager.");
        SigbotExporterManagerFactory::shutdown().await;
        info!("Shutdown Exporter manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::shutdown().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
