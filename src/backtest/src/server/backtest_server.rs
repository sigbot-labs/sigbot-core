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

use crate::manager::backtest_factory::SigbotBacktestManagerFactory;
use common_telemetry::{debug, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use std::sync::{Arc, OnceLock};

static REPLAY_RUNTIME: OnceLock<Arc<super::replay_runtime::ReplayRuntime>> = OnceLock::new();

pub struct SigbotBacktestServer {}

impl SigbotBacktestServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        debug!("Initializing Backtest manager.");
        let (manager, argument) = SigbotBacktestManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Backtest manager.");
        info!("Initialized Backtest manager. {}", manager.provider().as_str());

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        if let Some(properties) = argument
            .properties
            .as_ref()
            .filter(|p| p.get("runtime_v2").is_some_and(|value| value == "true"))
        {
            super::execution_runtime::ExecutionRuntime::start(properties, messager.clone())
                .await
                .expect("failed to start v2 backtest execution runtime");
            if properties.get("replay_v2").is_some_and(|value| value == "true") {
                let runtime = super::replay_runtime::ReplayRuntime::start(properties, messager)
                    .await
                    .expect("failed to start historical replay");
                if REPLAY_RUNTIME.set(runtime).is_err() {
                    panic!("historical replay runtime already started");
                }
            }
            return;
        }
        manager.startup(argument, messager).await;
        info!("Started Backtest manager. {}", manager.provider().as_str());
    }

    pub async fn shutdown() {
        if let Some(runtime) = REPLAY_RUNTIME.get() {
            runtime.close();
        }
        info!("Shutting down Backtest manager.");
        SigbotBacktestManagerFactory::shutdown().await;
        info!("Shutdown Backtest manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::shutdown().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
