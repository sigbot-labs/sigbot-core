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

use crate::executor::strategy_factory::SigbotStrategyExecutorFactory;
use common_telemetry::info;
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use std::sync::Arc;

pub struct SigbotStrategyRunner {}

impl SigbotStrategyRunner {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Strategy Executor.");
        let (executor, argument) = SigbotStrategyExecutorFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Strategy Executor.");
        info!("Initialized Strategy Executor. {:?}", executor.name());

        info!("Initializing Messaging Client.");
        let messaging = SigbotMessagingClientFactory::init(matches, argument.to_owned().messaging_config.to_owned())
            .await
            .expect("Failed to initialize Messaging Client.");
        info!("Initialized Messaging Client. {:?}", messaging.name());

        info!("Starting Strategy Executor: {}", executor.name());
        executor.startup(messaging.to_owned()).await;
        info!("Started Strategy Executor: {}.", executor.name());
    }

    pub async fn shutdown() {
        info!("Shutting down Strategy Executor.");
        SigbotStrategyExecutorFactory::shutdown().await;
        info!("Shutdown Strategy Executor.");

        info!("Shutting down Messaging Client.");
        SigbotMessagingClientFactory::close().await;
        info!("Shutdown Messaging Client.");
    }
}

#[cfg(test)]
mod tests {}
