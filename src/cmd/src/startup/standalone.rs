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

use crate::startup::api::http::SigbotHttpStarter;
use crate::startup::internal::banner::print_banner;
use crate::startup::internal::management::SigbotManagementStarter;
use clap::Command;
use sigbot_backtest::server::SigbotBacktestServer;
use sigbot_core::config::config::get_config;
use sigbot_core::llm::llm::LlmApplication;
use sigbot_datafeed::server::SigbotDatafeedIngestor;
use sigbot_notifier::server::SigbotNotifier;
use sigbot_order::server::SigbotOrderServer;
use sigbot_strategy_runner::server::SigbotStrategyRunner;
use sigbot_wallet::server::SigbotWalletServer;

#[derive(Default)]
pub struct SigbotStandaloneStarter {
    management: SigbotManagementStarter,
}

impl SigbotStandaloneStarter {
    pub const COMMAND_NAME: &'static str = "standalone";

    // http://www.network-science.de/ascii/#larry3d,graffiti,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ____                __              __      
/\  _`\   __        /\ \            /\ \__   
\ \,\L\_\/\_\     __\ \ \____    ___\ \ ,_\  
 \/_\__ \\/\ \  /'_ `\ \ '__`\  / __`\ \ \/  
   /\ \L\ \ \ \/\ \L\ \ \ \L\ \/\ \L\ \ \ \_ 
   \ `\____\ \_\ \____ \ \_,__/\ \____/\ \__\
    \/_____/\/_/\/___L\ \/___/  \/___/  \/__/
                  /\____/                    
                  \_/__/      (Sigbot Standalone (All-in-One))
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot All-in-One Standalone service")
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) {
        let service = Self::default();
        let signal_handle = service.management.start().await;
        print_banner(verbose, Self::ASCII_NAME, None);

        Self::start(matches, verbose).await;

        service
            .management
            .wait(signal_handle)
            .await
            .expect("service lifecycle failed");
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotDatafeedIngestor::startup().await;
        SigbotStrategyRunner::startup().await;
        SigbotOrderServer::startup().await;
        SigbotWalletServer::startup().await;
        SigbotNotifier::startup().await;
        SigbotBacktestServer::startup().await;
        LlmApplication::register_default()
            .await
            .expect("default LLM provider initialization failed");
        if get_config().services.platform.api.a2a.enabled {
            crate::startup::api::a2a::SigbotA2aStarter::startup().await;
        }
        SigbotHttpStarter::startup(matches, verbose, None, None).await;
    }
}
