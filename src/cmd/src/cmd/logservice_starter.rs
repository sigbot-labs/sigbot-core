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

use crate::cmd::internal::banner::print_banner;
use crate::cmd::internal::management_server::SigbotManagementServer;
use clap::{Arg, Command};
use common_telemetry::info;
use sigbot_core::config::config::get_config;
use sigbot_core::mgmt::apm;
use sigbot_logservice::server::log_server::SigbotLogServer;
use sigbot_types::modules::messager::messager::MessagerProvider;
use sigbot_types::sys::log::LogMgrProvider;
use sigbot_utils::panics::PanicHelper;
use tokio::sync::oneshot;

pub struct SigbotLogServiceStarter {}

impl SigbotLogServiceStarter {
    pub const COMMAND_NAME: &'static str = "logservice";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 __                                                                                     
/\ \                          /'\_/`\                                                   
\ \ \        ___      __     /\      \     __      ___      __       __      __   _ __  
 \ \ \  __  / __`\  /'_ `\   \ \ \__\ \  /'__`\  /' _ `\  /'__`\   /'_ `\  /'__`\/\`'__\
  \ \ \L\ \/\ \L\ \/\ \L\ \   \ \ \_/\ \/\ \L\.\_/\ \/\ \/\ \L\.\_/\ \L\ \/\  __/\ \ \/ 
   \ \____/\ \____/\ \____ \   \ \_\\ \_\ \__/.\_\ \_\ \_\ \__/.\_\ \____ \ \____\\ \_\ 
    \/___/  \/___/  \/___L\ \   \/_/ \/_/\/__/\/_/\/_/\/_/\/__/\/_/\/___L\ \/____/ \/_/ 
                      /\____/                                        /\____/            
                      \_/__/                                         \_/__/             
                                                                 (Sigbot Log Manager)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot Log Service Server.")
            .arg_required_else_help(true) // When no args are provided, show help.
            .arg(
                Arg::new("MESSAGER_PROVIDER")
                    .short('m')
                    .long("messager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(1)
                    .help(format!(
                        "The provider of Messager. (supported are: {}, {})",
                        MessagerProvider::LOCAL.as_str(),
                        MessagerProvider::MQTT.as_str(),
                    ))
                    .default_value(MessagerProvider::LOCAL.as_str()),
            )
            .arg(
                Arg::new("MESSAGER_CONFIGURATION")
                    .short('c')
                    .long("messager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(2)
                    .help("The configuration of Messager. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("LOG_MANAGER_PROVIDER")
                    .long("log-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(3)
                    .help(format!(
                        "The provider of Log Manager. (supported are: {})",
                        LogMgrProvider::DEFAULT.as_str(),
                    ))
                    .default_value(LogMgrProvider::DEFAULT.as_str()),
            )
            .arg(
                Arg::new("LOG_MANAGER_CONFIGURATION")
                    .long("log-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(4)
                    .help("The configuration of Log Manager. (base64 encoded JSON string)"),
            )
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) -> () {
        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        print_banner(verbose, Self::ASCII_NAME, Some("Log Service listen on"));
        let config = get_config();
        eprintln!(
            "            WebSocket endpoint: \"ws://{}:{}/ws/logs\"",
            &config.server.host, config.server.port
        );

        apm::init().await;

        let (signal_s, signal_r) = oneshot::channel();
        let signal_handle = SigbotManagementServer::start(verbose, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        info!("Management server is ready on {}", get_config().mgmt.get_bind_addr());

        Self::start(matches, verbose).await;

        signal_handle.await.expect("Failed to start Management server.");
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotLogServer::startup(matches, verbose).await;
    }
}
