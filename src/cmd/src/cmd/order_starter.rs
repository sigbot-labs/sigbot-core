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
use sigbot_order::server::order_server::SigbotOrderServer;
use sigbot_types::modules::messager::messager::MessagerProvider;
use sigbot_types::modules::order::OrderMgrProvider;
use sigbot_utils::panics::PanicHelper;
use tokio::sync::oneshot;

pub struct SigbotOrderManagerStarter {}

impl SigbotOrderManagerStarter {
    pub const COMMAND_NAME: &'static str = "order";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 _____            __                                              
/\  __`\         /\ \                     /'\_/`\                 
\ \ \/\ \  _ __  \_\ \     __   _ __     /\      \     __   _ __  
 \ \ \ \ \/\`'__\/'_` \  /'__`\/\`'__\   \ \ \__\ \  /'_ `\/\`'__\
  \ \ \_\ \ \ \//\ \L\ \/\  __/\ \ \/     \ \ \_/\ \/\ \L\ \ \ \/ 
   \ \_____\ \_\\ \___,_\ \____\\ \_\      \ \_\\ \_\ \____ \ \_\ 
    \/_____/\/_/ \/__,_ /\/____/ \/_/       \/_/ \/_/\/___L\ \/_/ 
                                                       /\____/    
                                                       \_/__/     
                                             (Sigbot Order Manager)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot tenantization Order Manager")
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
                Arg::new("ORDER_MANAGER_PROVIDER")
                    .short('p')
                    .long("order-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    // .value_parser(OrderMgrProvider::of)
                    .display_order(2)
                    .help(format!(
                        "The provider of Order Manager. (supported are: {})",
                        OrderMgrProvider::DEFAULT.as_str(),
                    ))
                    .default_value(OrderMgrProvider::DEFAULT.as_str()),
            )
            .arg(
                Arg::new("ORDER_MANAGER_CONFIGURATION")
                    .short('c')
                    .long("order-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(3)
                    .help("The configuration of Order Manager. (base64 encoded JSON string)"),
            )
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) -> () {
        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        print_banner(verbose, Self::ASCII_NAME, None);

        apm::init().await;

        let (signal_s, signal_r) = oneshot::channel();
        let signal_handle = SigbotManagementServer::start(verbose, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        info!("Management server is ready on {}", get_config().mgmt.get_bind_addr());

        Self::start(matches, verbose).await;

        signal_handle.await.expect("Failed to start Management server.");
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotOrderServer::startup(matches, verbose).await;
    }
}
