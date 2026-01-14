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
use sigbot_backtest::server::backtest_server::SigbotBacktestServer;
use sigbot_core::config::config::get_config;
use sigbot_core::mgmt::apm;
use sigbot_types::modules::backtest::BacktestMgrProvider;
use sigbot_types::modules::messager::messager::MessagerProvider;
use sigbot_utils::panics::PanicHelper;
use tokio::sync::oneshot;

pub struct SigbotBacktestRunnerStarter {}

impl SigbotBacktestRunnerStarter {
    pub const COMMAND_NAME: &'static str = "backtest";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ____                     __          __                   __      
/\  _`\                  /\ \        /\ \__               /\ \__   
\ \ \L\ \     __      ___\ \ \/'\    \ \ ,_\    __    ____\ \ ,_\ 
 \ \  _ <'  /'__`\   /'___\ \ , <     \ \ \/  /'__`\ /',__\\ \ \/  
  \ \ \L\ \/\ \L\.\_/\ \__/\ \ \\`\    \ \ \_/\  __//\__, `\\ \ \_ 
   \ \____/\ \__/.\_\ \____\\ \_\ \_\   \ \__\ \____\/\____/ \ \__\
    \/___/  \/__/\/_/\/____/ \/_/\/_/    \/__/\/____/\/___/   \/__/

                                            (Sigbot Backtest Runner)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot tenantization Backtest Runner.")
            .arg_required_else_help(true) // When no args are provided, show help.
            .arg(
                Arg::new("MESSAGER_PROVIDER")
                    .short('m')
                    .long("messager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(1)
                    .help(format!(
                        "The providers of Messager. (supported are: {})",
                        MessagerProvider::MQTT.as_str(),
                    ))
                    .default_value(MessagerProvider::MQTT.as_str()),
            )
            .arg(
                Arg::new("BACKTEST_MANAGER_PROVIDER")
                    .short('p')
                    .long("backtest-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(2)
                    .help(format!(
                        "The Backtest Manager provider to use. (supported are: {}, {})",
                        BacktestMgrProvider::KLINE.as_str(),
                        BacktestMgrProvider::TRADES.as_str(),
                    ))
                    .default_value(BacktestMgrProvider::KLINE.as_str()),
            )
            .arg(
                Arg::new("BACKTEST_MANAGER_CONFIGURATION")
                    .short('c')
                    .long("backtest-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(3)
                    .help("The configuration of Backtest Manager. (base64 encoded JSON string)"),
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
        SigbotBacktestServer::startup(matches, verbose).await;
    }
}
