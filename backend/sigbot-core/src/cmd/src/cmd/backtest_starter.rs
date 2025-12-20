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

use crate::cmd::internal::management_server::SigbotManagementServer;
use clap::{Arg, Command};
use common_telemetry::info;
use sigbot_backtest::server::backtest_factory::{BacktestProvider, SigbotBacktestRunnerFactory};
use sigbot_backtest::server::kline::backtest_kline::SigbotKlineBacktestRunner;
use sigbot_backtest::server::trades::backtest_trades::SigbotTradesBacktestRunner;
use sigbot_core::config::config::get_config;
use sigbot_core::{
    config::config::{GIT_BUILD_DATE, GIT_COMMIT_HASH, GIT_VERSION},
    mgmt::apm,
};
use sigbot_messager::client::messager_mqtt::SigbotMqttMessagerClient;
use sigbot_types::modules::messager::messager::MessagerProvider;
use sigbot_utils::panics::PanicHelper;
use std::env;
use tokio::sync::oneshot;

pub struct SigbotBacktestRunnerStarter {}

impl SigbotBacktestRunnerStarter {
    pub const COMMAND_NAME: &'static str = "backtest";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot tenantization Backtest Runner.")
            .arg_required_else_help(true) // When no args are provided, show help.
            .arg(
                Arg::new("provider")
                    .short('p')
                    .long("provider")
                    .value_parser(clap::value_parser!(String))
                    .help(format!(
                        "The backtest runner provider to use. (supported are: {}, {})",
                        BacktestProvider::KLINE.as_str(),
                        BacktestProvider::TRADES.as_str(),
                    ))
                    .default_value(BacktestProvider::KLINE.as_str()),
            )
            .arg(
                Arg::new("messager")
                    .short('m')
                    .long("messager")
                    .value_parser(clap::value_parser!(String))
                    .help(format!(
                        "The providers of messager. (supported are: {})",
                        MessagerProvider::MQTT.as_str(),
                    ))
                    .default_value(MessagerProvider::MQTT.as_str()),
            )
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) -> () {
        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        Self::print_banner(verbose);

        apm::init().await;

        let (signal_s, signal_r) = oneshot::channel();
        let signal_handle = SigbotManagementServer::start(verbose, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        info!("Management server is ready on {}", get_config().mgmt.get_bind_addr());

        Self::start(matches, verbose).await;

        signal_handle.await.expect("Failed to start Management server.");
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotBacktestRunnerFactory::startup(matches, verbose).await;
    }

    fn print_banner(verbose: bool) {
        let config = get_config();
        // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
        let ascii_name = r#"
 ____                     __          __                   __      
/\  _`\                  /\ \        /\ \__               /\ \__   
\ \ \L\ \     __      ___\ \ \/'\    \ \ ,_\    __    ____\ \ ,_\ 
 \ \  _ <'  /'__`\   /'___\ \ , <     \ \ \/  /'__`\ /',__\\ \ \/  
  \ \ \L\ \/\ \L\.\_/\ \__/\ \ \\`\    \ \ \_/\  __//\__, `\\ \ \_ 
   \ \____/\ \__/.\_\ \____\\ \_\ \_\   \ \__\ \____\/\____/ \ \__\
    \/___/  \/__/\/_/\/____/ \/_/\/_/    \/__/\/____/\/___/   \/__/

                                            (Sigbot Backtest Runner)
 "#;
        eprintln!("");
        eprintln!("{}", ascii_name);
        eprintln!("                Program Version: {:?}", GIT_VERSION);
        eprintln!(
            "                Package Version: {:?}",
            env!("CARGO_PKG_VERSION").to_string()
        );
        eprintln!("                Git Commit Hash: {:?}", GIT_COMMIT_HASH);
        eprintln!("                 Git Build Date: {:?}", GIT_BUILD_DATE);
        let path = env::var("SIGBOT_CFG_PATH").unwrap_or("none".to_string());
        eprintln!("        Configuration file path: {:?}", path);
        eprintln!(
            "            Web Serve listen on: \"{}://{}:{}\"",
            "http", &config.server.host, config.server.port
        );
        if config.mgmt.enabled {
            eprintln!(
                "     Management serve listen on: \"{}://{}:{}\"",
                "http", config.mgmt.host, config.mgmt.port
            );
            if config.mgmt.tokio_console.enabled {
                #[cfg(feature = "profiling-tokio-console")]
                let server_addr = &config.mgmt.tokio_console.server_bind;
                #[cfg(feature = "profiling-tokio-console")]
                eprintln!("   TokioConsole serve listen on: \"{}://{}\"", "http", server_addr);
            }
            if config.mgmt.pyroscope.enabled {
                #[cfg(feature = "profiling-pyroscope")]
                let server_url = &config.mgmt.pyroscope.server_url;
                #[cfg(feature = "profiling-pyroscope")]
                eprintln!("     Pyroscope agent connect to: \"{}\"", server_url);
            }
            if config.mgmt.otel.enabled {
                let endpoint = &config.mgmt.otel.endpoint;
                eprintln!("          Otel agent connect to: \"{}\"", endpoint);
            }
        }
        if verbose {
            let config_json = serde_json::to_string(&config.inner).unwrap_or_default();
            eprintln!("Configuration loaded: {}", config_json);
        }
        eprintln!("");
    }
}
