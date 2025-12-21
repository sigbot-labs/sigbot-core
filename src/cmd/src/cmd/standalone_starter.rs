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

use super::api_starter::SigbotAPIServer;
use crate::cmd::internal::management_server::SigbotManagementServer;
use clap::{Arg, Command};
use common_telemetry::info;
use sigbot_backtest::server::backtest_server::SigbotBacktestServer;
use sigbot_core::config::config::get_config;
use sigbot_core::llm::handler::llm_factory::SigbotLLMFactory;
use sigbot_core::{
    config::config::{GIT_BUILD_DATE, GIT_COMMIT_HASH, GIT_VERSION},
    mgmt::apm,
};
use sigbot_datafeed::server::datafeed_ingestor::SigbotDatafeedIngestor;
use sigbot_notification::server::notification_forwarder::SigbotNotificationForwarder;
use sigbot_order::server::order_server::SigbotOrderServer;
use sigbot_strategy_runner::server::strategy_runner::SigbotStrategyRunner;
use sigbot_types::modules::backtest::BacktestMgrProvider;
use sigbot_types::modules::datafeed::datafeed::DatafeedProvider;
use sigbot_types::modules::messager::messager::MessagerProvider;
use sigbot_types::modules::notification::notification::NotificationProvider;
use sigbot_types::modules::order::OrderMgrProvider;
use sigbot_types::modules::strategy::strategy::StrategyProvider;
use sigbot_types::modules::wallet::WalletMgrProvider;
use sigbot_utils::panics::PanicHelper;
use sigbot_wallet::server::wallet_server::SigbotWalletServer;
use std::env;
use tokio::sync::oneshot;

pub struct SigbotStandaloneStarter {}

impl SigbotStandaloneStarter {
    pub const COMMAND_NAME: &'static str = "standalone";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot Components All in One with Standalone.")
            .arg_required_else_help(true) // When no args are provided, show help.
            .arg(
                Arg::new("MESSAGER_PROVIDER")
                    .long("messager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(1)
                    .help(format!(
                        "The providers of messager. (supported are: {}, {})",
                        MessagerProvider::LOCAL.as_str(),
                        MessagerProvider::MQTT.as_str(),
                    ))
                    .default_value(MessagerProvider::LOCAL.as_str()),
            )
            .arg(
                Arg::new("DATAFEED_PROVIDERS")
                    .long("datafeed-providers")
                    .value_parser(clap::value_parser!(String))
                    .display_order(10)
                    .help(format!(
                        "The providers of multi datafeeds separated by commas. (supported are: {}, {}, {})",
                        DatafeedProvider::BINANCE.as_str(),
                        DatafeedProvider::TWITTER.as_str(),
                        DatafeedProvider::TRUTHSOCIAL.as_str()
                    ))
                    .default_value(DatafeedProvider::BINANCE.as_str()),
            )
            .arg(
                Arg::new("DATAFEED_CONFIGURATION")
                    .long("datafeed-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(11)
                    .help("The configuration of datafeed. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("STRATEGY_RUNNER_PROVIDER")
                    .long("strategy-runner-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(20)
                    .help(format!(
                        "The strategy runner provider to use. (supported are: {})",
                        StrategyProvider::PYTHON.as_str(),
                    ))
                    .default_value(StrategyProvider::PYTHON.as_str()),
            )
            .arg(
                Arg::new("STRATEGY_RUNNER_CONFIGURATION")
                    .long("strategy-runner-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(21)
                    .help("The configuration of strategy. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("ORDER_MANAGER_PROVIDER")
                    .long("order-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(30)
                    .help(format!(
                        "The provider of Order Manager. (supported are: {})",
                        OrderMgrProvider::DEFAULT.as_str(),
                    ))
                    .default_value(OrderMgrProvider::DEFAULT.as_str()),
            )
            .arg(
                Arg::new("ORDER_MANAGER_CONFIGURATION")
                    .long("order-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(31)
                    .help("The configuration of order manager. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("WALLET_MANAGER_PROVIDER")
                    .long("wallet-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(40)
                    .help(format!(
                        "The provider of Wallet Manager. (supported are: {})",
                        WalletMgrProvider::DEFAULT.as_str()
                    ))
                    .default_value(WalletMgrProvider::DEFAULT.as_str()),
            )
            .arg(
                Arg::new("WALLET_MANAGER_CONFIGURATION")
                    .long("wallet-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(41)
                    .help("The configuration of wallet manager. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("BACKTEST_MANAGER_PROVIDER")
                    .long("backtest-manager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(50)
                    .help(format!(
                        "The backtest manager provider to use. (supported are: {}, {})",
                        BacktestMgrProvider::KLINE.as_str(),
                        BacktestMgrProvider::TRADES.as_str(),
                    ))
                    .default_value(BacktestMgrProvider::KLINE.as_str()),
            )
            .arg(
                Arg::new("BACKTEST_MANAGER_CONFIGURATION")
                    .long("backtest-manager-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(51)
                    .help("The configuration of backtest manager. (base64 encoded JSON string)"),
            )
            .arg(
                Arg::new("NOTIFICATION_PROVIDERS")
                    .long("notification-providers")
                    .value_parser(clap::value_parser!(String))
                    .display_order(60)
                    .help(format!(
                        "The providers of multi notification separated by commas. (supported are: {}, {})",
                        NotificationProvider::EMAIL.as_str(),
                        NotificationProvider::TELEGRAM.as_str(),
                    ))
                    .default_value(NotificationProvider::EMAIL.as_str()),
            )
            .arg(
                Arg::new("NOTIFICATION_CONFIGURATION")
                    .long("notification-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(61)
                    .help("The configuration of notification. (base64 encoded JSON string)"),
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

        signal_handle.await.unwrap();
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotDatafeedIngestor::startup(matches, verbose).await;
        SigbotStrategyRunner::startup(matches, verbose).await;
        SigbotOrderServer::startup(matches, verbose).await;
        SigbotWalletServer::startup(matches, verbose).await;
        SigbotNotificationForwarder::startup(matches, verbose).await;
        SigbotBacktestServer::startup(matches, verbose).await;
        SigbotLLMFactory::init().await;
        SigbotAPIServer::startup(matches, verbose, None, None).await;
    }

    fn print_banner(verbose: bool) {
        let config = get_config();

        // http://www.network-science.de/ascii/#larry3d,graffiti,basic,drpepper,rounded,roman
        let ascii_name = r#"
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
