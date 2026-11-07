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

use crate::startup::internal::banner::print_banner;
use crate::startup::internal::management::SigbotManagementServer;
use clap::Arg;
use clap::Command;
use common_context::util::panics::PanicHelper;
use common_context::ApplicationContext;
use sigbot_core::config::config::get_config;
use sigbot_core::mgmt::apm;
use sigbot_datafeed::server::SigbotDatafeedIngestor;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use sigbot_types::messager::messager::MessagerProvider;

#[derive(Default)]
pub struct SigbotDatafeed {
    management: SigbotManagementServer,
}

impl SigbotDatafeed {
    pub const COMMAND_NAME: &'static str = "datafeed";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ____              __             ____                  __     
/\  _`\           /\ \__         /\  _`\               /\ \    
\ \ \/\ \     __  \ \ ,_\    __  \ \ \L\_\ __     __   \_\ \   
 \ \ \ \ \  /'__`\ \ \ \/  /'__`\ \ \  _\/'__`\ /'__`\ /'_` \  
  \ \ \_\ \/\ \L\.\_\ \ \_/\ \L\.\_\ \ \/\  __//\  __//\ \L\ \ 
   \ \____/\ \__/.\_\\ \__\ \__/.\_\\ \_\ \____\ \____\ \___,_\
    \/___/  \/__/\/_/ \/__/\/__/\/_/ \/_/\/____/\/____/\/__,_ /
                                                                         
                                        (Sigbot Datafeed Service)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot Tenantization Datafeed Service for External Ingestion.")
            .arg_required_else_help(true) // When no args are provided, show help.
            .arg(
                Arg::new("MESSAGER_PROVIDER")
                    .short('m')
                    .long("messager-provider")
                    .value_parser(clap::value_parser!(String))
                    .display_order(1)
                    .help(format!(
                        "The provider of Messager. (supported are: {}, {})",
                        MessagerProvider::Local.as_str(),
                        MessagerProvider::Mqtt.as_str(),
                    ))
                    .default_value(MessagerProvider::Local.as_str()),
            )
            .arg(
                Arg::new("DATAFEED_PROVIDERS")
                    .short('p')
                    .long("datafeed-providers")
                    .value_parser(clap::value_parser!(String))
                    .display_order(2)
                    .help(format!(
                        "The providers of multi Datafeeds separated by commas. (supported are: {}, {}, {}, {}, {})",
                        DatafeedProvider::Binance.as_str(),
                        DatafeedProvider::Twitter.as_str(),
                        DatafeedProvider::TruthSocial.as_str(),
                        DatafeedProvider::Fed.as_str(),
                        DatafeedProvider::Treasury.as_str()
                    ))
                    .default_value(DatafeedProvider::Binance.as_str()),
            )
            .arg(
                Arg::new("DATAFEED_CONFIGURATION")
                    .short('c')
                    .long("datafeed-configuration")
                    .value_parser(clap::value_parser!(String))
                    .display_order(3)
                    .help("The configuration of Datafeed. (base64 encoded JSON string)"),
            )
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) {
        let service = Self::default();
        ApplicationContext::global()
            .init()
            .await
            .expect("provider context initialization failed");

        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        print_banner(verbose, Self::ASCII_NAME, Some("DataFeed Server listen on"));

        apm::init().await;

        let signal_handle = service.management.start().await;

        Self::start(matches, verbose).await;

        signal_handle.await.unwrap();
        ApplicationContext::global()
            .shutdown()
            .await
            .expect("provider shutdown failed");
    }

    async fn start(matches: &clap::ArgMatches, verbose: bool) {
        SigbotDatafeedIngestor::startup(matches, verbose).await;
    }
}
