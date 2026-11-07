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
use crate::startup::internal::management::SigbotManagementStarter;
use clap::Command;
use sigbot_backtest::server::SigbotBacktestServer;

#[derive(Default)]
pub struct SigbotBacktestStarter {
    management: SigbotManagementStarter,
}

impl SigbotBacktestStarter {
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

                                            (Sigbot Backtest Service)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Tenantization Backtest Service for Strategies Performance.")
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

    async fn start(_matches: &clap::ArgMatches, _verbose: bool) {
        SigbotBacktestServer::startup().await;
    }
}
