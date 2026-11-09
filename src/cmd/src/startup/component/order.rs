// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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
use sigbot_order::server::SigbotOrderServer;

#[derive(Default)]
pub struct SigbotOrderStarter {
    management: SigbotManagementStarter,
}

impl SigbotOrderStarter {
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
                                             (Sigbot Order Service)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Tenantization Order Service for Position Execution.")
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
        SigbotOrderServer::startup().await;
    }
}
