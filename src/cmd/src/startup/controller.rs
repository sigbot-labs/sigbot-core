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
use clap::Command;
use common_context::util::panics::PanicHelper;
use common_context::ApplicationContext;
use sigbot_controller::server::ControllerServer;
use sigbot_core::config::config::get_config;
use sigbot_core::mgmt::apm;

#[derive(Default)]
pub struct SigbotController {
    management: SigbotManagementServer,
}

impl SigbotController {
    pub const COMMAND_NAME: &'static str = "controller";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ____                    ___                                   
/\  _`\                 /\_ \                                  
\ \ \/\ \     __   _____\//\ \     ___   __  __     __   _ __  
 \ \ \ \ \  /'__`\/\ '__`\\ \ \   / __`\/\ \/\ \  /'__`\/\`'__\
  \ \ \_\ \/\  __/\ \ \L\ \\_\ \_/\ \L\ \ \ \_\ \/\  __/\ \ \/ 
   \ \____/\ \____\\ \ ,__//\____\ \____/\/`____ \ \____\\ \_\ 
    \/___/  \/____/ \ \ \/ \/____/\/___/  `/___/> \/____/ \/_/ 
                     \ \_\                   /\___/            
                      \/_/                   \/__/             
                                                (Sigbot Controller)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Run Sigbot Platformization Controller for Components Orchestration.")
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) {
        let service = Self::default();
        ApplicationContext::global()
            .init()
            .await
            .expect("provider context initialization failed");

        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        print_banner(verbose, Self::ASCII_NAME, None);

        apm::init().await;

        let signal_handle = service.management.start().await;

        Self::start(matches, verbose).await;

        signal_handle.await.expect("Failed to start Management server.");
        ApplicationContext::global()
            .shutdown()
            .await
            .expect("provider shutdown failed");
    }

    async fn start(_matches: &clap::ArgMatches, _verbose: bool) {
        ControllerServer::startup().await;
    }
}
