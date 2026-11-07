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
use sigbot_evaluator::server::SigbotEvaluatorRunner;

#[derive(Default)]
pub struct SigbotEvaluatorStarter {
    management: SigbotManagementStarter,
}

impl SigbotEvaluatorStarter {
    pub const COMMAND_NAME: &'static str = "evaluator";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ____                      ___                      __                   
/\  _`\                   /\_ \                    /\ \__                
\ \ \L\_\  __  __     __  \//\ \    __  __     __  \ \ ,_\   ___   _ __  
 \ \  _\L /\ \/\ \  /'__`\  \ \ \  /\ \/\ \  /'__`\ \ \ \/  / __`\/\`'__\
  \ \ \L\ \ \ \_/ |/\ \L\.\_ \_\ \_\ \ \_\ \/\ \L\.\_\ \ \_/\ \L\ \ \ \/ 
   \ \____/\ \___/ \ \__/.\_\/\____\\ \____/\ \__/.\_\\ \__\ \____/\ \_\ 
    \/___/  \/__/   \/__/\/_/\/____/ \/___/  \/__/\/_/ \/__/\/___/  \/_/ 

                                                (Sigbot Evaluator Service)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Tenantization Evaluator Service for AI-powered Evaluation.")
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
        SigbotEvaluatorRunner::startup().await;
    }
}
