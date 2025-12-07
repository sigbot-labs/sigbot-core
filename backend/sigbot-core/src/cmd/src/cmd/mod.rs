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

pub mod api_starter;
pub mod backtest_starter;
pub mod controller_starter;
pub mod datafeed_starter;
pub mod internal;
pub mod notification_starter;
pub mod standalone_starter;
pub mod strategy_starter;

use api_starter::SigbotAPIServer;
use backtest_starter::SigbotBacktestRunnerStarter;
use clap::{Arg, ArgMatches, Command};
use sigbot_core::config::config;
use standalone_starter::StandaloneServer;
use std::sync::OnceLock;
use strategy_starter::SigbotStrategyRunnerStarter;

use crate::cmd::{
    controller_starter::SigbotControllerManagerStarter, datafeed_starter::SigbotDatafeedIngestorStarter,
    notification_starter::SigbotNotificationForwarderStarter,
};

type SubcommandBuildFn = fn() -> Command;
type SubcommandHandleFn = fn(&ArgMatches, bool) -> ();

static SUBCOMMAND_MAP: OnceLock<Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))>> = OnceLock::new();

pub fn register_subcommand_handles() -> &'static Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))> {
    SUBCOMMAND_MAP.get_or_init(|| {
        let mut vec = Vec::new();
        vec.push((
            SigbotAPIServer::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotAPIServer::build as SubcommandBuildFn,
                SigbotAPIServer::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotControllerManagerStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotControllerManagerStarter::build as SubcommandBuildFn,
                SigbotControllerManagerStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotDatafeedIngestorStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotDatafeedIngestorStarter::build as SubcommandBuildFn,
                SigbotDatafeedIngestorStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotStrategyRunnerStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotStrategyRunnerStarter::build as SubcommandBuildFn,
                SigbotStrategyRunnerStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotBacktestRunnerStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotBacktestRunnerStarter::build as SubcommandBuildFn,
                SigbotBacktestRunnerStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotNotificationForwarderStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotNotificationForwarderStarter::build as SubcommandBuildFn,
                SigbotNotificationForwarderStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            StandaloneServer::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                StandaloneServer::build as SubcommandBuildFn,
                StandaloneServer::run as SubcommandHandleFn,
            ),
        ));
        vec
    })
}

pub fn execute_commands_app() -> () {
    let mut app = Command::new("Sigbot")
        .version(sigbot_core::config::config::VERSION.as_str())
        .author("James Wong")
        .about(
            format!(
                "Sigbot - An Open Source Multi-Strategy, AI-driven Fast Trading Bot written in Rust.\n\n{}",
                config::VERSION.as_str()
            )
            .to_owned(),
        )
        .arg_required_else_help(true) // When no args are provided, show help.
        //.help_template("{about}\n\n{usage-heading}\n\n{usage}\n\n{all-args}")
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .value_name("PRINT") // Tips for the user.
                .help("Set up global details print flag")
                .global(true), // Global args are available to all subcommands.
        );

    let subcommand_map = register_subcommand_handles();
    // Add to all subcommands.
    for (name, (build_fn, _)) in subcommand_map.iter() {
        app = app.subcommand(build_fn().name(name));
    }

    let matches = app.get_matches();
    let verbose = matches.contains_id("verbose");

    // Handling to actual subcommand.
    match matches.subcommand() {
        Some((name, sub_matches)) => {
            if let Some((_, (_, handler))) = subcommand_map.iter().find(|(n, _)| *n == name) {
                tracing::info!("Executing subcommand: {}", name);
                handler(sub_matches, verbose);
            } else {
                // panic!("Unknown subcommand: {}. Use --help for a list of available commands.", name);
                eprintln!("Invalid commands and Use <command> --help for more information about a specific command.");
                std::process::exit(1);
            }
        }
        None => {
            tracing::info!("No subcommand was used. Available commands are:");
            for (name, _) in subcommand_map.iter() {
                tracing::info!("  {}", name);
            }
            tracing::info!("Use <command> --help for more information about a specific command.");
        }
    }
}
