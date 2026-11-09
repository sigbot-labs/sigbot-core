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

pub mod api;
pub mod component;
pub mod controller;
pub mod internal;
pub mod standalone;

use api::http::SigbotHttpStarter;
use clap::Arg;
use clap::ArgMatches;
use clap::Command;
use common_telemetry::info;
use component::backtest::SigbotBacktestStarter;
use component::strategy::SigbotStrategyStarter;
use sigbot_core::config::config::{self, ConfigLoader};
use standalone::SigbotStandaloneStarter;
use std::sync::OnceLock;

use crate::startup::component::datafeed::SigbotDatafeedStarter;

use crate::startup::controller::SigbotControllerStarter;

use crate::startup::component::evaluator::SigbotEvaluatorStarter;

use crate::startup::component::exporter::SigbotExporterStarter;

use crate::startup::component::logger::SigbotLoggerStarter;

use crate::startup::component::notifier::SigbotNotifierStarter;

use crate::startup::component::order::SigbotOrderStarter;

use crate::startup::component::wallet::SigbotWalletStarter;

type SubcommandBuildFn = fn() -> Command;
type SubcommandHandleFn = fn(&ArgMatches, bool) -> ();

static SUBCOMMAND_MAP: OnceLock<Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))>> = OnceLock::new();

pub fn register_subcommand_handles() -> &'static Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))> {
    SUBCOMMAND_MAP.get_or_init(|| {
        let mut vec = Vec::new();
        vec.push((
            SigbotStandaloneStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotStandaloneStarter::build as SubcommandBuildFn,
                SigbotStandaloneStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotHttpStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotHttpStarter::build as SubcommandBuildFn,
                SigbotHttpStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotControllerStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotControllerStarter::build as SubcommandBuildFn,
                SigbotControllerStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotDatafeedStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotDatafeedStarter::build as SubcommandBuildFn,
                SigbotDatafeedStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotStrategyStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotStrategyStarter::build as SubcommandBuildFn,
                SigbotStrategyStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotOrderStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotOrderStarter::build as SubcommandBuildFn,
                SigbotOrderStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotWalletStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotWalletStarter::build as SubcommandBuildFn,
                SigbotWalletStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotBacktestStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotBacktestStarter::build as SubcommandBuildFn,
                SigbotBacktestStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotNotifierStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotNotifierStarter::build as SubcommandBuildFn,
                SigbotNotifierStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotLoggerStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotLoggerStarter::build as SubcommandBuildFn,
                SigbotLoggerStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotExporterStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotExporterStarter::build as SubcommandBuildFn,
                SigbotExporterStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotEvaluatorStarter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotEvaluatorStarter::build as SubcommandBuildFn,
                SigbotEvaluatorStarter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            api::a2a::SigbotA2aStarter::COMMAND_NAME,
            (
                api::a2a::SigbotA2aStarter::build as SubcommandBuildFn,
                api::a2a::SigbotA2aStarter::run as SubcommandHandleFn,
            ),
        ));
        vec
    })
}

pub struct SigbotCli;

impl SigbotCli {
    pub(crate) fn build() -> Command {
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

        for (name, (build_fn, _)) in register_subcommand_handles() {
            app = app.subcommand(build_fn().name(name).arg(Self::config_arg()));
        }
        app
    }

    fn config_arg() -> Arg {
        Arg::new("config")
            .short('c')
            .long("config")
            .value_name("PATH")
            .required(true)
            .help("Path to the process YAML configuration")
    }

    pub fn execute() {
        let subcommand_map = register_subcommand_handles();
        let matches = Self::build().get_matches();
        let verbose = matches.contains_id("verbose");

        match matches.subcommand() {
            Some((name, sub_matches)) => {
                if let Some((_, (_, handler))) = subcommand_map.iter().find(|(n, _)| *n == name) {
                    ConfigLoader::initialize(
                        sub_matches
                            .get_one::<String>("config")
                            .expect("configuration path is required"),
                    )
                    .expect("failed to load process configuration");
                    info!("Executing subcommand: {}", name);
                    handler(sub_matches, verbose);
                } else {
                    eprintln!(
                        "Invalid commands and Use <command> --help for more information about a specific command."
                    );
                    std::process::exit(1);
                }
            }
            None => {
                info!("No subcommand was used. Available commands are:");
                for (name, _) in subcommand_map.iter() {
                    info!("  {}", name);
                }
                info!("Use <command> --help for more information about a specific command.");
            }
        }
    }
}

pub fn execute_commands_app() {
    SigbotCli::execute();
}
