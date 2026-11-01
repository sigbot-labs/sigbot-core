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
pub mod backtest;
pub mod controller;
pub mod datafeed;
pub mod evaluator;
pub mod exporter;
pub mod internal;
pub mod logservice;
pub mod notifier;
pub mod order;
pub mod standalone;
pub mod strategy;
pub mod wallet;

use api::SigbotAPIServer;
use backtest::SigbotBacktest;
use clap::Arg;
use clap::ArgMatches;
use clap::Command;
use common_telemetry::info;
use sigbot_core::config::config;
use standalone::SigbotStandalone;
use std::sync::OnceLock;
use strategy::SigbotStrategy;

use crate::startup::datafeed::SigbotDatafeed;

use crate::startup::controller::SigbotController;

use crate::startup::evaluator::SigbotEvaluator;

use crate::startup::exporter::SigbotExporter;

use crate::startup::logservice::SigbotLogservice;

use crate::startup::notifier::SigbotNotifier;

use crate::startup::order::SigbotOrder;

use crate::startup::wallet::SigbotWallet;

type SubcommandBuildFn = fn() -> Command;
type SubcommandHandleFn = fn(&ArgMatches, bool) -> ();

static SUBCOMMAND_MAP: OnceLock<Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))>> = OnceLock::new();

pub fn register_subcommand_handles() -> &'static Vec<(&'static str, (SubcommandBuildFn, SubcommandHandleFn))> {
    SUBCOMMAND_MAP.get_or_init(|| {
        let mut vec = Vec::new();
        vec.push((
            SigbotStandalone::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotStandalone::build as SubcommandBuildFn,
                SigbotStandalone::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotAPIServer::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotAPIServer::build as SubcommandBuildFn,
                SigbotAPIServer::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotController::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotController::build as SubcommandBuildFn,
                SigbotController::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotDatafeed::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotDatafeed::build as SubcommandBuildFn,
                SigbotDatafeed::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotStrategy::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotStrategy::build as SubcommandBuildFn,
                SigbotStrategy::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotOrder::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotOrder::build as SubcommandBuildFn,
                SigbotOrder::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotWallet::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotWallet::build as SubcommandBuildFn,
                SigbotWallet::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotBacktest::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotBacktest::build as SubcommandBuildFn,
                SigbotBacktest::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotNotifier::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotNotifier::build as SubcommandBuildFn,
                SigbotNotifier::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotLogservice::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotLogservice::build as SubcommandBuildFn,
                SigbotLogservice::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotExporter::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotExporter::build as SubcommandBuildFn,
                SigbotExporter::run as SubcommandHandleFn,
            ),
        ));
        vec.push((
            SigbotEvaluator::COMMAND_NAME,
            (
                // Type inference error, forced conversion need.
                SigbotEvaluator::build as SubcommandBuildFn,
                SigbotEvaluator::run as SubcommandHandleFn,
            ),
        ));
        vec
    })
}

pub fn execute_commands_app() {
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
                info!("Executing subcommand: {}", name);
                handler(sub_matches, verbose);
            } else {
                // panic!("Unknown subcommand: {}. Use --help for a list of available commands.", name);
                eprintln!("Invalid commands and Use <command> --help for more information about a specific command.");
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
