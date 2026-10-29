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

//! Retained CLI compatibility for the retired unauthenticated MCP prototype.
//! External MCP ingestion is implemented by Datafeed using the upstream rmcp client.

use clap::{error::ErrorKind, Arg, ArgMatches, Command};

pub struct SigbotMCPServer;

impl SigbotMCPServer {
    pub const COMMAND_NAME: &'static str = "mcp-server";
    pub const MIGRATION_NOTICE: &'static str = "The legacy mcp-server prototype is disabled: it did not provide authenticated business resources. Configure a scoped Workflow Datafeed node with transport=mcp, endpoint and token_env to use the official rmcp client. Business operations remain behind the Authguard-protected API. No listener was started.";

    /// Keep existing flags parseable so migration fails with a precise explanation.
    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME)
            .about("Retired MCP prototype; migrate external ingestion to Workflow Datafeed")
            .arg(
                Arg::new("BIND_ADDRESS")
                    .long("bind-address")
                    .value_name("ADDRESS")
                    .default_value("0.0.0.0:8080"),
            )
            .arg(
                Arg::new("TRANSPORT")
                    .long("transport")
                    .value_name("TYPE")
                    .default_value("sse"),
            )
            .arg(
                Arg::new("AUTH_ENABLED")
                    .long("auth-enabled")
                    .value_name("BOOL")
                    .default_value("true"),
            )
            .arg(
                Arg::new("EXTERNAL_MCP_SERVICES")
                    .long("external-mcp-services")
                    .value_name("CONFIG"),
            )
    }

    pub fn run(_matches: &ArgMatches, _verbose: bool) {
        // Never read or print legacy JWT configuration, bind a socket, fabricate a
        // market price/position, or return a successful backtest receipt.
        clap::Error::raw(ErrorKind::InvalidValue, Self::MIGRATION_NOTICE).exit();
    }
}
