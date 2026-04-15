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

//! E2E Flow Tests
//!
//! Flow tests organized by data flow stages.
//!
//! **Data Flow**: `datafeed → EMQX → [strategy, evaluator] → EMQX → order → wallet → DB → export`
//!
//! # Naming Convention
//!
//! `fNN_<stage>_<action>.rs`
//! - `NN`: Stage number (10=Datafeed, 20=Strategy, 21=Evaluator, 30=Order, 40=Wallet, 50=Log, 60=Export)
//! - `<stage>`: Service/component name
//! - `<action>`: Operation type (ingest_pub/calc_pub/trade_exec_pub/ledger_balance/archive/external)
//!
//! # Stage Numbering
//!
//! | Range | Module | Description |
//! |-------|--------|-------------|
//! | 00 | Full Pipeline | Complete end-to-end flow |
//! | 10-19 | Datafeed | Market data ingestion & news publishing |
//! | 20-29 | Strategy/Evaluator | Signal & hyperparameter calculation/publishing (parallel) |
//! | 30-39 | Order | Order execution & publishing |
//! | 40-49 | Wallet | Ledger & balance updates |
//! | 50-59 | LogService | Audit log archiving |
//! | 60-69 | Export | External system exports (Kafka, HTTP, Google) |
//! | 70-79 | Audit | Cross-service audit trail |

pub mod f00_full_pipeline;         // Full: datafeed → backtest → strategy → order → wallet → DB → export
pub mod f10_datafeed_ingest_pub;   // Datafeed ingests market data & news from external sources
pub mod f20_strategy_signal_calc_pub; // Strategy calculates & publishes trading signals
pub mod f21_evaluator_hyperparam_pub; // Evaluator calculates & publishes hyperparameters (parallel with strategy)
pub mod f30_order_trade_exec_pub;  // Order service executes trades & publishes results
pub mod f40_wallet_ledger_balance; // Wallet service updates ledger & balance
pub mod f50_logservice_archive;    // Log service archives logs to DB
pub mod f60_export_external;       // Export service sends data to external systems
pub mod f70_logservice_audit;      // Audit log across all services (compliance trail)
