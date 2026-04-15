// SPDX-License-Identifier: GNU GENERAL Public LICENSE Version 3
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

//! E2E All Tests Runner
//!
//! This test runner executes all E2E integration tests.

// Use crate modules
use crate::common::*;
use crate::flows::*;

// ============================================================================
// Flow 00: Full Pipeline Tests
// ============================================================================
#[cfg(test)]
mod f00_full_pipeline {
    use crate::flows::f00_full_pipeline;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_datafeed_to_database() {
        f00_full_pipeline::test_complete_workflow_datafeed_to_database().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_concurrent_multiple_trades() {
        f00_full_pipeline::test_concurrent_workflow_multiple_trades().await;
    }
}

// ============================================================================
// Flow 10: Datafeed → EMQX (Market Data Ingest & Publish)
// ============================================================================
#[cfg(test)]
mod f10_datafeed_ingest_pub {
    use crate::flows::f10_datafeed_ingest_pub;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_ingest_from_source() {
        f10_datafeed_ingest_pub::test_market_data_ingest_from_source().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_ingest_multiple_sources() {
        f10_datafeed_ingest_pub::test_market_data_ingest_multiple_sources().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_ingest_publish_to_emqx() {
        f10_datafeed_ingest_pub::test_market_data_ingest_publish_to_emqx().await;
    }
}

// ============================================================================
// Flow 20: Strategy → EMQX (Trading Signal Calc & Publish)
// ============================================================================
#[cfg(test)]
mod f20_strategy_signal_calc_pub {
    use crate::flows::f20_strategy_signal_calc_pub;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_signal_from_strategy() {
        f20_strategy_signal_calc_pub::test_trading_signal_from_strategy().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_signal_multiple() {
        f20_strategy_signal_calc_pub::test_trading_signal_multiple().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_signal_buy_sell() {
        f20_strategy_signal_calc_pub::test_trading_signal_buy_sell().await;
    }
}

// ============================================================================
// Flow 21: Evaluator → EMQX (Hyperparameter Calc & Publish)
// ============================================================================
#[cfg(test)]
mod f21_evaluator_hyperparam_pub {
    use crate::flows::f21_evaluator_hyperparam_pub;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_hyperparam_basic() {
        f21_evaluator_hyperparam_pub::test_hyperparameter_basic().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_hyperparam_multiple_updates() {
        f21_evaluator_hyperparam_pub::test_hyperparameter_multiple_updates().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_hyperparam_trading_levels() {
        f21_evaluator_hyperparam_pub::test_hyperparameter_trading_levels().await;
    }
}

// ============================================================================
// Flow 30: Order → Wallet via EMQX (Trade Execute & Publish)
// ============================================================================
#[cfg(test)]
mod f30_order_trade_exec_pub {
    use crate::flows::f30_order_trade_exec_pub;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_trade_execute() {
        f30_order_trade_exec_pub::test_order_trade_execute().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_trade_multiple() {
        f30_order_trade_exec_pub::test_order_trade_multiple().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_trade_publish_result() {
        f30_order_trade_exec_pub::test_order_trade_publish_result().await;
    }
}

// ============================================================================
// Flow 40: Wallet Service (Ledger & Balance Updates)
// ============================================================================
#[cfg(test)]
mod f40_wallet_ledger_balance {
    use crate::flows::f40_wallet_ledger_balance;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_ledger_balance_single() {
        f40_wallet_ledger_balance::test_wallet_ledger_balance_single().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_ledger_balance_multiple() {
        f40_wallet_ledger_balance::test_wallet_ledger_balance_multiple().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_ledger_balance_with_position() {
        f40_wallet_ledger_balance::test_wallet_ledger_balance_with_position().await;
    }
}

// ============================================================================
// Flow 50: Log Service → DB (Archive)
// ============================================================================
#[cfg(test)]
mod f50_logservice_archive {
    use crate::flows::f50_logservice_archive;

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_archive_basic() {
        f50_logservice_archive::test_log_archive_basic().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_archive_levels() {
        f50_logservice_archive::test_log_archive_levels().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and EMQX environment"]
    async fn test_archive_multiple_services() {
        f50_logservice_archive::test_log_archive_multiple_services().await;
    }
}

// ============================================================================
// Flow 60: Export Service → External Systems
// ============================================================================
#[cfg(test)]
mod f60_export_external {
    use crate::flows::f60_export_external;

    #[tokio::test]
    #[ignore = "Requires Docker and Kafka environment"]
    async fn test_export_to_kafka() {
        f60_export_external::test_export_trade_to_kafka().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and HTTP mock server"]
    async fn test_export_to_http_webhook() {
        f60_export_external::test_export_trade_via_http_webhook().await;
    }

    #[tokio::test]
    #[ignore = "Requires Google API credentials"]
    async fn test_export_to_google_sheets() {
        f60_export_external::test_export_trade_to_google_sheets().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker and Kafka environment"]
    async fn test_export_balance_snapshot() {
        f60_export_external::test_export_balance_snapshot().await;
    }
}

// ============================================================================
// Flow 70: Audit Log Service (Cross-Service Compliance Trail)
// ============================================================================
#[cfg(test)]
mod f70_logservice_audit {
    use crate::flows::f70_logservice_audit;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_news_event() {
        f70_logservice_audit::test_audit_log_datafeed_news_event().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_strategy_signal() {
        f70_logservice_audit::test_audit_log_strategy_signal_calculation().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_order_execution() {
        f70_logservice_audit::test_audit_log_order_execution().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_wallet_update() {
        f70_logservice_audit::test_audit_log_wallet_update().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_complete_lifecycle() {
        f70_logservice_audit::test_audit_log_complete_trade_lifecycle().await;
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_audit_query_filter() {
        f70_logservice_audit::test_audit_log_query_and_filter().await;
    }
}
