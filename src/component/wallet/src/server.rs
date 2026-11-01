// SPDX-License-Identifier: GPL-3.0-only

use crate::ledger::account::WalletAccountProcessor;
use common_context::ApplicationContext;
use sigbot_core::config::config::get_config;
use sigbot_messager::server::MessagerServer;
use sigbot_types::wallet::WalletArgument;
use sigbot_types::ArgumentConfig;

pub struct SigbotWalletServer;

impl SigbotWalletServer {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("WALLET_MANAGER_CONFIGURATION")
            .expect("wallet host configuration required");
        let argument =
            WalletArgument::from_json(&ArgumentConfig::decode(encoded).expect("invalid wallet configuration encoding"))
                .expect("invalid wallet configuration");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("wallet host messager initialization failed");
        let config = get_config();
        config
            .authguard
            .service_principal_id
            .as_ref()
            .filter(|principal| !principal.is_empty())
            .expect("wallet host service principal required");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<WalletAccountProcessor, _>("wallet", 30, move || {
                let argument = argument.clone();
                let messager = messager.clone();
                async move { WalletAccountProcessor::start(&argument, messager).await }
            })
            .expect("wallet registration failed");
        context
            .get::<WalletAccountProcessor>("wallet")
            .await
            .expect("wallet initialization failed");
    }
}
