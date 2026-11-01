// SPDX-License-Identifier: GPL-3.0-only

use crate::backtest::IBacktest;
use crate::kline::kline::KlineBacktest;
use crate::trades::trades::TradesBacktest;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::backtest::BacktestArgument;
use sigbot_types::backtest::BacktestProvider;
use sigbot_types::ArgumentConfig;
use std::sync::Arc;

pub struct SigbotBacktestServer;
impl SigbotBacktestServer {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let configuration = matches
            .get_one::<String>("BACKTEST_MANAGER_CONFIGURATION")
            .expect("backtest configuration required");
        let argument =
            BacktestArgument::from_json(&ArgumentConfig::decode(configuration).expect("invalid backtest encoding"))
                .expect("invalid backtest configuration");
        let provider = BacktestProvider::of(
            matches
                .get_one::<String>("BACKTEST_MANAGER_PROVIDER")
                .map(String::as_str)
                .unwrap_or("KLINE"),
        )
        .expect("invalid backtest provider");
        let key = sigbot_utils::providers::ProviderInstanceId::configured(provider.as_str(), &argument)
            .expect("invalid backtest identity");
        let properties = argument.properties.expect("backtest properties required");
        let messager = MessagerServer::connect(matches, argument.messager_config)
            .await
            .expect("backtest messager failed");
        let context = ApplicationContext::global();
        context
            .register_with_shutdown_phase::<dyn IBacktest, _>(&key, 20, move || {
                let properties = properties.clone();
                let messager = messager.clone();
                let provider = provider.clone();
                async move {
                    let instance: Arc<dyn IBacktest> = match provider {
                        BacktestProvider::Kline => KlineBacktest::new(properties, messager),
                        BacktestProvider::Trades => TradesBacktest::new(properties, messager),
                    };
                    Ok(instance)
                }
            })
            .expect("backtest registration failed");
        context
            .get::<dyn IBacktest>(&key)
            .await
            .expect("backtest startup failed");
    }
    pub async fn shutdown() {
        ApplicationContext::global()
            .close_all::<dyn IBacktest>()
            .await
            .expect("backtest close failed");
    }
}
