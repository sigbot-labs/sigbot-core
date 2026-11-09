// SPDX-License-Identifier: GPL-3.0-only

use crate::backtest::IBacktest;
use crate::kline::kline::KlineBacktest;
use crate::trades::trades::TradesBacktest;
use common_context::ApplicationContext;
use sigbot_messager::server::MessagerServer;
use sigbot_types::backtest::BacktestProvider;
use std::sync::Arc;

pub struct SigbotBacktestServer;
impl SigbotBacktestServer {
    pub async fn startup() {
        let argument = sigbot_core::config::config::get_config()
            .services
            .component
            .backtest
            .clone()
            .expect("services.component.backtest required");
        let provider = argument.provider.clone().unwrap_or(BacktestProvider::Kline);
        let key = sigbot_utils::providers::ProviderInstanceId::configured(provider.as_str(), &argument)
            .expect("invalid backtest identity");
        let properties = argument.properties.expect("backtest properties required");
        let messager = MessagerServer::connect(
            sigbot_core::config::config::get_config()
                .services
                .component
                .messager
                .clone(),
        )
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
