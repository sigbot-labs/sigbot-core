// SPDX-License-Identifier: GPL-3.0-only

use crate::binance::orders::BinanceSpotExecution;
use crate::exchange::ExecutionAdapterConfig;
use crate::exchange::ExecutionMode;
use crate::exchange::IExecutionAdapter;
use crate::exchange::LiveExecutionConfig;
use crate::ibkr::orders::IbkrExecution;
use crate::paper::paper::PaperExchange;
use crate::polymarket::orders::PolymarketExecution;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use common_context::ApplicationContext;
use std::sync::Arc;

pub struct ExchangeServer;
impl ExchangeServer {
    pub async fn connect(config: &ExecutionAdapterConfig) -> Result<Arc<dyn IExecutionAdapter>> {
        ensure!((1..=120).contains(&config.timeout_seconds), "invalid exchange timeout");
        let key = sigbot_utils::providers::ProviderInstanceId::configured("exchange_execution", config)?;
        let config = config.clone();
        let context = ApplicationContext::global();
        context.register_if_absent::<dyn IExecutionAdapter, _>(&key, move || {
            let config = config.clone();
            async move {
                let adapter: Arc<dyn IExecutionAdapter> = match config.mode {
                    ExecutionMode::Paper | ExecutionMode::Backtest => PaperExchange::new(&config)?,
                    ExecutionMode::Live => {
                        ensure!(config.endpoint.is_none(), "live exchange URLs cannot be overridden");
                        match config.live.as_ref().context("scoped live SDK configuration required")? {
                            LiveExecutionConfig::BinanceSpot(venue) => {
                                BinanceSpotExecution::new(venue, config.timeout_seconds)?
                            }
                            LiveExecutionConfig::Ibkr(venue) => {
                                Arc::new(IbkrExecution::new(venue, config.timeout_seconds)?)
                            }
                            LiveExecutionConfig::Polymarket(venue) => {
                                Arc::new(PolymarketExecution::new(venue, config.timeout_seconds)?)
                            }
                        }
                    }
                };
                Ok(adapter)
            }
        })?;
        context.get::<dyn IExecutionAdapter>(&key).await
    }
}
