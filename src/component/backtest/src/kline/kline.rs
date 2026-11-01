// SPDX-License-Identifier: GPL-3.0-only

use crate::backtest::IBacktest;
use crate::trades::trades::TradesBacktest;
use common_context::IProvider;
use sigbot_messager::messager::IMessager;
use sigbot_types::backtest::BacktestProvider;
use std::collections::HashMap;
use std::sync::Arc;

pub struct KlineBacktest {
    trades: Arc<TradesBacktest>,
}
impl KlineBacktest {
    pub fn new(properties: HashMap<String, String>, messager: Arc<dyn IMessager>) -> Arc<Self> {
        Arc::new(Self {
            trades: TradesBacktest::new(properties, messager),
        })
    }
}
#[async_trait::async_trait]
impl IProvider for KlineBacktest {
    async fn init(&self) -> anyhow::Result<()> {
        IProvider::init(self.trades.as_ref()).await
    }
    async fn close(&self) -> anyhow::Result<()> {
        IProvider::close(self.trades.as_ref()).await
    }
}
#[async_trait::async_trait]
impl IBacktest for KlineBacktest {
    fn provider(&self) -> BacktestProvider {
        BacktestProvider::Kline
    }
}
