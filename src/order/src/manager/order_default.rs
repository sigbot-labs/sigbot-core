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

use crate::manager::order_factory::ISigbotOrderManager;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_exchange::client::exchange_factory::ISigbotExchangeClient;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::messager::TOPIC_TRADING_RESULTS;
use sigbot_types::modules::order::events::{SigbotTradeEvent, SigbotTradeSignal};
use sigbot_types::modules::order::{OrderMgrProvider, SigbotOrderManagerArgument};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

/// Risk check configuration.
#[derive(Clone, Debug)]
struct RiskConfig {
    max_order_size: f64,
    max_position_size: f64,
    max_leverage: f64,
}
impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_order_size: 100000.0,
            max_position_size: 1000000.0,
            max_leverage: 10.0,
        }
    }
}

/// Risk check manager.
struct RiskManager {
    config: RiskConfig,
}

impl RiskManager {
    fn new(config: RiskConfig) -> Self {
        Self { config }
    }

    /// Execute risk check.
    async fn check_risk(&self, signal: &SigbotTradeSignal) -> Result<(), Error> {
        let order_value = signal.signal.enter_pos.price.unwrap_or(0.0) * signal.signal.enter_pos.quantity;

        // Check order size.
        if order_value > self.config.max_order_size {
            return Err(Error::msg(format!(
                "Order size {} exceeds maximum {}",
                order_value, self.config.max_order_size
            )));
        }

        // TODO: Query current position from database and check maximum position size.
        // TODO: Check leverage limit.

        info!(
            "Risk check passed for signal_id={}, order_value={}",
            signal.signal_id, order_value
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct SigbotDefaultOrderManager {
    messager_client: Arc<Mutex<Option<Arc<dyn ISigbotMessagerClient>>>>,
    exchange_clients: Arc<Mutex<HashMap<String, Arc<dyn ISigbotExchangeClient>>>>,
    risk_manager: Arc<RiskManager>,
}

impl SigbotDefaultOrderManager {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            messager_client: Arc::new(Mutex::new(None)),
            exchange_clients: Arc::new(Mutex::new(HashMap::new())),
            risk_manager: Arc::new(RiskManager::new(RiskConfig::default())),
        })
    }

    async fn get_exchange_client(
        &self,
        exchange: &str,
        tenant_id: &str,
    ) -> Result<Arc<dyn ISigbotExchangeClient>, Error> {
        let clients_guard = self.exchange_clients.lock().await;

        if let Some(client) = clients_guard.get(exchange) {
            return Ok(client.clone());
        }

        // TODO: 从数据库或配置加载交易所信息
        Err(Error::msg(format!(
            "Exchange client not found for exchange={}, tenant_id={}",
            exchange, tenant_id
        )))
    }

    async fn process_signal_internal(&self, signal: SigbotTradeSignal) -> Result<(), Error> {
        // Check risk.
        self.risk_manager.check_risk(&signal).await?;

        // Get exchange client.
        let exchange_client = self.get_exchange_client(&signal.exchange, &signal.tenant_id).await?;

        // Place order (with retry mechanism).
        let mut retries = 3;
        let mut delay = Duration::from_secs(1);
        let trade_result = loop {
            match exchange_client.enter_position(signal.signal.to_owned()).await {
                Ok(result) => break result,
                Err(e) => {
                    retries -= 1;
                    if retries == 0 {
                        return Err(e).context("Failed to place order after retries");
                    }
                    warn!("Order placement failed, retrying... Error: {}", e);
                    sleep(delay).await;
                    delay *= 2; // Exponential backoff.
                }
            }
        };

        // 构建 SigbotTradeEvent（简化版本，实际应该从交易所回调获取详细信息）
        let trade_event = SigbotTradeEvent {
            trade_id: 0, // TODO: 从交易所回调获取
            order_id: trade_result.order_id as i64,
            signal_id: signal.signal_id.clone(),
            tenant_id: signal.tenant_id.clone(),
            wallet_id: signal.wallet_id,
            exchange: signal.exchange.clone(),
            symbol: signal.signal.enter_pos.symbol.clone(),
            side: signal.signal.enter_pos.side.to_side_str().to_string(),
            price: signal.signal.enter_pos.price.unwrap_or(0.0),
            qty: signal.signal.enter_pos.quantity,
            fee: 0.0, // TODO: 从交易所回调获取
            fee_asset: None,
            exchange_order_id: Some(trade_result.order_id.to_string()),
            exchange_trade_id: None,
            ts: chrono::Utc::now().timestamp_millis(),
        };

        // 发布 SigbotTradeEvent
        let messager_guard = self.messager_client.lock().await;
        let messager = messager_guard
            .as_ref()
            .ok_or_else(|| Error::msg("Messager client not initialized"))?;

        let topic = TOPIC_TRADING_RESULTS.replace("{tenant_id}", &signal.tenant_id);
        let message = serde_json::to_string(&trade_event).context("Failed to serialize SigbotTradeEvent")?;

        messager
            .publish(&topic, &message)
            .await
            .context("Failed to publish SigbotTradeEvent")?;

        info!(
            "Published SigbotTradeEvent for signal_id={}, order_id={}",
            signal.signal_id, trade_result.order_id
        );

        Ok(())
    }
}

#[async_trait]
impl ISigbotOrderManager for SigbotDefaultOrderManager {
    fn provider(&self) -> OrderMgrProvider {
        OrderMgrProvider::DEFAULT
    }

    async fn init(&self, _argument: Arc<SigbotOrderManagerArgument>) {
        debug!("Initializing Order manager");
        info!("Order manager initialized");
    }

    async fn close(&self) {
        info!("Shutting down Order manager");
    }

    async fn subscribe(
        &self,
        _handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) {
        info!("Subscribed to trading orders");
        // 实际的订阅逻辑在 order_server.rs 中处理
    }

    async fn process_signal(
        &self,
        signal: sigbot_types::modules::order::events::SigbotTradeSignal,
    ) -> Result<(), Error> {
        self.process_signal_internal(signal).await
    }
}

#[cfg(test)]
mod tests {}
