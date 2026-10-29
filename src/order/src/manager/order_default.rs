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
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use sigbot_types::modules::order::events::SigbotTradeSignal;
use sigbot_types::modules::order::{OrderMgrProvider, SigbotOrderManagerArgument};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Compatibility facade: financial writes require the scoped v2 processor.
#[derive(Clone)]
pub struct SigbotDefaultOrderManager;

impl SigbotDefaultOrderManager {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    async fn process_signal_internal(&self, _signal: SigbotTradeSignal) -> Result<(), Error> {
        // Legacy messages have neither environment/space nor a durable run/sequence.
        // They cannot safely reserve funds or reconcile an uncertain venue outcome.
        Err(Error::msg("Unscoped v1 trading signals are read-only during migration; submit a v2 TradeIntent through OrderProcessor"))
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
