// SPDX-License-Identifier: GPL-3.0-only
use crate::exchange::{ExecutionMode, IExecutionAdapter};
use anyhow::{Context, Result};
use async_trait::async_trait;
use common_context::IProvider;
use reqwest::Client;
use sigbot_core::config::config::get_config;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::intent::TradeIntent;
use std::time::Duration;

pub struct PaperExchange {
    mode: ExecutionMode,
    endpoint: String,
    token: String,
    client: Client,
}

#[async_trait]
impl IExecutionAdapter for PaperExchange {
    fn mode(&self) -> ExecutionMode {
        self.mode
    }

    async fn submit(&self, intent: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        intent.validate()?;
        self.client
            .post(format!("{}/orders", self.endpoint))
            .bearer_auth(&self.token)
            .header("traceparent", &intent.traceparent)
            .json(intent)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("invalid replay acceptance receipt")
    }

    async fn reconcile(&self, intent: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        intent.validate()?;
        let response = self
            .client
            .post(format!("{}/orders/reconcile", self.endpoint))
            .bearer_auth(&self.token)
            .header("traceparent", &intent.traceparent)
            .json(intent)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.json().await?))
    }
}

impl PaperExchange {
    pub fn new(config: &crate::exchange::ExecutionAdapterConfig) -> Result<std::sync::Arc<Self>> {
        Ok(std::sync::Arc::new(Self {
            mode: config.mode,
            endpoint: config
                .endpoint
                .as_ref()
                .context("paper/backtest execution endpoint required")?
                .trim_end_matches('/')
                .to_owned(),
            token: get_config().secrets.require(&config.token_secret)?.to_owned(),
            client: Client::builder()
                .timeout(Duration::from_secs(config.timeout_seconds))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        }))
    }
}

#[async_trait::async_trait]
impl IProvider for PaperExchange {
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
