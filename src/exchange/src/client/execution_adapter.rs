// SPDX-License-Identifier: GPL-3.0-only
//! Execution mode selects an adapter; exchange production URLs are never rewritten.
use super::live_execution::{LiveExecutionConfig, LiveExecutionFactory};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::envelope::DurableMessage,
    order::{
        execution::{ExecutionFact, OrderReceipt, OrderStatusFact},
        intent::TradeIntent,
    },
};
use std::{sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Live,
    Paper,
    Simulated,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAdapterConfig {
    pub mode: ExecutionMode,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub token_env: String,
    pub timeout_seconds: u64,
    #[serde(default)]
    pub live: Option<LiveExecutionConfig>,
}

/// A venue's immutable submission identity and encrypted SDK payload. The order
/// repository persists this before claiming the single submission attempt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreparedSubmission {
    pub venue_order_id: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPage {
    pub facts: Vec<ExecutionFact>,
    pub status: Option<OrderStatusFact>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub struct SubmissionIdentity;
impl SubmissionIdentity {
    /// Deterministic venue encoding of the existing business key (<= 36 bytes).
    pub fn venue_reference(message: &DurableMessage<TradeIntent>) -> Result<String> {
        let key = serde_json::to_vec(&(
            &message.scope,
            message.payload.account_id,
            message.node_id,
            &message.run_id,
            message.seq,
        ))?;
        Ok(format!("sg{}", &hex::encode(Sha256::digest(key))[..32]))
    }
}

#[async_trait]
pub trait IExecutionAdapter: Send + Sync {
    fn mode(&self) -> ExecutionMode;
    /// Validate supported asset semantics before the order runtime accepts intents.
    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<()> {
        instrument.validate()
    }
    /// Read confirmed venue facts. Cursor advancement and publication are owned
    /// by Order's transaction, never by the SDK or an in-memory checkpoint.
    async fn read_executions(
        &self,
        _input: &DurableMessage<TradeIntent>,
        _receipt: &OrderReceipt,
        _prepared: Option<&PreparedSubmission>,
        _instrument: &InstrumentSpec,
        _cursor: Option<&str>,
        _limit: u16,
    ) -> Result<ExecutionPage> {
        anyhow::bail!("authoritative execution/fee polling is unavailable for this adapter")
    }
    /// Local signing / venue ID allocation only; never sends a trading request.
    async fn prepare(&self, _intent: &DurableMessage<TradeIntent>) -> Result<Option<PreparedSubmission>> {
        Ok(None)
    }
    async fn submit_prepared(
        &self,
        _intent: &DurableMessage<TradeIntent>,
        _prepared: &PreparedSubmission,
    ) -> Result<OrderReceipt> {
        anyhow::bail!("this exchange adapter does not accept prepared submissions")
    }
    async fn reconcile_prepared(
        &self,
        _intent: &DurableMessage<TradeIntent>,
        _prepared: &PreparedSubmission,
    ) -> Result<Option<OrderReceipt>> {
        anyhow::bail!("this exchange adapter does not reconcile prepared submissions")
    }
    async fn submit(&self, intent: &DurableMessage<TradeIntent>) -> Result<OrderReceipt>;
    /// None means the venue has no authoritative answer yet, never permission to resubmit.
    async fn reconcile(&self, intent: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>>;
}

pub struct ExecutionAdapterFactory;

impl ExecutionAdapterFactory {
    pub fn create(config: &ExecutionAdapterConfig) -> Result<Arc<dyn IExecutionAdapter>> {
        ensure!((1..=120).contains(&config.timeout_seconds), "invalid exchange timeout");
        match config.mode {
            ExecutionMode::Paper | ExecutionMode::Simulated => Ok(Arc::new(ReplayExecutionAdapter {
                mode: config.mode,
                endpoint: config
                    .endpoint
                    .as_ref()
                    .context("backtest execution endpoint required")?
                    .trim_end_matches('/')
                    .to_owned(),
                token: std::env::var(&config.token_env).context("execution token environment variable missing")?,
                client: Client::builder()
                    .timeout(Duration::from_secs(config.timeout_seconds))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
            })),
            ExecutionMode::Live => {
                ensure!(
                    config.endpoint.is_none(),
                    "live exchange URLs cannot be overridden by replay endpoints"
                );
                LiveExecutionFactory::create(
                    config.live.as_ref().context("scoped live SDK configuration required")?,
                    config.timeout_seconds,
                )
            }
        }
    }
}

struct ReplayExecutionAdapter {
    mode: ExecutionMode,
    endpoint: String,
    token: String,
    client: Client,
}

#[async_trait]
impl IExecutionAdapter for ReplayExecutionAdapter {
    fn mode(&self) -> ExecutionMode {
        self.mode
    }

    async fn submit(&self, intent: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        intent.validate()?;
        ensure!(
            intent.scope.env != "live" && intent.scope.env != "production",
            "replay cannot use a live environment"
        );
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
