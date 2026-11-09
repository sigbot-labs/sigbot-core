// SPDX-License-Identifier: GPL-3.0-only

use crate::datafeed::IDatafeed;
use crate::social::social::FeedHandler;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use rust_decimal::Decimal;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::DatafeedProperties;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use sigbot_types::datafeed::event::DatafeedEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct BinanceSource {
    endpoint: String,
    symbol: String,
    interval: Duration,
    client: reqwest::Client,
}

pub struct BinanceDatafeed {
    source: Mutex<Option<BinanceSource>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl BinanceDatafeed {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            source: Mutex::new(None),
            task: Mutex::new(None),
        })
    }

    pub async fn snapshot(endpoint: &str, symbol: &str, client: &reqwest::Client) -> Result<DatafeedEvent> {
        ensure!(
            !symbol.is_empty()
                && symbol.len() <= 32
                && symbol.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
            "invalid Binance symbol"
        );
        let mut response = client
            .get(format!("{}/api/v3/ticker/24hr", endpoint.trim_end_matches('/')))
            .query(&[("symbol", symbol), ("type", "MINI")])
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            response.content_length().unwrap_or(0) <= 65_536,
            "Binance snapshot too large"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(bytes.len() + chunk.len() <= 65_536, "response exceeds bounded buffer");
            bytes.extend_from_slice(&chunk);
        }
        ensure!(bytes.len() <= 65_536, "Binance snapshot too large");
        let raw: Value = serde_json::from_slice(&bytes)?;
        let price = raw
            .get("lastPrice")
            .and_then(Value::as_str)
            .context("Binance lastPrice missing")?;
        ensure!(price.parse::<Decimal>()? > Decimal::ZERO, "invalid Binance price");
        ensure!(
            raw.get("symbol").and_then(Value::as_str) == Some(symbol),
            "Binance snapshot symbol mismatch"
        );
        let seq = raw
            .get("closeTime")
            .and_then(Value::as_i64)
            .context("Binance closeTime missing")?;
        let event_time = DateTime::from_timestamp_millis(seq).context("invalid Binance closeTime")?;
        let available_time = Utc::now();
        ensure!(
            event_time <= available_time,
            "Binance clock is ahead; reject unverified future snapshot"
        );
        let payload = json!({"price": price, "source_payload": raw});
        Ok(DatafeedEvent {
            source: "binance".to_owned(),
            source_event_id: format!("{symbol}:{seq}"),
            symbol: symbol.to_owned(),
            event_type: "market".to_owned(),
            event_time,
            available_time,
            seq,
            revision: 0,
            content: None,
            raw: payload,
        })
    }
}

#[async_trait]
impl IDatafeed for BinanceDatafeed {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::Binance
    }

    async fn configure(&self, argument: Arc<DatafeedProperties>) -> Result<()> {
        let properties = argument
            .properties
            .as_ref()
            .context("Binance source properties required")?;
        let endpoint = properties
            .get("binance_endpoint")
            .cloned()
            .unwrap_or_else(|| "https://api.binance.com".to_owned());
        let parsed = url::Url::parse(&endpoint)?;
        ensure!(
            parsed.scheme() == "https" && parsed.username().is_empty() && parsed.password().is_none(),
            "public Binance source requires HTTPS"
        );
        let symbol = properties.get("symbol").context("Binance symbol required")?.clone();
        let interval_ms = properties
            .get("poll_interval_ms")
            .map(String::as_str)
            .unwrap_or("5000")
            .parse::<u64>()?;
        ensure!(
            (1000..=300_000).contains(&interval_ms),
            "Binance poll interval out of bounds"
        );
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        *self.source.lock().await = Some(BinanceSource {
            endpoint,
            symbol,
            interval: Duration::from_millis(interval_ms),
            client,
        });
        Ok(())
    }

    async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        let source = self
            .source
            .lock()
            .await
            .clone()
            .context("Binance source not initialized")?;
        let mut task = self.task.lock().await;
        ensure!(task.is_none(), "Binance source already subscribed");
        let first = Self::snapshot(&source.endpoint, &source.symbol, &source.client).await?;
        handler(serde_json::to_vec(&first)?).await?;
        *task = Some(tokio::spawn(async move {
            let mut delay = source.interval;
            loop {
                tokio::time::sleep(delay).await;
                let result = async {
                    let event = Self::snapshot(&source.endpoint, &source.symbol, &source.client).await?;
                    handler(serde_json::to_vec(&event)?).await?;
                    anyhow::Ok(())
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "Binance snapshot failed; backing off without invented data");
                    delay = (delay * 2).min(Duration::from_secs(60));
                } else {
                    delay = source.interval;
                }
            }
        }));
        Ok(())
    }
}

#[async_trait::async_trait]
impl IProvider for BinanceDatafeed {
    async fn close(&self) -> anyhow::Result<()> {
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }

        Ok(())
    }
}
