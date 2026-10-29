// SPDX-License-Identifier: GPL-3.0-only
//! Official policy RSS/Atom sources. The existing ingestor owns facts and delivery.
use super::social_feed::FeedHandler;
use crate::client::datafeed_factory::ISigbotDatafeedClient;
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::json;
use sigbot_types::modules::{
    datafeed::{datafeed::DatafeedProvider, event::DatafeedEvent, SigbotDatefeedArgument},
    messager::envelope::TraceContext,
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Clone)]
pub struct PolicyNewsSource {
    source: String,
    endpoint: String,
    traceparent: String,
    interval: Duration,
    client: reqwest::Client,
}

impl PolicyNewsSource {
    const MAX_BYTES: usize = 2 * 1024 * 1024;
    const MAX_ENTRIES: usize = 1000;

    pub fn default_endpoint(provider: &DatafeedProvider) -> Result<&'static str> {
        match provider {
            DatafeedProvider::FED => Ok("https://www.federalreserve.gov/feeds/press_monetary.xml"),
            DatafeedProvider::TREASURY => Ok("https://www.treasurydirect.gov/TA_WS/securities/announced/rss"),
            _ => anyhow::bail!("unsupported policy news provider"),
        }
    }

    pub fn new(
        provider: &DatafeedProvider,
        endpoint: Option<String>,
        interval: Duration,
        traceparent: String,
    ) -> Result<Self> {
        let default_endpoint = Self::default_endpoint(provider)?;
        let endpoint = endpoint.unwrap_or_else(|| default_endpoint.to_owned());
        let url = url::Url::parse(&endpoint)?;
        ensure!(
            url.scheme() == "https" && url.username().is_empty() && url.password().is_none(),
            "policy source requires HTTPS without URL credentials"
        );
        ensure!(
            (60..=3600).contains(&interval.as_secs()),
            "policy polling interval must be 60–3600 seconds"
        );
        TraceContext::parse(&traceparent)?;
        Ok(Self {
            source: provider.as_str().to_ascii_lowercase(),
            endpoint,
            traceparent,
            interval,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("Sigbot-PolicyFeed/2")
                .build()?,
        })
    }

    pub async fn fetch(&self) -> Result<Vec<DatafeedEvent>> {
        let span = MessageTrace::child("datafeed.policy.fetch", &self.traceparent);
        let mut response = self
            .client
            .get(&self.endpoint)
            .header("traceparent", span.traceparent())
            .header(
                "accept",
                "application/rss+xml, application/atom+xml, text/xml, application/xml",
            )
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            response.content_length().unwrap_or(0) <= Self::MAX_BYTES as u64,
            "policy feed exceeds byte limit"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= Self::MAX_BYTES,
                "policy feed exceeds byte limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        Self::parse(&self.source, &self.endpoint, &bytes, Utc::now())
    }

    pub fn parse(
        source: &str,
        endpoint: &str,
        bytes: &[u8],
        available_time: DateTime<Utc>,
    ) -> Result<Vec<DatafeedEvent>> {
        ensure!(matches!(source, "fed" | "treasury"), "unknown policy source");
        ensure!(bytes.len() <= Self::MAX_BYTES, "policy feed exceeds byte limit");
        // Do not accept the parser's random fallback ID when both GUID and links are missing.
        let feed = feed_rs::parser::Builder::new()
            .base_uri(Some(endpoint))
            .id_generator(|links, _, _| links.first().map(|link| link.href.clone()).unwrap_or_default())
            .build()
            .parse(bytes)?;
        ensure!(
            feed.entries.len() <= Self::MAX_ENTRIES,
            "policy feed exceeds entry limit"
        );
        let mut events = Vec::with_capacity(feed.entries.len());
        for entry in feed.entries {
            ensure!(
                !entry.id.is_empty() && entry.id.len() <= 2048,
                "policy source ID missing or too large"
            );
            let event_time = entry
                .updated
                .or(entry.published)
                .context("policy item has no published/updated timestamp")?;
            ensure!(
                event_time <= available_time && entry.published.is_none_or(|time| time <= available_time),
                "future policy item rejected"
            );
            let seq = event_time.timestamp_micros();
            ensure!(seq >= 0, "policy timestamp precedes supported epoch");
            let title = entry
                .title
                .as_ref()
                .map(|text| text.content.as_str())
                .unwrap_or_default();
            let body = entry
                .content
                .as_ref()
                .and_then(|content| content.body.as_deref())
                .or_else(|| entry.summary.as_ref().map(|text| text.content.as_str()))
                .unwrap_or_default();
            let content = html2text::from_read(format!("{title}\n{body}").as_bytes(), 4096)?
                .trim()
                .to_owned();
            ensure!(
                !content.is_empty() && content.len() <= 65_536,
                "policy content missing or too large"
            );
            let url = entry
                .links
                .iter()
                .find(|link| link.rel.as_deref().is_none_or(|rel| rel == "alternate"))
                .map(|link| link.href.as_str());
            let raw = json!({"title":title,"content":content,"url":url,"feed_url":endpoint,"source_payload":entry});
            events.push(DatafeedEvent {
                source: source.to_owned(),
                source_event_id: entry.id.clone(),
                symbol: "*".to_owned(),
                event_type: "news".to_owned(),
                event_time,
                available_time,
                seq,
                revision: 0,
                content: Some(content),
                raw,
            });
        }
        // A timestamp can be shared by several facts; stable source IDs break ties.
        events.sort_by(|a, b| (a.event_time, &a.source_event_id).cmp(&(b.event_time, &b.source_event_id)));
        Ok(events)
    }

    async fn deliver(&self, handler: &FeedHandler) -> Result<()> {
        for event in self.fetch().await? {
            handler(serde_json::to_vec(&event)?).await?;
        }
        Ok(())
    }
}

pub struct SigbotPolicyDatafeedClient {
    provider: DatafeedProvider,
    source: Mutex<Option<PolicyNewsSource>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SigbotPolicyDatafeedClient {
    pub fn new(provider: DatafeedProvider) -> Arc<Self> {
        Arc::new(Self {
            provider,
            source: Mutex::new(None),
            task: Mutex::new(None),
        })
    }
}

#[async_trait]
impl ISigbotDatafeedClient for SigbotPolicyDatafeedClient {
    fn provider(&self) -> DatafeedProvider {
        self.provider.clone()
    }

    async fn init(&self, argument: Arc<SigbotDatefeedArgument>) -> Result<()> {
        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .context("policy feed properties required")?;
        let prefix = self.provider.as_str().to_ascii_lowercase();
        let property = |key: &str| {
            properties
                .get(&format!("{prefix}_{key}"))
                .or_else(|| properties.get(key))
        };
        let interval = property("poll_interval_ms")
            .map(String::as_str)
            .unwrap_or("300000")
            .parse::<u64>()?;
        let traceparent = property("traceparent").context("policy traceparent required")?.clone();
        *self.source.lock().await = Some(PolicyNewsSource::new(
            &self.provider,
            property("endpoint").cloned(),
            Duration::from_millis(interval),
            traceparent,
        )?);
        Ok(())
    }

    async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        let source = self
            .source
            .lock()
            .await
            .clone()
            .context("policy feed is not initialized")?;
        let mut task = self.task.lock().await;
        ensure!(task.is_none(), "policy feed already subscribed");
        source.deliver(&handler).await?;
        *task = Some(tokio::spawn(async move {
            let mut delay = source.interval;
            loop {
                tokio::time::sleep(delay).await;
                match source.deliver(&handler).await {
                    Ok(()) => delay = source.interval,
                    Err(error) => {
                        tracing::warn!(source=%source.source, %error, "policy source failed; no checkpoint advanced");
                        delay = (delay * 2).min(Duration::from_secs(3600));
                    }
                }
            }
        }));
        Ok(())
    }

    async fn close(&self) {
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
