// SPDX-License-Identifier: GPL-3.0-only
//! Social source lifecycle and normalization; persistence is owned by the ingestor callback.
use super::social_transport::{SocialTransport, SocialTransportMode};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::modules::datafeed::SigbotDatefeedArgument;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle};

pub type FeedHandler = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>> + Send + Sync>;

pub use sigbot_types::modules::datafeed::event::DatafeedEvent as NewsEvent;

pub struct NewsNormalizer;

impl NewsNormalizer {
    pub fn from_post(source: &str, post: Value, available_time: DateTime<Utc>) -> Result<NewsEvent> {
        let id = post
            .get("id")
            .and_then(Value::as_str)
            .context("news source ID missing")?
            .to_owned();
        let seq = id
            .parse::<i64>()
            .context("news source ID must fit positive 64-bit sequence")?;
        ensure!(seq > 0, "invalid news source sequence");
        let event_time = DateTime::parse_from_rfc3339(
            post.get("created_at")
                .and_then(Value::as_str)
                .context("news event time missing")?,
        )?
        .with_timezone(&Utc);
        ensure!(
            event_time <= available_time,
            "news cannot become available before its event time"
        );
        let content = if source == "truthsocial" {
            html2text::from_read(
                post.get("content")
                    .and_then(Value::as_str)
                    .context("news HTML content missing")?
                    .as_bytes(),
                4096,
            )?
            .trim()
            .to_owned()
        } else {
            post.get("text")
                .and_then(Value::as_str)
                .context("news text missing")?
                .to_owned()
        };
        ensure!(
            !content.is_empty() && content.len() <= 65_536,
            "news content invalid or too large"
        );
        Ok(NewsEvent {
            source: source.to_owned(),
            source_event_id: id,
            symbol: "*".to_owned(),
            event_type: "news".to_owned(),
            event_time,
            available_time,
            seq,
            revision: 0,
            content: Some(content),
            raw: post,
        })
    }
}

#[derive(Clone)]
struct FeedSession {
    transport: SocialTransport,
    poll_interval: Duration,
    traceparent: String,
    run_id: String,
}

pub struct SocialFeed {
    provider: &'static str,
    session: Mutex<Option<FeedSession>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SocialFeed {
    pub fn new(provider: &'static str) -> Self {
        Self {
            provider,
            session: Mutex::new(None),
            task: Mutex::new(None),
        }
    }

    pub async fn init(&self, argument: Arc<SigbotDatefeedArgument>) -> Result<()> {
        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .context("datafeed properties required")?;
        let endpoint = Self::property(properties, self.provider, "endpoint")?;
        let mode = Self::property(properties, self.provider, "transport").unwrap_or_else(|_| "rest".to_owned());
        let mode: SocialTransportMode = serde_json::from_value(Value::String(mode))?;
        let account_id = Self::property(properties, self.provider, "account_id")?;
        ensure!(
            account_id.bytes().all(|c| c.is_ascii_digit()),
            "source account_id must be numeric"
        );
        let token_env = Self::property(properties, self.provider, "token_env")?;
        let token = std::env::var(token_env).context("datafeed API token environment variable missing")?;
        let timeout = Self::property(properties, self.provider, "timeout_seconds")
            .unwrap_or_else(|_| "15".to_owned())
            .parse::<u64>()?;
        let interval = Self::property(properties, self.provider, "poll_interval_ms")
            .unwrap_or_else(|_| "1000".to_owned())
            .parse::<u64>()?;
        ensure!(
            (1..=120).contains(&timeout) && (100..=300_000).contains(&interval),
            "invalid feed timeout or polling interval"
        );
        let traceparent = Self::property(properties, self.provider, "traceparent")?;
        sigbot_types::modules::messager::envelope::TraceContext::parse(&traceparent)?;
        let run_id = Self::property(properties, self.provider, "run_id")?;
        *self.session.lock().await = Some(FeedSession {
            transport: SocialTransport::new(
                endpoint,
                mode,
                account_id,
                self.provider.to_owned(),
                token,
                Duration::from_secs(timeout),
            )?,
            poll_interval: Duration::from_millis(interval),
            traceparent,
            run_id,
        });
        Ok(())
    }

    pub async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        let session = self
            .session
            .lock()
            .await
            .clone()
            .context("social source is not initialized")?;
        let mut task = self.task.lock().await;
        ensure!(task.is_none(), "social source already subscribed");
        // Validate source access before marking startup successful.
        let first = session
            .transport
            .fetch("0", "0", &session.traceparent, &session.run_id)
            .await?;
        *task = Some(tokio::spawn(Self::poll(session, first, handler)));
        Ok(())
    }

    async fn poll(session: FeedSession, first: Value, handler: FeedHandler) {
        let mut initial = Some(first);
        let mut delay = session.poll_interval;
        loop {
            let result = Self::poll_once(&session, initial.take(), &handler).await;
            if let Err(error) = result {
                tracing::warn!(source = %session.transport.provider, error = %error, "social poll failed; retrying without advancing checkpoint");
                delay = (delay * 2).min(Duration::from_secs(30));
            } else {
                delay = session.poll_interval;
            }
            tokio::time::sleep(delay).await;
        }
    }

    async fn poll_once(session: &FeedSession, first: Option<Value>, handler: &FeedHandler) -> Result<()> {
        // Replay the bounded source window after restart/poll. Durable source keys deduplicate it
        // in the ingestor transaction; late arrivals are not dropped by a high-water ID filter.
        let mut cursor = "0".to_owned();
        let mut initial = first;
        for _ in 0..100 {
            let value = match initial.take() {
                Some(value) => value,
                None => {
                    session
                        .transport
                        .fetch("0", &cursor, &session.traceparent, &session.run_id)
                        .await?
                }
            };
            let posts = value
                .as_array()
                .or_else(|| value.get("data").and_then(Value::as_array))
                .context("timeline must return a data array")?;
            ensure!(posts.len() <= 100, "timeline page exceeds 100 posts");
            for post in posts {
                let event = NewsNormalizer::from_post(&session.transport.provider, post.clone(), Utc::now())?;
                handler(serde_json::to_vec(&event)?).await?;
            }
            let next = value
                .get("next_token")
                .or_else(|| value.pointer("/meta/next_token"))
                .and_then(Value::as_str);
            match next {
                Some(next) => {
                    ensure!(next != cursor, "timeline pagination did not progress");
                    cursor = next.to_owned();
                }
                None => return Ok(()),
            }
        }
        anyhow::bail!("timeline exceeded bounded 100-page history window")
    }

    pub async fn close(&self) {
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }

    fn property(properties: &HashMap<String, String>, provider: &str, key: &str) -> Result<String> {
        properties
            .get(&format!("{provider}_{key}"))
            .or_else(|| properties.get(key))
            .cloned()
            .with_context(|| format!("missing {provider} configuration key {key}"))
    }
}
