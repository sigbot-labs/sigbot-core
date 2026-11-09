// SPDX-License-Identifier: GPL-3.0-only

use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::config::config::DatafeedProperties;
pub use sigbot_types::datafeed::event::DatafeedEvent as NewsEvent;
use sigbot_types::messager::envelope::TraceContext;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

pub type FeedHandler = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>> + Send + Sync>;

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

    pub async fn configure(&self, argument: Arc<DatafeedProperties>) -> Result<()> {
        let properties = argument.properties.as_ref().context("datafeed properties required")?;
        let endpoint = Self::property(properties, self.provider, "endpoint")?;
        let mode = Self::property(properties, self.provider, "transport").unwrap_or_else(|_| "rest".to_owned());
        let mode: SocialTransportMode = serde_json::from_value(Value::String(mode))?;
        let account_id = Self::property(properties, self.provider, "account_id")?;
        ensure!(
            account_id.bytes().all(|c| c.is_ascii_digit()),
            "source account_id must be numeric"
        );
        let token_secret = Self::property(properties, self.provider, "token_secret")?;
        let token = get_config().secrets.require(&token_secret)?.to_owned();
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
        TraceContext::parse(&traceparent)?;
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SocialTransportMode {
    Rest,
    Mcp,
    A2a,
}

#[derive(Clone)]
pub struct SocialTransport {
    pub endpoint: String,
    pub mode: SocialTransportMode,
    pub account_id: String,
    pub provider: String,
    pub token: String,
    pub timeout: Duration,
    client: reqwest::Client,
}

impl SocialTransport {
    pub fn new(
        endpoint: String,
        mode: SocialTransportMode,
        account_id: String,
        provider: String,
        token: String,
        timeout: Duration,
    ) -> Result<Self> {
        let url = url::Url::parse(&endpoint)?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "unsupported social endpoint scheme"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "credentials cannot appear in endpoint"
        );
        ensure!(!token.is_empty(), "social datafeed requires an API token");
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            mode,
            account_id,
            provider,
            token,
            timeout,
            client,
        })
    }

    pub async fn fetch(&self, since_id: &str, cursor: &str, traceparent: &str, run_id: &str) -> Result<Value> {
        let span = common_telemetry::tracing_context::MessageTrace::child("datafeed.fetch", traceparent);
        let traceparent = span.traceparent();
        match self.mode {
            SocialTransportMode::Rest => self.rest(since_id, cursor, &traceparent).await,
            SocialTransportMode::Mcp => tokio::time::timeout(self.timeout, self.mcp(since_id, cursor, &traceparent))
                .await
                .context("MCP timeline timeout")?,
            SocialTransportMode::A2a => self.a2a(since_id, cursor, &traceparent, run_id).await,
        }
    }

    async fn rest(&self, since_id: &str, cursor: &str, traceparent: &str) -> Result<Value> {
        let path = match self.provider.as_str() {
            "twitter" => format!("/2/users/{}/tweets", self.account_id),
            "truthsocial" => format!("/api/v1/accounts/{}/statuses", self.account_id),
            _ => bail!("unsupported news source"),
        };
        let mut query = if self.provider == "twitter" {
            vec![("max_results", "100")]
        } else {
            vec![("limit", "40")]
        };
        if since_id != "0" {
            query.push(("since_id", since_id));
        }
        if cursor != "0" {
            query.push((
                if self.provider == "twitter" {
                    "pagination_token"
                } else {
                    "max_id"
                },
                cursor,
            ));
        }
        if self.provider == "twitter" {
            query.push(("tweet.fields", "created_at,author_id,text"));
        }
        let response = self
            .client
            .get(format!("{}{path}", self.endpoint))
            .query(&query)
            .bearer_auth(&self.token)
            .header("traceparent", traceparent)
            .send()
            .await?;
        if self.provider == "twitter" {
            return Self::decode(response).await;
        }
        let mut next_token = None;
        for header in response.headers().get_all(reqwest::header::LINK) {
            let links = parse_link_header::parse_with_rel(header.to_str()?)?;
            if let Some(next) = links.get("next") {
                let endpoint = url::Url::parse(&self.endpoint)?;
                let next_url = endpoint.join(&next.raw_uri)?;
                ensure!(
                    next_url.origin() == endpoint.origin() && next_url.path() == path,
                    "timeline pagination escaped its source endpoint"
                );
                let next = next
                    .queries
                    .get("max_id")
                    .context("timeline next link missing max_id")?;
                ensure!(next.parse::<u64>().is_ok() && next != "0", "invalid timeline max_id");
                next_token = Some(next.clone());
            }
        }
        let posts = Self::decode(response).await?;
        ensure!(posts.is_array(), "Mastodon-compatible timeline must return an array");
        Ok(json!({"data": posts, "next_token": next_token}))
    }

    async fn mcp(&self, since_id: &str, cursor: &str, traceparent: &str) -> Result<Value> {
        let mut headers = HashMap::new();
        headers.insert(http::HeaderName::from_static("traceparent"), traceparent.parse()?);
        let config = StreamableHttpClientTransportConfig::with_uri(format!("{}/mcp", self.endpoint))
            .auth_header(self.token.clone())
            .custom_headers(headers)
            .max_sse_event_size(1_048_576);
        let transport = StreamableHttpClientTransport::from_config(config);
        let client = ().serve(transport).await?;
        let response = client
            .call_tool(
                CallToolRequestParams::new("timeline").with_arguments(
                    json!({"since_id": since_id, "limit": 100, "cursor": cursor})
                        .as_object()
                        .context("invalid MCP query")?
                        .clone(),
                ),
            )
            .await;
        let _ = client.cancel().await;
        let response = response?;
        ensure!(response.is_error != Some(true), "MCP timeline returned a tool error");
        if let Some(value) = response.structured_content {
            return Ok(value);
        }
        let text = response
            .content
            .iter()
            .find_map(|item| item.as_text())
            .context("MCP timeline has no JSON result")?;
        Ok(serde_json::from_str(&text.text)?)
    }

    async fn a2a(&self, since_id: &str, cursor: &str, traceparent: &str, run_id: &str) -> Result<Value> {
        // Bounded A2A 0.3 message/send adapter. The external gateway uses the official SDK;
        // it must return a Message, never an unbounded asynchronous Task.
        let query = json!({"since_id": since_id, "limit": 100, "cursor": cursor});
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": "message/send", "params": {
            "message": {"kind": "message", "role": "user", "messageId": run_id,
                "parts": [{"kind": "text", "text": query.to_string()}]},
            "configuration": {"blocking": true, "acceptedOutputModes": ["text/plain"]}}});
        let response = self
            .client
            .post(format!("{}/a2a/", self.endpoint))
            .bearer_auth(&self.token)
            .header("traceparent", traceparent)
            .json(&body)
            .send()
            .await?;
        let response = Self::decode(response).await?;
        ensure!(
            response.get("error").is_none(),
            "A2A timeline returned a protocol error"
        );
        let result = response.get("result").context("A2A missing result")?;
        ensure!(
            result.get("kind").and_then(Value::as_str) == Some("message"),
            "A2A timeline must return a bounded Message"
        );
        let parts = result
            .get("parts")
            .and_then(Value::as_array)
            .context("A2A missing parts")?;
        let text = parts
            .iter()
            .find_map(|part| part.get("text").and_then(Value::as_str))
            .context("A2A timeline missing text result")?;
        Ok(serde_json::from_str(text)?)
    }

    async fn decode(response: reqwest::Response) -> Result<Value> {
        let mut response = response.error_for_status()?;
        ensure!(
            response.content_length().unwrap_or(0) <= 1_048_576,
            "timeline response too large"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= 1_048_576,
                "response exceeds bounded buffer"
            );
            bytes.extend_from_slice(&chunk);
        }
        ensure!(bytes.len() <= 1_048_576, "timeline response too large");
        Ok(serde_json::from_slice(&bytes)?)
    }
}
