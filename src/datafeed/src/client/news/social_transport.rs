// SPDX-License-Identifier: GPL-3.0-only
//! Bounded external transport. MCP lifecycle and framing belong to the official SDK.
use anyhow::{bail, ensure, Context, Result};
use rmcp::{
    model::CallToolRequestParams,
    transport::{streamable_http_client::StreamableHttpClientTransportConfig, StreamableHttpClientTransport},
    ServiceExt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};

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
