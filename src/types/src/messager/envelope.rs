// SPDX-License-Identifier: GPL-3.0-only
//! Wire pairing is deliberately absent from the durable representation.

use super::topic::MessageScope;

use super::topic::TopicAddress;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

pub struct MessageIdentity;

impl MessageIdentity {
    pub fn validate(existing_topic: String, mut existing: Value, topic: &str, mut proposed: Value) -> Result<()> {
        for object in [&mut existing, &mut proposed]
            .into_iter()
            .filter_map(Value::as_object_mut)
        {
            for field in ["attempt_no", "traceparent", "produced_at"] {
                object.remove(field);
            }
        }
        ensure!(
            existing_topic == topic && existing == proposed,
            "business identity reused with a different message"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Event,
    Request,
    Result,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageEnvelope<T> {
    #[serde(flatten)]
    pub durable: DurableMessage<T>,
    /// Short-lived request/reply pairing only. Never put this wire DTO in a database.
    #[serde(rename = "requestSpanId", default, skip_serializing_if = "Option::is_none")]
    pub request_span_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableMessage<T> {
    pub version: u16,
    pub scope: MessageScope,
    pub workflow_id: Option<Uuid>,
    pub account_id: Option<Uuid>,
    pub node_id: i64,
    pub target_node_id: Option<i64>,
    pub run_id: Uuid,
    pub seq: i64,
    pub attempt_no: u32,
    pub kind: MessageKind,
    pub event_time: DateTime<Utc>,
    pub available_time: DateTime<Utc>,
    pub produced_at: DateTime<Utc>,
    pub traceparent: String,
    pub payload: T,
}

impl<T> DurableMessage<T> {
    pub fn map_payload<U>(self, transform: impl FnOnce(T) -> U) -> DurableMessage<U> {
        DurableMessage {
            version: self.version,
            scope: self.scope,
            workflow_id: self.workflow_id,
            account_id: self.account_id,
            node_id: self.node_id,
            target_node_id: self.target_node_id,
            run_id: self.run_id,
            seq: self.seq,
            attempt_no: self.attempt_no,
            kind: self.kind,
            event_time: self.event_time,
            available_time: self.available_time,
            produced_at: self.produced_at,
            traceparent: self.traceparent,
            payload: transform(self.payload),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        ensure!(self.version == 2, "unsupported envelope version");
        ensure!(self.run_id.get_version_num() == 7, "run_id must be UUIDv7");
        ensure!(self.node_id > 0 && self.seq >= 0, "invalid node_id or seq");
        ensure!(
            self.workflow_id.is_some() || self.account_id.is_some(),
            "message needs workflow or account"
        );
        ensure!(
            self.workflow_id.is_none_or(|id| id.get_version_num() == 7)
                && self.account_id.is_none_or(|id| id.get_version_num() == 7),
            "invalid resource ID"
        );
        ensure!(self.target_node_id.is_none_or(|id| id > 0), "invalid target node");
        ensure!(
            self.event_time <= self.available_time && self.available_time <= self.produced_at,
            "event/available/produced timestamps are inconsistent"
        );
        TraceContext::parse(&self.traceparent)?;
        Ok(())
    }

    pub fn validate_topic(&self, topic: &str) -> Result<()> {
        self.validate()?;
        let address = TopicAddress::parse(topic)?;
        ensure!(
            address.scope() == &self.scope,
            "topic/envelope isolation scope mismatch"
        );
        match address {
            TopicAddress::Workflow { workflow_id, .. } => {
                ensure!(self.workflow_id == Some(workflow_id), "workflow mismatch")
            }
            TopicAddress::Account { account_id, .. } => {
                ensure!(self.account_id == Some(account_id), "account mismatch")
            }
        }
        Ok(())
    }

    /// A retry preserves business identity and uses the caller's new OTel producer span.
    pub fn retry(&mut self, traceparent: String, now: DateTime<Utc>) -> Result<()> {
        let previous = TraceContext::parse(&self.traceparent)?;
        let next = TraceContext::parse(&traceparent)?;
        ensure!(previous.span_id != next.span_id, "retry requires a new producer span");
        ensure!(now >= self.produced_at, "retry clock cannot move backwards");
        self.attempt_no = self.attempt_no.checked_add(1).context("attempt_no overflow")?;
        self.traceparent = traceparent;
        self.produced_at = now;
        Ok(())
    }
}

impl<T> MessageEnvelope<T> {
    pub fn validate(&self) -> Result<()> {
        self.durable.validate()?;
        if let Some(span) = &self.request_span_id {
            TraceContext::validate_hex(span, 16)?;
        }
        match self.durable.kind {
            MessageKind::Request => ensure!(
                self.request_span_id.as_deref() == Some(TraceContext::parse(&self.durable.traceparent)?.span_id),
                "requestSpanId must equal the producer span_id"
            ),
            MessageKind::Event => ensure!(self.request_span_id.is_none(), "events cannot carry requestSpanId"),
            MessageKind::Result => {} // A replay may no longer have the ephemeral pairing cache.
        }
        Ok(())
    }

    pub fn from_durable(durable: DurableMessage<T>) -> Result<Self> {
        let request_span_id = match durable.kind {
            MessageKind::Request => Some(TraceContext::parse(&durable.traceparent)?.span_id.to_owned()),
            _ => None,
        };
        let result = Self {
            durable,
            request_span_id,
        };
        result.validate()?;
        Ok(result)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TraceContext<'a> {
    pub trace_id: &'a str,
    pub span_id: &'a str,
    pub flags: &'a str,
}

impl<'a> TraceContext<'a> {
    pub fn parse(value: &'a str) -> Result<Self> {
        let parts: Vec<_> = value.split('-').collect();
        ensure!(
            parts.len() == 4 && parts[0] == "00",
            "invalid W3C traceparent version/format"
        );
        Self::validate_hex(parts[1], 32)?;
        Self::validate_hex(parts[2], 16)?;
        ensure!(
            parts[3].len() == 2
                && parts[3]
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "invalid trace flags"
        );
        Ok(Self {
            trace_id: parts[1],
            span_id: parts[2],
            flags: parts[3],
        })
    }

    fn validate_hex(value: &str, len: usize) -> Result<()> {
        ensure!(
            value.len() == len
                && value.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
                && value.bytes().any(|c| c != b'0'),
            "invalid W3C trace identifier"
        );
        Ok(())
    }
}
