// SPDX-License-Identifier: GPL-3.0-only
//! This bounded cache is intentionally lost on restart. It is not recovery state.
use anyhow::{ensure, Result};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageKind, TraceContext},
    topic::MessageScope,
};
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReplyKey {
    scope: MessageScope,
    node_id: i64,
    run_id: String,
    seq: i64,
}

pub struct ReplyPairingCache {
    capacity: usize,
    ttl: Duration,
    replies: Mutex<HashMap<ReplyKey, (String, Instant)>>,
}

impl ReplyPairingCache {
    pub fn new(capacity: usize, ttl: Duration) -> Result<Self> {
        ensure!(
            (1..=65_536).contains(&capacity) && !ttl.is_zero() && ttl <= Duration::from_secs(300),
            "invalid reply-cache bounds"
        );
        Ok(Self {
            capacity,
            ttl,
            replies: Mutex::new(HashMap::new()),
        })
    }

    pub fn shared() -> &'static Self {
        static CACHE: OnceLock<ReplyPairingCache> = OnceLock::new();
        CACHE.get_or_init(|| Self::new(4096, Duration::from_secs(60)).expect("fixed valid reply cache bounds"))
    }

    pub fn remember<T>(&self, response: &DurableMessage<T>, request_span_id: &str) -> Result<()> {
        response.validate()?;
        ensure!(
            response.kind == MessageKind::Result,
            "only results have a reply pairing"
        );
        TraceContext::parse(&format!("00-11111111111111111111111111111111-{}-01", request_span_id))?;
        let mut replies = self
            .replies
            .lock()
            .map_err(|_| anyhow::anyhow!("reply cache poisoned"))?;
        replies.retain(|_, (_, expires)| *expires > Instant::now());
        let key = Self::key(response);
        if replies.len() == self.capacity && !replies.contains_key(&key) {
            if let Some(oldest) = replies
                .iter()
                .min_by_key(|(_, (_, expires))| *expires)
                .map(|(key, _)| key.clone())
            {
                replies.remove(&oldest);
            }
        }
        replies.insert(key, (request_span_id.to_owned(), Instant::now() + self.ttl));
        Ok(())
    }

    pub fn get<T>(&self, response: &DurableMessage<T>) -> Option<String> {
        if response.kind != MessageKind::Result {
            return None;
        }
        let mut replies = self.replies.lock().ok()?;
        replies.retain(|_, (_, expires)| *expires > Instant::now());
        replies.get(&Self::key(response)).map(|(span, _)| span.clone())
    }

    fn key<T>(message: &DurableMessage<T>) -> ReplyKey {
        ReplyKey {
            scope: message.scope.clone(),
            node_id: message.node_id,
            run_id: message.run_id.clone(),
            seq: message.seq,
        }
    }
}
