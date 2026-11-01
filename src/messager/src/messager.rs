// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Error, Result};
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use sigbot_core::modules::messager::handler::messager::MessagerHandler;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind, TraceContext};
use sigbot_types::messager::messager::MessagerProvider;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

pub type MessageHandler =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>;

#[async_trait]
pub trait IMessager: IProvider {
    fn provider(&self) -> MessagerProvider;

    async fn publish(&self, to: &str, message: &str) -> Result<String, Error>;
    /// Broker durability boundary for an outbox relay. Enqueuing locally is insufficient.
    async fn publish_confirmed(&self, _topic: &str, _message: &str) -> Result<(), Error> {
        Err(Error::msg(
            "this transport does not provide durable publish confirmation",
        ))
    }
    async fn subscribe(
        &self,
        topic: &str,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error>;

    /// Registers one named consumer without replacing other consumers of the same topic.
    async fn subscribe_scoped(&self, topic: &str, consumer: &str, handler: MessageHandler) -> Result<(), Error> {
        let _ = consumer;
        self.subscribe(topic, handler).await
    }

    async fn unsubscribe_scoped(&self, _topic: &str, _consumer: &str) -> Result<(), Error> {
        Err(Error::msg("this transport does not support scoped unsubscribe"))
    }
}

#[derive(Default)]
pub struct SubscriptionSet {
    client: OnceLock<Arc<dyn IMessager>>,
    entries: tokio::sync::Mutex<Vec<(String, String)>>,
}

impl SubscriptionSet {
    pub fn bind(&self, client: Arc<dyn IMessager>) -> Result<()> {
        ensure!(self.client.set(client).is_ok(), "message subscriptions already bound");
        Ok(())
    }

    pub async fn subscribe(&self, topic: String, consumer: String, handler: MessageHandler) -> Result<()> {
        let client = self
            .client
            .get()
            .ok_or_else(|| Error::msg("message subscriptions are not bound"))?;
        client.subscribe_scoped(&topic, &consumer, handler).await?;
        self.entries.lock().await.push((topic, consumer));
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let Some(client) = self.client.get() else {
            return Ok(());
        };
        let mut first = None;
        let entries = self.entries.lock().await.drain(..).rev().collect::<Vec<_>>();
        for (topic, consumer) in entries {
            if let Err(error) = client.unsubscribe_scoped(&topic, &consumer).await {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
}

/// A scoped host task, never an independently deployed service.
pub struct OutboxRelay {
    task: JoinHandle<()>,
}

impl OutboxRelay {
    /// Bounded publish batches keep the Core row lock through PUBACK and outcome commit.
    pub async fn dispatch_pending(
        handler: &MessagerHandler,
        producer: &str,
        client: &dyn IMessager,
        limit: u32,
        next_traceparent: &(dyn Fn(&str) -> Result<String> + Send + Sync),
    ) -> Result<usize> {
        ensure!((1..=256).contains(&limit), "relay batch must be between 1 and 256");
        let mut sent = 0;
        for _ in 0..limit {
            let Some(mut pending) = handler.claim(producer).await? else {
                break;
            };
            let attempt = pending.attempt();
            let topic = pending.topic().to_owned();
            let durable = pending.message_mut();
            if attempt > durable.attempt_no as i32 {
                durable.attempt_no = (attempt - 1).try_into()?;
                durable.retry(next_traceparent(&durable.traceparent)?, chrono::Utc::now())?;
            }
            durable.validate_topic(&topic)?;
            let mut envelope = MessageEnvelope::from_durable(durable.clone())?;
            if durable.kind == MessageKind::Result {
                envelope.request_span_id = ReplyPairingCache::shared().get(durable);
            }
            let result = client
                .publish_confirmed(&topic, &serde_json::to_string(&envelope)?)
                .await;
            pending.complete(result.is_ok()).await?;
            result?;
            sent += 1;
        }
        Ok(sent)
    }

    pub fn start(handler: MessagerHandler, producer: String, client: Arc<dyn IMessager>) -> Self {
        let readiness = common_telemetry::readiness::RuntimeReadiness::task(format!("outbox:{producer}"));
        let task = tokio::spawn(async move {
            readiness.set(true);
            loop {
                let result = Self::dispatch_pending(&handler, &producer, client.as_ref(), 32, &|parent| {
                    Ok(MessageTrace::child("messager.retry", parent).traceparent())
                })
                .await;
                if let Err(error) = result {
                    common_telemetry::warn!("outbox relay will retry after durable failure: {}", error);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        Self { task }
    }

    pub fn close(&self) {
        self.task.abort();
    }
}

impl Drop for OutboxRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReplyKey {
    scope: MessageScope,
    node_id: i64,
    run_id: Uuid,
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
            run_id: message.run_id,
            seq: message.seq,
        }
    }
}
