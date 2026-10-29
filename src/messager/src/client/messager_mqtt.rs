// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::client::messager_factory::{ISigbotMessagerClient, MessageHandler};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use common_telemetry::{info, warn};
use rumqttc::v5::{
    mqttbytes::{
        v5::{ConnectReturnCode, Packet, PubAckReason, Publish, SubscribeReasonCode},
        QoS,
    },
    AsyncClient, Event, EventLoop, MqttOptions,
};
use rumqttc::{mqttbytes::matches, Outgoing};
use sigbot_types::modules::messager::messager::{MessagerConfiguration, MessagerProvider};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};

#[derive(Clone)]
pub struct SigbotMqttMessagerClientConfig {
    pub mqtt_server: String,
    pub mqtt_port: u16,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub mqtt_client_id: Option<String>,
    pub mqtt_timeout: Duration,
    pub mqtt_clean_start: bool,
    pub mqtt_qos: u8,
    pub mqtt_retain: bool,
}

impl std::fmt::Display for SigbotMqttMessagerClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mqtt_server={:?}, mqtt_port={:?}, mqtt_username={:?}, mqtt_password_len={:?}, mqtt_client_id={:?}, mqtt_timeout={:?}, mqtt_clean_start={:?}, mqtt_qos={:?}, mqtt_retain={:?})",
            self.mqtt_server,
            self.mqtt_port,
            self.mqtt_username,
            self.mqtt_password.chars().count(),
            self.mqtt_client_id,
            self.mqtt_timeout,
            self.mqtt_clean_start,
            self.mqtt_qos,
            self.mqtt_retain,
        )
    }
}

impl SigbotMqttMessagerClientConfig {
    pub fn from_config(configuration: Arc<MessagerConfiguration>) -> Self {
        let plain_config = configuration
            .properties
            .as_ref()
            .expect("Plain configuration is required");
        let secret_config = configuration
            .secrets
            .as_ref()
            .expect("Secret configuration is required");
        Self {
            mqtt_server: plain_config
                .get("mqtt_server")
                .expect("MQTT server is required")
                .to_string(),
            mqtt_port: plain_config
                .get("mqtt_port")
                .expect("MQTT port is required")
                .to_string()
                .parse::<u16>()
                .expect("MQTT port must be a valid number"),
            mqtt_username: plain_config
                .get("mqtt_username")
                .expect("MQTT username is required")
                .to_string(),
            mqtt_password: if let Some(name) = secret_config.get("mqtt_password_env") {
                std::env::var(name).expect("MQTT password environment variable is missing")
            } else {
                secret_config
                    .get("mqtt_password")
                    .expect("MQTT password environment reference is required")
                    .to_string()
            },
            mqtt_client_id: plain_config.get("mqtt_client_id").map(|s| s.to_string()),
            mqtt_clean_start: plain_config
                .get("mqtt_clean_start")
                .map(|value| value.parse::<bool>().expect("mqtt_clean_start must be boolean"))
                .unwrap_or(false),
            mqtt_qos: plain_config
                .get("mqtt_qos")
                .map(|value| value.parse::<u8>().expect("mqtt_qos must be numeric"))
                .unwrap_or(1),
            mqtt_retain: plain_config
                .get("mqtt_retain")
                .map(|value| value.parse::<bool>().expect("mqtt_retain must be boolean"))
                .unwrap_or(false),
            mqtt_timeout: Duration::from_secs(
                plain_config
                    .get("mqtt_timeout")
                    .expect("MQTT timeout is required")
                    .parse::<u64>()
                    .expect("MQTT timeout must be a valid number"),
            ),
        }
    }
}

type Confirmation = oneshot::Sender<Result<()>>;

#[derive(Default)]
struct PublishConfirmations {
    queued: VecDeque<Option<Confirmation>>,
    inflight: HashMap<u16, Option<Confirmation>>,
}

/// One bounded dispatcher per persistent MQTT session. Handler success means the
/// consumer has committed its inbox/domain transaction; only then is PUBACK sent.
/// Handlers enqueue their outgoing work transactionally, never wait for replies.
pub struct SigbotMqttMessagerClient {
    config: Arc<SigbotMqttMessagerClientConfig>,
    client: Mutex<Option<AsyncClient>>,
    subscriptions: Arc<Mutex<HashMap<String, MessageHandler>>>,
    confirmations: Arc<Mutex<PublishConfirmations>>,
    subscription_confirmations: Arc<Mutex<PublishConfirmations>>,
    subscription_lock: Arc<Mutex<()>>,
    publish_lock: Mutex<()>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl SigbotMqttMessagerClient {
    pub async fn new(config: Arc<SigbotMqttMessagerClientConfig>) -> Arc<Self> {
        Arc::new(Self {
            config,
            client: Mutex::new(None),
            subscriptions: Arc::default(),
            confirmations: Arc::default(),
            subscription_confirmations: Arc::default(),
            subscription_lock: Arc::default(),
            publish_lock: Mutex::new(()),
            tasks: Mutex::new(Vec::new()),
        })
    }

    async fn connected_client(&self) -> Result<AsyncClient> {
        self.client.lock().await.clone().context("MQTT client not initialized")
    }

    async fn queue_publish(&self, topic: &str, message: &str, confirm: Option<Confirmation>) -> Result<()> {
        ensure!(
            !topic.contains(['+', '#', '{', '}']),
            "publish topic contains wildcard or unresolved placeholder"
        );
        ensure!(message.len() <= 1024 * 1024, "MQTT message exceeds 1 MiB bound");
        if topic.starts_with("internal/v2/") {
            let envelope: sigbot_types::modules::messager::envelope::MessageEnvelope<serde_json::Value> =
                serde_json::from_str(message)?;
            envelope.validate()?;
            envelope.durable.validate_topic(topic)?;
        }
        let client = self.connected_client().await?;
        let _serial = self.publish_lock.lock().await;
        self.confirmations.lock().await.queued.push_back(confirm);
        if let Err(error) = client
            .publish(topic, QoS::AtLeastOnce, false, message.as_bytes().to_vec())
            .await
        {
            self.confirmations.lock().await.queued.pop_back();
            return Err(error.into());
        }
        Ok(())
    }

    async fn queue_subscription(
        client: &AsyncClient,
        confirmations: &Arc<Mutex<PublishConfirmations>>,
        submit_lock: &Arc<Mutex<()>>,
        topic: &str,
        confirm: Option<Confirmation>,
    ) -> Result<()> {
        let _serial = submit_lock.lock().await;
        confirmations.lock().await.queued.push_back(confirm);
        if let Err(error) = client.subscribe(topic, QoS::AtLeastOnce).await {
            confirmations.lock().await.queued.pop_back();
            return Err(error.into());
        }
        Ok(())
    }

    async fn dispatch(
        mut incoming: mpsc::Receiver<Publish>,
        client: AsyncClient,
        subscriptions: Arc<Mutex<HashMap<String, MessageHandler>>>,
        readiness: common_telemetry::readiness::ReadinessGuard,
    ) {
        readiness.set(true);
        while let Some(publish) = incoming.recv().await {
            let Ok(topic) = std::str::from_utf8(&publish.topic) else {
                continue;
            };
            let handlers = loop {
                let handlers: Vec<_> = subscriptions
                    .lock()
                    .await
                    .iter()
                    .filter(|(filter, _)| matches(topic, filter))
                    .map(|(_, handler)| handler.clone())
                    .collect();
                if !handlers.is_empty() {
                    break handlers;
                }
                // Session replay can precede startup registration. Keep the delivery
                // in the bounded queue until its consumer is ready, without ACK.
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            for handler in handlers {
                let mut delay = Duration::from_millis(100);
                loop {
                    match handler(publish.payload.to_vec()).await {
                        Ok(_) => break,
                        Err(error) => {
                            warn!(
                                "MQTT consumer transaction failed; delivery remains unacknowledged: {}",
                                error
                            );
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(Duration::from_secs(30));
                        }
                    }
                }
            }
            if let Err(error) = client.ack(&publish).await {
                warn!("MQTT durable delivery ACK failed; broker may redeliver: {}", error);
            }
        }
    }

    async fn poll(
        mut eventloop: EventLoop,
        incoming: mpsc::Sender<Publish>,
        client: AsyncClient,
        subscriptions: Arc<Mutex<HashMap<String, MessageHandler>>>,
        confirmations: Arc<Mutex<PublishConfirmations>>,
        subscription_confirmations: Arc<Mutex<PublishConfirmations>>,
        subscription_lock: Arc<Mutex<()>>,
        readiness: common_telemetry::readiness::ReadinessGuard,
    ) {
        let mut resubscribe_remaining = 0usize;
        let mut subscriptions_accepted = true;
        let mut delay = Duration::from_millis(100);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if incoming.send(publish).await.is_err() {
                        break;
                    }
                }
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    readiness.set(false);
                    subscriptions_accepted = ack.code == ConnectReturnCode::Success;
                    delay = Duration::from_millis(100);
                    if !ack.session_present {
                        let filters: Vec<_> = subscriptions.lock().await.keys().cloned().collect();
                        resubscribe_remaining = filters.len();
                        readiness.set(subscriptions_accepted && resubscribe_remaining == 0);
                        let client = client.clone();
                        let pending = subscription_confirmations.clone();
                        let submit_lock = subscription_lock.clone();
                        tokio::spawn(async move {
                            for topic in filters {
                                if let Err(error) =
                                    Self::queue_subscription(&client, &pending, &submit_lock, &topic, None).await
                                {
                                    warn!("MQTT resubscribe failed: {}", error);
                                }
                            }
                        });
                    } else {
                        resubscribe_remaining = 0;
                        readiness.set(subscriptions_accepted);
                    }
                }
                Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                    let mut pending = confirmations.lock().await;
                    // Reconnect retransmission reuses a packet ID and its existing waiter.
                    if !pending.inflight.contains_key(&pkid) {
                        let waiter = pending.queued.pop_front().flatten();
                        pending.inflight.insert(pkid, waiter);
                    }
                }
                Ok(Event::Incoming(Packet::PubAck(ack))) => {
                    if let Some(Some(waiter)) = confirmations.lock().await.inflight.remove(&ack.pkid) {
                        let result = match ack.reason {
                            PubAckReason::Success | PubAckReason::NoMatchingSubscribers => Ok(()),
                            reason => Err(anyhow::anyhow!("broker rejected publish: {:?}", reason)),
                        };
                        let _ = waiter.send(result);
                    }
                }
                Ok(Event::Outgoing(Outgoing::Subscribe(pkid))) => {
                    let mut pending = subscription_confirmations.lock().await;
                    if !pending.inflight.contains_key(&pkid) {
                        let waiter = pending.queued.pop_front().flatten();
                        pending.inflight.insert(pkid, waiter);
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    let accepted = !ack.return_codes.is_empty()
                        && ack
                            .return_codes
                            .iter()
                            .all(|code| matches!(code, SubscribeReasonCode::Success(QoS::AtLeastOnce)));
                    subscriptions_accepted &= accepted;
                    resubscribe_remaining = resubscribe_remaining.saturating_sub(1);
                    readiness.set(subscriptions_accepted && resubscribe_remaining == 0);
                    if let Some(Some(waiter)) = subscription_confirmations.lock().await.inflight.remove(&ack.pkid) {
                        let result = if accepted {
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!(
                                "broker denied durable subscription: {:?}",
                                ack.return_codes
                            ))
                        };
                        let _ = waiter.send(result);
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    readiness.set(false);
                    warn!("MQTT disconnected; preserving session and retrying: {}", error);
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

#[async_trait]
impl ISigbotMessagerClient for SigbotMqttMessagerClient {
    fn provider(&self) -> MessagerProvider {
        MessagerProvider::MQTT
    }

    async fn init(&self) {
        let mut guard = self.client.lock().await;
        if guard.is_some() {
            return;
        }
        let client_id = self
            .config
            .mqtt_client_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .expect("persistent MQTT requires a stable, scoped mqtt_client_id");
        assert!(
            !self.config.mqtt_clean_start,
            "durable MQTT requires mqtt_clean_start=false"
        );
        assert_eq!(self.config.mqtt_qos, 1, "durable MQTT requires mqtt_qos=1");
        assert!(
            !self.config.mqtt_retain,
            "event/request MQTT messages cannot be retained"
        );
        let mut options = MqttOptions::new(client_id, &self.config.mqtt_server, self.config.mqtt_port);
        options.set_credentials(&self.config.mqtt_username, &self.config.mqtt_password);
        options.set_keep_alive(self.config.mqtt_timeout.max(Duration::from_secs(5)));
        options.set_clean_start(false);
        options.set_session_expiry_interval(Some(7 * 24 * 60 * 60));
        options.set_manual_acks(true);
        options.set_receive_maximum(Some(32));
        options.set_max_packet_size(Some(1024 * 1024));
        let (client, eventloop) = AsyncClient::new(options, 64);
        let (sender, receiver) = mpsc::channel(64);
        let worker = tokio::spawn(Self::dispatch(
            receiver,
            client.clone(),
            self.subscriptions.clone(),
            common_telemetry::readiness::RuntimeReadiness::task(format!("mqtt.consumer:{client_id}")),
        ));
        let poller = tokio::spawn(Self::poll(
            eventloop,
            sender,
            client.clone(),
            self.subscriptions.clone(),
            self.confirmations.clone(),
            self.subscription_confirmations.clone(),
            self.subscription_lock.clone(),
            common_telemetry::readiness::RuntimeReadiness::task(format!("mqtt.session:{client_id}")),
        ));
        self.tasks.lock().await.extend([worker, poller]);
        *guard = Some(client);
        info!("Initialized persistent MQTT session {}", client_id);
    }

    async fn close(&self) {
        if let Some(client) = self.client.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), client.disconnect()).await;
        }
        for task in self.tasks.lock().await.drain(..) {
            task.abort();
        }
        self.subscriptions.lock().await.clear();
        *self.confirmations.lock().await = PublishConfirmations::default();
        *self.subscription_confirmations.lock().await = PublishConfirmations::default();
    }

    async fn publish(&self, topic: &str, message: &str) -> Result<String> {
        self.queue_publish(topic, message, None).await?;
        Ok(message.to_owned())
    }

    async fn publish_confirmed(&self, topic: &str, message: &str) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.queue_publish(topic, message, Some(sender)).await?;
        tokio::time::timeout(Duration::from_secs(30), receiver)
            .await
            .context("broker PUBACK timed out; delivery outcome is unknown")?
            .context("MQTT session closed before PUBACK")?
    }

    async fn subscribe(&self, topic: &str, handler: MessageHandler) -> Result<()> {
        ensure!(!topic.contains(['{', '}']), "subscription has unresolved placeholders");
        if topic.starts_with("internal/v2/") {
            sigbot_types::modules::messager::topic::TopicAddress::parse(topic)?;
        }
        let client = self.connected_client().await?;
        let mut subscriptions = self.subscriptions.lock().await;
        ensure!(
            !subscriptions.contains_key(topic),
            "topic already has a consumer in this session"
        );
        subscriptions.insert(topic.to_owned(), handler);
        drop(subscriptions);
        let (sender, receiver) = oneshot::channel();
        if let Err(error) = Self::queue_subscription(
            &client,
            &self.subscription_confirmations,
            &self.subscription_lock,
            topic,
            Some(sender),
        )
        .await
        {
            self.subscriptions.lock().await.remove(topic);
            return Err(error.into());
        }
        tokio::time::timeout(Duration::from_secs(30), receiver)
            .await
            .context("broker SUBACK timed out")?
            .context("MQTT session closed before SUBACK")?
    }
}
