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

use crate::client::messaging_factory::ISigbotMessagingClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use rumqttc::v5::{
    mqttbytes::{v5::Packet, QoS},
    AsyncClient, Event, EventLoop, MqttOptions,
};
use sigbot_types::modules::messaging::messaging::MessagingInfo;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{sync::Mutex, task};

#[derive(Clone)]
pub struct SigbotMqttClientConfig {
    pub id: i64,
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

impl std::fmt::Display for SigbotMqttClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotMqttClientConfig=(
                    id={}, mqtt_server={:?}, mqtt_port={:?}, mqtt_username={:?}, mqtt_password={:?}),
            )
            ",
            self.id, self.mqtt_server, self.mqtt_port, self.mqtt_username, self.mqtt_password,
        )
    }
}

impl SigbotMqttClientConfig {
    pub fn from_messaging(messaging: Arc<MessagingInfo>) -> Self {
        let plain_config = messaging
            .configuration
            .as_ref()
            .expect("Plain configuration is required");
        let secret_config = messaging.secrets.as_ref().expect("Secret configuration is required");
        Self {
            id: messaging.base.id.expect("Messaging ID is required"),
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
            mqtt_password: secret_config
                .get("mqtt_password")
                .expect("MQTT password is required")
                .to_string(),
            mqtt_client_id: plain_config.get("mqtt_client_id").map(|s| s.to_string()),
            mqtt_clean_start: plain_config
                .get("mqtt_clean_start")
                .expect("MQTT clean start is required")
                .to_string()
                .parse::<bool>()
                .expect("MQTT clean start must be a valid boolean"),
            mqtt_qos: plain_config
                .get("mqtt_qos")
                .expect("MQTT QoS is required")
                .to_string()
                .parse::<u8>()
                .expect("MQTT QoS must be a valid number"),
            mqtt_retain: plain_config
                .get("mqtt_retain")
                .expect("MQTT retain is required")
                .to_string()
                .parse::<bool>()
                .expect("MQTT retain must be a valid boolean"),
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

pub struct SigbotMqttClient {
    config: Arc<SigbotMqttClientConfig>,
    client: Arc<Mutex<Option<AsyncClient>>>,
    eventloop: Arc<Mutex<Option<EventLoop>>>,
    // This is concurrent map to store the subscription topics and their handlers.
    subscription_registrations: Arc<
        Mutex<
            HashMap<
                String,
                Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
            >,
        >,
    >,
    // TODO: Add the memory message queue for subscription messages.
}

impl SigbotMqttClient {
    pub const NAME: &'static str = "MQTT";

    pub async fn new(config: Option<Arc<SigbotMqttClientConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Config is required"), // TODO: required input param
            client: Arc::new(Mutex::new(None)),
            eventloop: Arc::new(Mutex::new(None)),
            subscription_registrations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn qos_from_u8(qos: u8) -> QoS {
        match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce, // default
        }
    }
}

#[async_trait]
impl ISigbotMessagingClient for SigbotMqttClient {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn init(&self) {
        info!("Initializing MQTT messaging with config={}", self.config);

        let client_id = self.config.mqtt_client_id.as_deref().unwrap_or("sigbot-client");
        let mut mqttoptions = MqttOptions::new(client_id, &self.config.mqtt_server, self.config.mqtt_port);
        mqttoptions.set_keep_alive(self.config.mqtt_timeout);
        mqttoptions.set_credentials(self.config.mqtt_username.clone(), self.config.mqtt_password.clone());
        mqttoptions.set_clean_start(self.config.mqtt_clean_start);

        info!("Connecting to MQTT broker with options={:?}", mqttoptions);
        let (client, eventloop) = AsyncClient::new(mqttoptions, 16);
        *self.client.lock().await = Some(client);
        *self.eventloop.lock().await = Some(eventloop);

        info!("Initialized MQTT messaging with clientId={}", client_id);
    }

    async fn close(&self) {
        info!("Closing MQTT messaging with {}", self.config);
        let client = {
            let mut guard = self.client.lock().await;
            guard.take()
        };
        if let Some(mqtt_client) = client {
            mqtt_client.disconnect().await.unwrap();
        }
        info!("Closed MQTT messaging with {}", self.config);
    }

    async fn publish(&self, topic: &str, message: &str) -> Result<String, Error> {
        info!("Sending MQTT message to topic={} with message={}", topic, message);

        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_mut() {
            client
                .publish(
                    topic,
                    Self::qos_from_u8(self.config.mqtt_qos),
                    self.config.mqtt_retain,
                    message.as_bytes().to_vec(),
                )
                .await
                .context(format!("Failed to publish MQTT message to topic={}", topic))?;
            Ok(message.to_string())
        } else {
            Err(Error::msg(format!("MQTT client not initialized for topic={}", topic)))
        }
    }

    async fn subscribe(
        &self,
        topic: &str,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error> {
        let store_key = topic.to_string();
        if !self.subscription_registrations.lock().await.contains_key(&store_key) {
            let mut client_guard = self.client.lock().await;
            if let Some(client) = client_guard.as_mut() {
                client
                    .subscribe(topic, Self::qos_from_u8(self.config.mqtt_qos))
                    .await
                    .context(format!("Failed to subscribe to MQTT topic={}", topic))?;
            }
            // Explicitly release the lock early for avoid unnecessarily to acquire in subsequent code.
            drop(client_guard); // It's optional due to the lock will be automatically released at the end of the scope.

            // check if the eventloop is already running, if not then start it
            let handler0 = handler.clone();
            let mut guard = self.eventloop.lock().await;
            if let Some(eventloop) = guard.take() {
                let mut eventloop0 = eventloop;
                let handler1 = handler0.clone();
                task::spawn(async move {
                    while let Ok(notification) = eventloop0.poll().await {
                        debug!("Received = {:?}", notification);
                        match notification {
                            Event::Incoming(incoming) => {
                                debug!("Incoming = {:?}", incoming);
                                match incoming {
                                    Packet::Publish(publish) => {
                                        debug!("Publish = {:?}", publish);
                                        let data = publish.payload.to_vec();
                                        let handler2 = handler1.clone();
                                        tokio::spawn(async move {
                                            if let Err(e) = handler2(data).await {
                                                warn!("Error handling MQTT message: {:?}", e);
                                            }
                                        });
                                    }
                                    _ => {
                                        debug!("Ignored subscribed packet = {:?}", incoming);
                                    }
                                }
                            }
                            Event::Outgoing(outgoing) => {
                                debug!("Ignore outgoing = {:?}", outgoing);
                            }
                        }
                    }
                });
            }
            // Explicitly release the lock early for avoid unnecessarily to acquire in subsequent code.
            drop(guard); // It's optional due to the lock will be automatically released at the end of the scope.

            self.subscription_registrations.lock().await.insert(store_key, handler0);
        }
        Ok(())
    }
}
