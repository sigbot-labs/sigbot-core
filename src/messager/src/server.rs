// SPDX-License-Identifier: GPL-3.0-only

use crate::local::local::LocalMessager;
use crate::local::local::LocalMessagerConfig;
use crate::messager::IMessager;
use crate::mqtt::mqtt::MqttMessager;
use crate::mqtt::mqtt::MqttMessagerConfig;
use anyhow::Context;
use anyhow::Error;
use common_context::ApplicationContext;
use sigbot_types::messager::messager::MessagerConfig;
use sigbot_types::messager::messager::MessagerProvider;
use std::sync::Arc;

pub struct MessagerServer;

impl MessagerServer {
    pub async fn connect(config: Arc<MessagerConfig>) -> Result<Arc<dyn IMessager>, Error> {
        let provider = config.provider.clone();

        let properties = config.properties.as_ref().context("messager properties are required")?;
        let key = match provider {
            MessagerProvider::Mqtt => format!(
                "MQTT:{}:{}:{}:{}",
                properties.get("mqtt_server").context("mqtt_server required")?,
                properties.get("mqtt_port").context("mqtt_port required")?,
                properties.get("mqtt_username").context("mqtt_username required")?,
                properties
                    .get("mqtt_client_id")
                    .context("scoped persistent mqtt_client_id required")?
            ),
            MessagerProvider::Local => format!(
                "LOCAL:{}",
                properties
                    .get("local_client_id")
                    .map(String::as_str)
                    .unwrap_or("default")
            ),
        };

        let context = ApplicationContext::global();
        context.register_if_absent::<dyn IMessager, _>(&key, move || {
            let config = config.clone();
            let provider = provider.clone();
            async move {
                let instance: Arc<dyn IMessager> = match provider {
                    MessagerProvider::Local => {
                        LocalMessager::new(Arc::new(LocalMessagerConfig::from_config(config))).await
                    }
                    MessagerProvider::Mqtt => {
                        MqttMessager::new(Arc::new(MqttMessagerConfig::from_config(config)?)).await
                    }
                };
                Ok(instance)
            }
        })?;
        context.get::<dyn IMessager>(&key).await
    }

    pub async fn close(name: String) -> anyhow::Result<()> {
        ApplicationContext::global().close_for::<dyn IMessager>(&name).await
    }
    pub async fn shutdown() {
        if let Err(error) = ApplicationContext::global().close_all::<dyn IMessager>().await {
            common_telemetry::error!(%error, "domain provider shutdown failed");
        }
    }
}
