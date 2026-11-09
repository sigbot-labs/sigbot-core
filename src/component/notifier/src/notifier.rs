use crate::email::email::{EmailNotifier, EmailNotifierConfig};
use crate::telegram::telegram::{TelegramNotifier, TelegramNotifierConfig};
use common_context::ApplicationContext;
use common_context::IProvider;
// SPDX-License-Identifier: GPL-3.0-only

use anyhow::ensure;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use minijinja::Environment;
use serde::Deserialize;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::notifier::handler::notifier::NotifierHandler;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_messager::messager::ReplyPairingCache;
use sigbot_messager::messager::SubscriptionSet;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::notifier::notification::NotificationInfo;
use sigbot_types::notifier::notification::NotificationProvider;
pub use sigbot_types::notifier::request::NotificationRequest;
use sigbot_types::notifier::request::{NotificationScope, PendingNotification};
use sigbot_types::EntityBase;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;

type ReplyPairKey = (Uuid, i64, i64);
type ReplyPair = (String, Instant);

#[async_trait]
pub trait INotifier: IProvider {
    fn provider(&self) -> NotificationProvider;

    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error>;
}

/// Only an explicit provider rejection may be retried automatically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationDeliveryError {
    Retryable,
    Rejected,
    Unknown,
}
impl std::fmt::Display for NotificationDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Retryable => "notification provider rejected temporarily",
            Self::Rejected => "notification provider rejected permanently",
            Self::Unknown => "notification provider acceptance is unknown",
        })
    }
}
impl std::error::Error for NotificationDeliveryError {}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationTemplate {
    pub version: String,
    pub body: String,
    #[serde(default)]
    pub title: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationProviderConfig {
    pub channel: String,
    pub properties: HashMap<String, String>,
    pub secret_keys: HashMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub input_node_ids: Vec<i64>,
    pub templates: BTreeMap<String, NotificationTemplate>,
    pub recipients: BTreeMap<String, Vec<String>>,
    pub providers: Vec<NotificationProviderConfig>,
}

pub struct NotifierService {
    handler: NotifierHandler,
    config: NotificationNodeConfig,
    clients: HashMap<String, Arc<dyn INotifier>>,
    consumer: String,
    relay: Mutex<Option<OutboxRelay>>,
    delivery_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    subscriptions: SubscriptionSet,
    // Existing message identity is only a cache key; requestSpanId is never persisted.
    pairings: Mutex<HashMap<ReplyPairKey, ReplyPair>>,
}
impl NotifierService {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: NotificationNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("notification runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            config.workflow_id.get_version_num() == 7 && config.node_id > 0 && !config.input_node_ids.is_empty(),
            "notification identity/edges required"
        );
        ensure!(
            config.templates.len() <= 128
                && config.providers.len() <= 2
                && config.recipients.values().all(|values| values.len() <= 128),
            "notification configuration exceeds bounds"
        );
        let consumer = format!(
            "notification:{}:{}:{}:{}",
            config.scope.env, config.scope.tenant_id, config.workflow_id, config.node_id
        );
        let database_url = get_config().storage.postgres.connection_url()?;
        let handler = NotifierHandler::connect(
            &database_url,
            NotificationScope {
                scope: config.scope.clone(),
                workflow_id: config.workflow_id,
                node_id: config.node_id,
                consumer: consumer.clone(),
            },
        )
        .await?;
        let mut clients = HashMap::new();
        for settings in &config.providers {
            let provider = NotificationProvider::of(&settings.channel)?;
            ensure!(
                !settings
                    .properties
                    .keys()
                    .any(|key| key.contains("token") || key.contains("password")),
                "notification credentials must use configured secret keys"
            );
            let mut secrets = HashMap::new();
            for (key, secret_key) in &settings.secret_keys {
                secrets.insert(key.clone(), get_config().secrets.require(secret_key)?.to_owned());
            }
            let info = NotificationInfo {
                base: EntityBase::new_with_id(Some(Uuid::now_v7())),
                name: Some(settings.channel.clone()),
                provider: Some(provider),
                properties: Some(settings.properties.clone()),
                secrets: Some(secrets),
                description: None,
            };
            ensure!(
                clients
                    .insert(settings.channel.to_lowercase(), Self::channel(Arc::new(info)).await?)
                    .is_none(),
                "duplicate channel provider"
            );
        }
        Ok(Arc::new(Self {
            handler,
            config,
            clients,
            consumer,
            relay: Mutex::new(None),
            delivery_task: Mutex::new(None),
            subscriptions: SubscriptionSet::default(),
            pairings: Mutex::new(HashMap::new()),
        }))
    }
    pub async fn start(self: &Arc<Self>, messager: Arc<dyn IMessager>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("notifier", false);
        self.subscriptions.bind(messager.clone())?;
        self.handler
            .acquire_owner(&get_config().storage.postgres.connection_url()?)
            .await?;
        self.handler
            .recover_sending(&WorkflowTopic::NotificationResult.topic(&self.config.scope, self.config.workflow_id)?)
            .await?;
        let topic = WorkflowTopic::NotificationRequest.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        self.subscriptions
            .subscribe(
                topic,
                format!("{}:request", self.consumer),
                Arc::new(move |bytes| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime.consume(&bytes).await?;
                        Ok("durably queued".into())
                    })
                }),
            )
            .await?;
        *self.relay.lock().await = Some(OutboxRelay::start(
            self.handler.messager(),
            self.consumer.clone(),
            messager,
        ));
        let runtime = self.clone();
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("notifier.delivery");
        *self.delivery_task.lock().await = Some(tokio::spawn(async move {
            readiness.set(true);
            loop {
                if runtime.deliver_pending().await.is_err() {
                    common_telemetry::warn!("notification delivery operation failed; durable state retained");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }));
        common_telemetry::readiness::RuntimeReadiness::business("notifier", true);
        Ok(())
    }
    pub async fn close(&self) {
        common_telemetry::readiness::RuntimeReadiness::business("notifier", false);
        if let Some(task) = self.delivery_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(relay) = self.relay.lock().await.take() {
            relay.close();
        }
        if let Err(error) = self.subscriptions.close().await {
            common_telemetry::error!(%error, "notification subscription close failed");
        }
        self.handler.release_owner().await;
        for client in self.clients.values() {
            if let Err(error) = client.close().await {
                common_telemetry::error!(%error, "notification provider close failed");
            }
        }
    }
    pub fn render(template: &NotificationTemplate, request: &NotificationRequest) -> Result<(String, String)> {
        ensure!(
            !template.version.is_empty()
                && template.version.len() <= 128
                && request.variables.len() <= 128
                && template.body.len() <= 65_536
                && template.title.len() <= 512,
            "notification template is invalid or unbounded"
        );
        let mut environment = Environment::new();
        environment.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
        environment.add_template("body", &template.body)?;
        environment.add_template("title", &template.title)?;
        let body = environment.get_template("body")?.render(&request.variables)?;
        let title = environment.get_template("title")?.render(&request.variables)?;
        ensure!(
            body.len() <= 65_536 && title.len() <= 512,
            "rendered notification exceeds limits"
        );
        Ok((title, body))
    }
    pub async fn consume(&self, bytes: &[u8]) -> Result<()> {
        let wire: MessageEnvelope<NotificationRequest> = serde_json::from_slice(bytes)?;
        wire.validate()?;
        let input = &wire.durable;
        self.handler.ensure_owner().await?;
        ensure!(
            input.available_time <= Utc::now(),
            "future notification request rejected"
        );
        ensure!(
            input.scope == self.config.scope
                && input.workflow_id == Some(self.config.workflow_id)
                && input.kind == MessageKind::Request,
            "notification scope/kind mismatch"
        );
        if !self.config.input_node_ids.contains(&input.node_id)
            || input.target_node_id.is_some_and(|id| id != self.config.node_id)
        {
            return Ok(());
        }
        let request = &input.payload;
        request.validate()?;
        ensure!(
            ["in_app", "email", "telegram"].contains(&request.channel.as_str()),
            "unknown notification channel"
        );
        ensure!(
            self.config
                .recipients
                .get(&request.channel)
                .is_some_and(|items| items.contains(&request.recipient)),
            "recipient is not allowed by this workflow"
        );
        ensure!(
            request.channel == "in_app" || self.clients.contains_key(&request.channel),
            "notification provider not configured"
        );
        let template = self
            .config
            .templates
            .get(&request.template)
            .context("unknown notification template")?;
        let (title, body) = Self::render(template, request)?;
        let value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        let topic = WorkflowTopic::NotificationRequest.topic(&input.scope, self.config.workflow_id)?;
        let result_topic = WorkflowTopic::NotificationResult.topic(&self.config.scope, self.config.workflow_id)?;
        if let Some(response) = self
            .handler
            .queue(&topic, &value, input, &template.version, &title, &body, &result_topic)
            .await?
        {
            if let Some(pairing) = wire.request_span_id.as_ref() {
                let mut pairings = self.pairings.lock().await;
                pairings.retain(|_, (_, expires)| *expires > Instant::now());
                if pairings.len() >= 1024 {
                    if let Some(oldest) = pairings
                        .iter()
                        .min_by_key(|(_, (_, time))| *time)
                        .map(|(key, _)| key.clone())
                    {
                        pairings.remove(&oldest);
                    }
                }
                pairings.insert(
                    (input.run_id.clone(), input.node_id, input.seq),
                    (pairing.clone(), Instant::now() + Duration::from_secs(60)),
                );
                ReplyPairingCache::shared().remember(&response, pairing)?;
            }
        }
        Ok(())
    }
    async fn deliver_pending(&self) -> Result<()> {
        let Some(pending) = self.handler.claim_pending().await? else {
            return Ok(());
        };
        let client = self
            .clients
            .get(&pending.source.payload.channel)
            .context("notification channel unavailable")?;
        let result = client
            .send_simple_message(Some(vec![pending.source.payload.recipient.clone()]), &pending.body)
            .await;
        let (state, receipt) = match result {
            Ok(receipt) => ("delivered", Some(receipt)),
            Err(error) => match error.downcast_ref::<NotificationDeliveryError>() {
                Some(NotificationDeliveryError::Retryable) if pending.attempt < 5 => ("pending", None),
                Some(NotificationDeliveryError::Retryable | NotificationDeliveryError::Rejected) => ("rejected", None),
                _ => ("unknown", None),
            },
        };
        let response = self
            .handler
            .finish(
                &pending,
                state,
                receipt.as_deref(),
                &WorkflowTopic::NotificationResult.topic(&self.config.scope, self.config.workflow_id)?,
            )
            .await?;
        self.remember_pairing(&pending, &response).await
    }
    async fn remember_pairing(
        &self,
        pending: &PendingNotification,
        response: &sigbot_types::messager::envelope::DurableMessage<Value>,
    ) -> Result<()> {
        if let Some((pairing, expires)) = self.pairings.lock().await.get(&(
            pending.source.run_id.clone(),
            pending.source.node_id,
            pending.source.seq,
        )) {
            if *expires > Instant::now() {
                ReplyPairingCache::shared().remember(response, pairing)?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl IProvider for NotifierService {
    async fn close(&self) -> anyhow::Result<()> {
        NotifierService::close(self).await;
        Ok(())
    }
}

impl NotifierService {
    async fn channel(
        info: Arc<sigbot_types::notifier::notification::NotificationInfo>,
    ) -> Result<Arc<dyn INotifier>, Error> {
        let instance_id = sigbot_utils::providers::ProviderInstanceId::configured("notifier", &*info)?;
        let context = ApplicationContext::global();
        context.register_if_absent::<dyn INotifier, _>(&instance_id, move || {
            let info = info.clone();
            async move {
                let instance: Arc<dyn INotifier> =
                    match info.provider.as_ref().context("notification provider required")? {
                        NotificationProvider::Email => {
                            EmailNotifier::new(Some(EmailNotifierConfig::from_config(info.clone()))).await
                        }
                        NotificationProvider::Telegram => {
                            TelegramNotifier::new(Some(TelegramNotifierConfig::from_config(info.clone()))).await
                        }
                    };
                Ok(instance)
            }
        })?;
        context.get::<dyn INotifier>(&instance_id).await
    }
}
