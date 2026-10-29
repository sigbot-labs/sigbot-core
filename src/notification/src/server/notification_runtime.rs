// SPDX-License-Identifier: GPL-3.0-only
//! Durable notification intake, one scoped delivery owner, and explicit uncertain outcomes.
use crate::client::notification_factory::{
    ISigbotNotificationClient, NotificationDeliveryError, SigbotNotificationClientFactory,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use minijinja::Environment;
use serde::Deserialize;
use serde_json::{json, Value};
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay, reply_pairing::ReplyPairingCache},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::{
    modules::{
        messager::{
            envelope::{DurableMessage, MessageEnvelope, MessageKind},
            topic::{MessageScope, WorkflowTopic},
        },
        notification::notification::{NotificationInfo, NotificationProvider},
    },
    EntityBase,
};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Row, Transaction};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

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
    pub secret_env: HashMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: i64,
    pub node_id: i64,
    pub input_node_ids: Vec<i64>,
    pub database_url_env: String,
    pub templates: BTreeMap<String, NotificationTemplate>,
    pub recipients: BTreeMap<String, Vec<String>>,
    pub providers: Vec<NotificationProviderConfig>,
}
pub use sigbot_types::modules::notification::request::NotificationRequest;

pub struct NotificationRuntime {
    pool: PgPool,
    config: NotificationNodeConfig,
    clients: HashMap<String, Arc<dyn ISigbotNotificationClient>>,
    consumer: String,
    owner: Mutex<Option<PgConnection>>,
    relay: Mutex<Option<OutboxRelay>>,
    delivery_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Existing message identity is only a cache key; requestSpanId is never persisted.
    pairings: Mutex<HashMap<(String, i64, i64), (String, Instant)>>,
}
impl NotificationRuntime {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: NotificationNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("notification runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            config.workflow_id > 0 && config.node_id > 0 && !config.input_node_ids.is_empty(),
            "notification identity/edges required"
        );
        ensure!(
            config.templates.len() <= 128
                && config.providers.len() <= 2
                && config.recipients.values().all(|values| values.len() <= 128),
            "notification configuration exceeds bounds"
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var(&config.database_url_env)?)
            .await?;
        let consumer = format!(
            "notification:{}:{}:{}:{}:{}",
            config.scope.env, config.scope.tenant_id, config.scope.space_id, config.workflow_id, config.node_id
        );
        let mut clients = HashMap::new();
        for settings in &config.providers {
            let provider = NotificationProvider::of(&settings.channel)?;
            ensure!(
                !settings
                    .properties
                    .keys()
                    .any(|key| key.contains("token") || key.contains("password")),
                "notification credentials must use secret_env"
            );
            let mut secrets = HashMap::new();
            for (key, variable) in &settings.secret_env {
                secrets.insert(key.clone(), std::env::var(variable)?);
            }
            let info = NotificationInfo {
                base: EntityBase::new_with_id(Some(config.node_id)),
                name: Some(settings.channel.clone()),
                provider: Some(provider),
                properties: Some(settings.properties.clone()),
                secrets: Some(secrets),
                description: None,
            };
            ensure!(
                clients
                    .insert(
                        settings.channel.to_lowercase(),
                        SigbotNotificationClientFactory::create(Arc::new(info)).await?
                    )
                    .is_none(),
                "duplicate channel provider"
            );
        }
        Ok(Arc::new(Self {
            pool,
            config,
            clients,
            consumer,
            owner: Mutex::new(None),
            relay: Mutex::new(None),
            delivery_task: Mutex::new(None),
            pairings: Mutex::new(HashMap::new()),
        }))
    }
    pub async fn start(self: &Arc<Self>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("notification", false);
        let mut owner = self.owner.lock().await;
        ensure!(owner.is_none(), "notification runtime already started");
        let mut connection = PgConnection::connect(&std::env::var(&self.config.database_url_env)?).await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1,0))")
            .bind(&self.consumer)
            .fetch_one(&mut connection)
            .await?;
        ensure!(acquired, "another process owns this notification node");
        *owner = Some(connection);
        drop(owner);
        self.recover_sending().await?;
        let topic = WorkflowTopic::NotificationRequest.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        messager
            .subscribe(
                &topic,
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
            PostgresMessageStore::new(self.pool.clone()),
            self.consumer.clone(),
            messager,
        ));
        let runtime = self.clone();
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("notification.delivery");
        *self.delivery_task.lock().await = Some(tokio::spawn(async move {
            readiness.set(true);
            loop {
                if runtime.deliver_pending().await.is_err() {
                    common_telemetry::warn!("notification delivery operation failed; durable state retained");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }));
        common_telemetry::readiness::RuntimeReadiness::business("notification", true);
        Ok(())
    }
    pub async fn close(&self) {
        common_telemetry::readiness::RuntimeReadiness::business("notification", false);
        if let Some(task) = self.delivery_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(relay) = self.relay.lock().await.take() {
            relay.close();
        }
        self.owner.lock().await.take(); // Dedicated connection drop releases the session lock.
        for client in self.clients.values() {
            client.close().await;
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
        self.ensure_owner().await?;
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
        let mut tx = self.pool.begin().await?;
        let topic = WorkflowTopic::NotificationRequest.topic(&input.scope, self.config.workflow_id)?;
        if !PostgresMessageStore::accept(&mut tx, &self.consumer, &topic, &value).await? {
            tx.commit().await?;
            return Ok(());
        }
        let state = if request.channel == "in_app" {
            "delivered"
        } else {
            "pending"
        };
        let inserted=sqlx::query("INSERT INTO notification_delivery(env,tenant_id,space_id,workflow_id,owner_node_id,producer_node_id,run_id,seq,channel,recipient,template_version,payload,state,delivered_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,CASE WHEN $13='delivered' THEN NOW() ELSE NULL END) ON CONFLICT DO NOTHING")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(self.config.workflow_id).bind(self.config.node_id).bind(input.node_id)
            .bind(&input.run_id).bind(input.seq).bind(&request.channel).bind(&request.recipient).bind(&template.version)
            .bind(json!({"title":title,"body":body,"source":input,"kind":request.variables.get("kind")})).bind(state).execute(&mut *tx).await?.rows_affected()>0;
        if inserted {
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
            }
            self.result(&mut tx, input, state, None, 0).await?;
        }
        PostgresMessageStore::complete(&mut tx, &self.consumer, &value.durable).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn result(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt_no: u32,
    ) -> Result<()> {
        let sequence =
            PostgresMessageStore::next_sequence(tx, &self.config.scope, self.config.node_id, &source.run_id).await?;
        let response = DurableMessage {
            version: 2,
            scope: self.config.scope.clone(),
            workflow_id: Some(self.config.workflow_id),
            account_id: None,
            node_id: self.config.node_id,
            target_node_id: Some(source.node_id),
            run_id: source.run_id.clone(),
            seq: sequence,
            attempt_no,
            kind: MessageKind::Result,
            event_time: source.event_time,
            available_time: source.available_time,
            produced_at: Utc::now(),
            traceparent: MessageTrace::child("notification.result", &source.traceparent).traceparent(),
            payload: json!({"producer_node_id":source.node_id,"seq":source.seq,"channel":source.payload.channel,"recipient":source.payload.recipient,"state":state,"provider_receipt":receipt}),
        };
        if let Some((pairing, expires)) =
            self.pairings
                .lock()
                .await
                .get(&(source.run_id.clone(), source.node_id, source.seq))
        {
            if *expires > Instant::now() {
                ReplyPairingCache::shared().remember(&response, pairing)?;
            }
        }
        PostgresMessageStore::enqueue(
            tx,
            &self.consumer,
            &WorkflowTopic::NotificationResult.topic(&self.config.scope, self.config.workflow_id)?,
            &response,
        )
        .await?;
        Ok(())
    }
    async fn recover_sending(&self) -> Result<()> {
        loop {
            let mut tx = self.pool.begin().await?;
            let rows=sqlx::query("SELECT payload,attempt_no FROM notification_delivery WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND workflow_id=$4 AND owner_node_id=$5 AND state='sending' LIMIT 32 FOR UPDATE")
                .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(self.config.scope.space_id).bind(self.config.workflow_id).bind(self.config.node_id).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                tx.commit().await?;
                return Ok(());
            }
            for row in rows {
                let source: DurableMessage<NotificationRequest> =
                    serde_json::from_value(row.get::<Value, _>("payload")["source"].clone())?;
                self.finish(
                    &mut tx,
                    &source,
                    "unknown",
                    None,
                    row.get::<i32, _>("attempt_no") as u32,
                )
                .await?;
            }
            tx.commit().await?;
        }
    }
    async fn ensure_owner(&self) -> Result<()> {
        let mut owner = self.owner.lock().await;
        let connection = owner.as_mut().context("notification node has no delivery lease")?;
        sqlx::query("SELECT 1")
            .execute(connection)
            .await
            .context("notification delivery lease connection lost")?;
        Ok(())
    }
    async fn deliver_pending(&self) -> Result<()> {
        self.ensure_owner().await?;
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query("SELECT payload,attempt_no FROM notification_delivery WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND workflow_id=$4 AND owner_node_id=$5 AND state='pending' AND available_at<=NOW() ORDER BY available_at LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(self.config.scope.space_id).bind(self.config.workflow_id).bind(self.config.node_id).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(());
        };
        let payload: Value = row.get("payload");
        let source: DurableMessage<NotificationRequest> = serde_json::from_value(payload["source"].clone())?;
        let attempt = row.get::<i32, _>("attempt_no") as u32 + 1;
        self.update(&mut tx, &source, "sending", None, attempt).await?;
        tx.commit().await?;
        let client = self
            .clients
            .get(&source.payload.channel)
            .context("notification channel unavailable")?;
        let result = client
            .send_simple_message(
                Some(vec![source.payload.recipient.clone()]),
                payload["body"].as_str().context("notification content missing")?,
            )
            .await;
        let (state, receipt) = match result {
            Ok(receipt) => ("delivered", Some(receipt)),
            Err(error) => match error.downcast_ref::<NotificationDeliveryError>() {
                Some(NotificationDeliveryError::Retryable) if attempt < 5 => ("pending", None),
                Some(NotificationDeliveryError::Retryable | NotificationDeliveryError::Rejected) => ("rejected", None),
                _ => ("unknown", None),
            },
        };
        let mut tx = self.pool.begin().await?;
        self.finish(&mut tx, &source, state, receipt.as_deref(), attempt)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn update(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt: u32,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE notification_delivery SET state=$10,provider_receipt=$11,attempt_no=$12,updated_at=NOW(),available_at=CASE WHEN $10='pending' THEN NOW()+make_interval(secs=>$13) ELSE available_at END,delivered_at=CASE WHEN $10='delivered' THEN NOW() ELSE delivered_at END
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND workflow_id=$4 AND owner_node_id=$5 AND producer_node_id=$6 AND run_id=$7 AND seq=$8 AND channel=$9 AND recipient=$14")
            .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(self.config.scope.space_id).bind(self.config.workflow_id).bind(self.config.node_id).bind(source.node_id).bind(&source.run_id).bind(source.seq).bind(&source.payload.channel)
            .bind(state).bind(receipt).bind(attempt as i32).bind((2_u32.pow(attempt.min(6)))as f64).bind(&source.payload.recipient).execute(&mut **tx).await?.rows_affected();
        ensure!(changed == 1, "notification owner/delivery mismatch");
        Ok(())
    }
    async fn finish(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        source: &DurableMessage<NotificationRequest>,
        state: &str,
        receipt: Option<&str>,
        attempt: u32,
    ) -> Result<()> {
        self.update(tx, source, state, receipt, attempt).await?;
        self.result(tx, source, state, receipt, attempt).await
    }
}
