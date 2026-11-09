// SPDX-License-Identifier: GPL-3.0-only
use crate::ledger::execution::WalletExecutionProcessor;
use crate::ledger::reservation::WalletReservationProcessor;
use crate::wallet::IWallet;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use sigbot_core::config::config::get_config;
use sigbot_core::config::config::RuntimeComponentProperties;
use sigbot_core::modules::wallet::handler::wallet::WalletReservationHandler;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_messager::messager::SubscriptionSet;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::wallet::funding::FundingFact;
use sigbot_types::wallet::reservation::ReservationRequest;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletAccountProcessorConfig {
    pub scope: MessageScope,
    pub account_id: Uuid,
    pub node_id: i64,
    pub order_node_ids: Vec<i64>,
}

pub struct WalletAccountProcessor {
    account_id: Uuid,
    relay: OutboxRelay,
    subscriptions: SubscriptionSet,
}

impl WalletAccountProcessor {
    pub async fn start(argument: &RuntimeComponentProperties, messager: Arc<dyn IMessager>) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("wallet", false);
        let properties = argument
            .properties
            .as_ref()
            .context("wallet runtime properties required")?;
        let config: WalletAccountProcessorConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("typed wallet runtime_config required")?,
        )?;
        let topic = AccountTopic::ExecutionEvent.topic(&config.scope, config.account_id)?;
        let subscriptions = SubscriptionSet::default();
        subscriptions.bind(messager.clone())?;
        let consumer = format!(
            "wallet:{}:{}:{}:{}",
            config.scope.env, config.scope.tenant_id, config.account_id, config.node_id
        );
        let reservations = Arc::new(WalletReservationProcessor::new(
            WalletReservationHandler::connect(
                &get_config().storage.postgres,
                config.scope.clone(),
                config.account_id,
                config.node_id,
                config.order_node_ids.clone(),
            )
            .await?,
        ));
        let request_topic = AccountTopic::WalletReservationRequest.topic(&config.scope, config.account_id)?;
        subscriptions
            .subscribe(
                request_topic,
                format!("{}:{}", consumer, AccountTopic::WalletReservationRequest.suffix()),
                Arc::new(move |bytes| {
                    let processor = reservations.clone();
                    Box::pin(async move {
                        let request: MessageEnvelope<ReservationRequest> = serde_json::from_slice(&bytes)?;
                        let _span = MessageTrace::child("wallet.reservation.consume", &request.durable.traceparent);
                        processor.process(&request).await?;
                        Ok("committed".into())
                    })
                }),
            )
            .await?;
        let processor = Arc::new(
            WalletExecutionProcessor::connect(
                &get_config().storage.postgres,
                config.scope.clone(),
                config.account_id,
                config.node_id,
            )
            .await?,
        );
        let message_store = processor.messager();
        let producer = processor.producer();
        let statuses = processor.clone();
        let order_nodes = config.order_node_ids;
        subscriptions
            .subscribe(
                AccountTopic::OrderEvent.topic(&config.scope, config.account_id)?,
                format!("{}:{}", consumer, AccountTopic::OrderEvent.suffix()),
                Arc::new(move |bytes| {
                    let processor = statuses.clone();
                    let allowed = order_nodes.clone();
                    Box::pin(async move {
                        let value: MessageEnvelope<serde_json::Value> = serde_json::from_slice(&bytes)?;
                        value.validate()?;
                        if !allowed.contains(&value.durable.node_id) || value.durable.payload.get("receipt").is_none() {
                            return Ok("not a terminal fact".into());
                        }
                        let status: MessageEnvelope<OrderStatusFact> = serde_json::from_slice(&bytes)?;
                        let trace = MessageTrace::child("wallet.order.status", &status.durable.traceparent);
                        processor.process_status(&status, trace.traceparent()).await?;
                        Ok("durably accepted".into())
                    })
                }),
            )
            .await?;
        subscriptions
            .subscribe(
                topic,
                format!("{}:{}", consumer, AccountTopic::ExecutionEvent.suffix()),
                Arc::new(move |bytes| {
                    let processor = processor.clone();
                    Box::pin(async move {
                        let message: MessageEnvelope<serde_json::Value> = serde_json::from_slice(&bytes)?;
                        let consumer = MessageTrace::child("wallet.fact", &message.durable.traceparent);
                        let output = MessageTrace::child("wallet.portfolio", &consumer.traceparent());
                        if message.durable.payload.get("source_event_id").is_some() {
                            let funding: MessageEnvelope<FundingFact> = serde_json::from_slice(&bytes)?;
                            processor.process_funding(&funding, output.traceparent()).await?;
                        } else {
                            let execution: MessageEnvelope<ExecutionFact> = serde_json::from_slice(&bytes)?;
                            processor.process(&execution, output.traceparent()).await?;
                        }
                        Ok(String::new())
                    })
                }),
            )
            .await?;
        common_telemetry::readiness::RuntimeReadiness::business("wallet", true);
        Ok(Arc::new(Self {
            account_id: config.account_id,
            relay: OutboxRelay::start(message_store, producer, messager),
            subscriptions,
        }))
    }

    pub async fn close(&self) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("wallet", false);
        self.relay.close();
        self.subscriptions.close().await
    }
}

impl IWallet for WalletAccountProcessor {
    fn account_id(&self) -> Uuid {
        self.account_id
    }
}

#[async_trait]
impl IProvider for WalletAccountProcessor {
    async fn close(&self) -> Result<()> {
        WalletAccountProcessor::close(self).await
    }
}
