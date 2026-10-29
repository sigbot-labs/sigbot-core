// SPDX-License-Identifier: GPL-3.0-only
use crate::manager::{execution_postgres::WalletExecutionProcessor, reservation_postgres::WalletReservationProcessor};
use anyhow::{Context, Result};
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use sigbot_core::{config::config::get_config, store::postgres::PostgresRepository};
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::{
    messager::{
        envelope::MessageEnvelope,
        topic::{AccountTopic, MessageScope},
    },
    order::execution::{ExecutionFact, OrderStatusFact},
    wallet::{funding::FundingFact, reservation::ReservationRequest, SigbotWalletManagerArgument},
};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletRuntimeConfig {
    pub scope: MessageScope,
    pub account_id: i64,
    pub node_id: i64,
    pub order_node_ids: Vec<i64>,
}

pub struct WalletRuntime {
    relay: OutboxRelay,
}

impl WalletRuntime {
    pub async fn start(
        argument: &SigbotWalletManagerArgument,
        messager: Arc<dyn ISigbotMessagerClient>,
    ) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("wallet", false);
        let properties = argument
            .properties
            .as_ref()
            .context("wallet runtime properties required")?;
        let config: WalletRuntimeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("typed wallet runtime_config required")?,
        )?;
        let repository = PostgresRepository::<()>::get_or_init(&get_config().appdb.postgres).await?;
        let pool = repository.get_pool().clone();
        let topic = AccountTopic::ExecutionEvent.topic(&config.scope, config.account_id)?;
        let reservations = Arc::new(WalletReservationProcessor::new(
            pool.clone(),
            config.scope.clone(),
            config.account_id,
            config.node_id,
            config.order_node_ids.clone(),
        )?);
        let request_topic = AccountTopic::WalletReservationRequest.topic(&config.scope, config.account_id)?;
        messager
            .subscribe(
                &request_topic,
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
        let processor = Arc::new(WalletExecutionProcessor::new(
            pool.clone(),
            config.scope.clone(),
            config.account_id,
            config.node_id,
        )?);
        let producer = processor.producer();
        let statuses = processor.clone();
        let order_nodes = config.order_node_ids;
        messager
            .subscribe(
                &AccountTopic::OrderEvent.topic(&config.scope, config.account_id)?,
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
        messager
            .subscribe(
                &topic,
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
            relay: OutboxRelay::start(PostgresMessageStore::new(pool), producer, messager),
        }))
    }

    pub fn close(&self) {
        common_telemetry::readiness::RuntimeReadiness::business("wallet", false);
        self.relay.close();
    }
}
