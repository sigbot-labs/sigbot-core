// SPDX-License-Identifier: GPL-3.0-only
use crate::manager::trades::simulated_exchange::{MatchingConfig, SimulatedExchange};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use serde_json::Value;
use sigbot_messager::{client::messager_factory::ISigbotMessagerClient, storage::postgres::PostgresMessageStore};
use sigbot_types::modules::{
    datafeed::event::DatafeedEvent,
    messager::{envelope::MessageEnvelope, topic::WorkflowTopic},
};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub struct ExecutionRuntime;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchingFeedConfig {
    workflow_id: i64,
    run_id: String,
    input_node_ids: Vec<i64>,
}

impl ExecutionRuntime {
    pub async fn start(properties: &HashMap<String, String>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("backtest.execution", false);
        let config: MatchingConfig =
            serde_json::from_str(properties.get("matching_config").context("matching_config required")?)?;
        let database_env = properties
            .get("database_url_env")
            .context("database_url_env required")?;
        let bind_addr = properties.get("bind_addr").context("execution bind_addr required")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var(database_env)?)
            .await?;
        let exchange = Arc::new(SimulatedExchange::new(pool.clone(), config)?);
        let producer = exchange.producer();
        if let Some(raw) = properties.get("matching_feed_config") {
            let feed: MatchingFeedConfig = serde_json::from_str(raw)?;
            ensure!(
                !exchange.config().virtual_clock
                    && feed.workflow_id > 0
                    && !feed.run_id.is_empty()
                    && !feed.input_node_ids.is_empty()
                    && feed.input_node_ids.iter().all(|id| *id > 0),
                "Paper matching requires explicit source edges; Replay drives its own clock"
            );
            let topic = WorkflowTopic::FeedMarket.topic(&exchange.config().scope, feed.workflow_id)?;
            let matcher = exchange.clone();
            let routing_pool = pool.clone();
            let expected_topic = topic.clone();
            messager
                .subscribe(
                    &topic,
                    Arc::new(move |bytes| {
                        let matcher = matcher.clone();
                        let pool = routing_pool.clone();
                        let topic = expected_topic.clone();
                        let run = feed.run_id.clone();
                        let owners = feed.input_node_ids.clone();
                        Box::pin(async move {
                            let wire: MessageEnvelope<DatafeedEvent> = serde_json::from_slice(&bytes)?;
                            wire.validate()?;
                            wire.durable.validate_topic(&topic)?;
                            if wire.durable.run_id != run
                                || !owners.contains(&wire.durable.node_id)
                                || wire
                                    .durable
                                    .target_node_id
                                    .is_some_and(|id| id != matcher.config().node_id)
                            {
                                let value: MessageEnvelope<Value> = serde_json::from_slice(&bytes)?;
                                let consumer = format!("{}.routing:{run}", matcher.producer());
                                let mut tx = pool.begin().await?;
                                if PostgresMessageStore::accept(&mut tx, &consumer, &topic, &value).await? {
                                    PostgresMessageStore::complete(&mut tx, &consumer, &value.durable).await?;
                                }
                                tx.commit().await?;
                                return Ok("outside matching route".into());
                            }
                            matcher.match_tick(&wire, Utc::now()).await?;
                            Ok("committed".into())
                        })
                    }),
                )
                .await?;
        }
        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.listener");
        tokio::spawn(async move {
            readiness.set(true);
            if let Err(error) = axum::serve(listener, exchange.router()).await {
                tracing::error!(%error, "backtest execution listener stopped");
            }
        });
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.outbox");
        tokio::spawn(async move {
            readiness.set(true);
            let store = PostgresMessageStore::new(pool);
            loop {
                if let Err(error) = store
                    .dispatch_pending(&producer, messager.as_ref(), 32, &|parent| {
                        Ok(MessageTrace::child("backtest.retry", parent).traceparent())
                    })
                    .await
                {
                    tracing::warn!(%error, "backtest execution relay pending");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        common_telemetry::readiness::RuntimeReadiness::business("backtest.execution", true);
        Ok(())
    }
}
