// SPDX-License-Identifier: GPL-3.0-only

use super::{OrderVenuePostgresRepository, VenueObservation, VenuePoll};
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::Result;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use sigbot_types::messager::envelope::{DurableMessage, MessageKind};
use sigbot_types::order::execution::OrderState;
use sqlx::Row;

pub struct PublishedVenueFact {
    pub topic: String,
    pub payload: serde_json::Value,
}

pub struct OrderPublicationPostgresRepository {
    venue: OrderVenuePostgresRepository,
}

impl OrderPublicationPostgresRepository {
    pub fn new(venue: OrderVenuePostgresRepository) -> Self {
        Self { venue }
    }

    pub async fn persist(
        &self,
        poll: &VenuePoll,
        observations: Vec<VenueObservation>,
        cursor: Option<&str>,
        has_more: bool,
        terminal: Option<(OrderState, Decimal)>,
    ) -> Result<()> {
        let mut transaction = self.venue.storage.pool.begin().await?;
        if !OrderVenuePostgresRepository::owns(&mut transaction, poll).await? {
            transaction.commit().await?;
            return Ok(());
        }
        for observation in observations {
            if OrderVenuePostgresRepository::observe(
                &mut transaction,
                poll,
                observation.kind,
                &observation.reference,
                &observation.payload,
            )
            .await?
            {
                let sequence = MessagerPostgresRepository::next_sequence(
                    &mut transaction,
                    &self.venue.scope,
                    self.venue.node_id,
                    &poll.input.run_id,
                )
                .await?;
                let output = DurableMessage {
                    version: 2,
                    scope: self.venue.scope.clone(),
                    workflow_id: Some(self.venue.workflow_id),
                    account_id: Some(self.venue.account_id),
                    node_id: self.venue.node_id,
                    target_node_id: None,
                    run_id: poll.input.run_id.clone(),
                    seq: sequence,
                    attempt_no: 0,
                    kind: MessageKind::Event,
                    event_time: observation.event_time,
                    available_time: observation.available_time,
                    produced_at: observation.available_time,
                    traceparent: MessageTrace::child("order.venue.observe", &poll.input.traceparent).traceparent(),
                    payload: observation.payload,
                };
                MessagerPostgresRepository::enqueue(
                    &mut transaction,
                    &self.venue.producer,
                    &observation.topic.topic(&self.venue.scope, self.venue.account_id)?,
                    &output,
                )
                .await?;
            }
        }
        OrderVenuePostgresRepository::finish(&mut transaction, poll, cursor, has_more, terminal).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn pending_publications(
        &self,
        execution_consumer: &str,
        status_consumer: &str,
    ) -> Result<Vec<PublishedVenueFact>> {
        let rows = sqlx::query(
            "SELECT o.topic,o.payload FROM msg_outbox o WHERE o.producer=$1
            AND (o.payload->>'workflow_id')::uuid=$2 AND NOT EXISTS(SELECT 1 FROM msg_inbox i
            WHERE i.env=o.env AND i.tenant_id=o.tenant_id AND i.node_id=o.node_id
            AND i.run_id=o.run_id AND i.seq=o.seq AND i.state='done' AND i.consumer=CASE
            WHEN o.topic LIKE '%/execution/event' THEN $3 ELSE $4 END) ORDER BY o.created_at,o.seq LIMIT 32",
        )
        .bind(&self.venue.producer)
        .bind(self.venue.workflow_id)
        .bind(execution_consumer)
        .bind(status_consumer)
        .fetch_all(&self.venue.storage.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PublishedVenueFact {
                    topic: row.try_get("topic")?,
                    payload: row.try_get("payload")?,
                })
            })
            .collect()
    }
}
