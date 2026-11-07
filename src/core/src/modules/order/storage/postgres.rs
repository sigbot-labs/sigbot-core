// SPDX-License-Identifier: GPL-3.0-only

mod admission;
mod compensation;
mod execution;
mod group;
mod publication;
mod record;
mod reservation;
mod risk;
mod status;
mod venue;

pub(crate) use admission::OrderAdmissionPostgresRepository;
pub(crate) use compensation::OrderCompensationPostgresRepository;
pub(crate) use execution::OrderExecutionPostgresRepository;
pub(crate) use group::{GroupIdentity, OrderGroupPostgresRepository, SettledLeg};
pub(crate) use publication::OrderPublicationPostgresRepository;
pub use publication::PublishedVenueFact;
pub(crate) use record::OrderRecordPostgresRepository;
pub(crate) use reservation::OrderReservationPostgresRepository;
pub use risk::OrderExposure;
pub(crate) use risk::OrderRiskPostgresRepository;
pub(crate) use status::OrderStatusPostgresRepository;
pub(crate) use venue::OrderVenuePostgresRepository;
pub use venue::{VenueObservation, VenuePoll};

use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::Result;
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::{AccountTopic, MessageScope};
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::TradeIntent;
use sqlx::Row;
use uuid::Uuid;

#[derive(Clone)]
pub struct OrderPostgresRepository {
    pub(crate) pool: sqlx::PgPool,
}

pub enum SignalAdmission {
    Duplicate,
    Accepted,
    Rejected(String),
}

impl OrderPostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                .connect(database_url)
                .await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::new(self.pool.clone())
    }

    pub fn owner(scope: &MessageScope, workflow_id: Uuid, node_id: i64) -> String {
        format!("order:{}:{}:{workflow_id}:{node_id}", scope.env, scope.tenant_id)
    }

    pub fn fact_owner(kind: &str, scope: &MessageScope, workflow_id: Uuid, node_id: i64) -> String {
        format!("order.{kind}:{}:{}:{workflow_id}:{node_id}", scope.env, scope.tenant_id)
    }

    pub async fn enqueue(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        producer: &str,
        node_id: i64,
        input: &DurableMessage<TradeIntent>,
        state: OrderState,
        exchange_order_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let sequence =
            MessagerPostgresRepository::next_sequence(transaction, &input.scope, node_id, &input.run_id).await?;
        let message = DurableMessage {
            version: 2,
            scope: input.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: input.account_id,
            node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq: sequence,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: MessageTrace::child("order.publish", &input.traceparent).traceparent(),
            payload: serde_json::json!({"state":state,"exchange_order_id":exchange_order_id,
                "wallet_id":input.payload.wallet_id,"reservation_node_id":input.node_id,
                "reservation_seq":input.seq}),
        };
        MessagerPostgresRepository::enqueue(
            transaction,
            producer,
            &AccountTopic::OrderEvent.topic(&input.scope, input.payload.account_id)?,
            &message,
        )
        .await?;
        Ok(())
    }

    pub async fn virtual_time(&self, scope: &MessageScope, run_id: &Uuid) -> Result<DateTime<Utc>> {
        Ok(sqlx::query_scalar(
            "SELECT virtual_time FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND state='running'",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("replay Order requires a running scoped clock checkpoint"))?)
    }

    pub async fn pending(&self, consumer: &str, workflow_id: Uuid) -> Result<Vec<DurableMessage<Value>>> {
        let rows = sqlx::query(
            "SELECT payload FROM msg_inbox WHERE consumer=$1 AND state='pending'
            AND (payload->>'workflow_id')::uuid=$2 ORDER BY received_at LIMIT 32",
        )
        .bind(consumer)
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| serde_json::from_value(row.get("payload")).map_err(Into::into))
            .collect()
    }

    pub async fn owns_account(&self, scope: &MessageScope, account_id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ledger_info WHERE account_id=$1 AND env=$2 AND tenant_id=$3
            AND del_flag=FALSE)",
        )
        .bind(account_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn is_compensation(&self, input: &DurableMessage<TradeIntent>, order_node_id: i64) -> Result<bool> {
        OrderAdmissionPostgresRepository::is_compensation(&self.pool, input, order_node_id).await
    }

    pub async fn admit_signal(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
        coordinators: &[i64],
        clock: DateTime<Utc>,
    ) -> Result<SignalAdmission> {
        let mut transaction = self.pool.begin().await?;
        if !MessagerPostgresRepository::accept(&mut transaction, consumer, topic, wire).await? {
            transaction.commit().await?;
            return Ok(SignalAdmission::Duplicate);
        }
        if let Err(error) =
            OrderAdmissionPostgresRepository::register(&mut transaction, input, order_node_id, coordinators, clock)
                .await
        {
            if error.downcast_ref::<sqlx::Error>().is_some() {
                return Err(error);
            }
            Self::quarantine_signal_in(&mut transaction, consumer, input, order_node_id, clock).await?;
            transaction.commit().await?;
            return Ok(SignalAdmission::Rejected(error.to_string()));
        }
        transaction.commit().await?;
        Ok(SignalAdmission::Accepted)
    }

    pub async fn reject_signal(
        &self,
        consumer: &str,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
        clock: DateTime<Utc>,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        Self::quarantine_signal_in(&mut transaction, consumer, input, order_node_id, clock).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn quarantine_signal_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        consumer: &str,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
        clock: DateTime<Utc>,
    ) -> Result<()> {
        let changed = sqlx::query(
            "UPDATE msg_inbox SET state='quarantined',completed_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6 AND state='pending'",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(consumer)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if changed == 1 {
            OrderAdmissionPostgresRepository::reject(transaction, input).await?;
            OrderPostgresRepository::enqueue(
                transaction,
                consumer,
                order_node_id,
                input,
                OrderState::Rejected,
                None,
                clock,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn complete_signal(&self, consumer: &str, topic: &str, wire: &MessageEnvelope<Value>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        if MessagerPostgresRepository::accept(&mut transaction, consumer, topic, wire).await? {
            MessagerPostgresRepository::complete(&mut transaction, consumer, &wire.durable).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn quarantine_route(&self, consumer: &str, topic: &str, wire: &MessageEnvelope<Value>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        if MessagerPostgresRepository::accept(&mut transaction, consumer, topic, wire).await? {
            sqlx::query(
                "UPDATE msg_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason='outside_order_route'
                WHERE env=$1 AND tenant_id=$2 AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
            )
            .bind(&wire.durable.scope.env)
            .bind(wire.durable.scope.tenant_id)
            .bind(consumer)
            .bind(wire.durable.node_id)
            .bind(&wire.durable.run_id)
            .bind(wire.durable.seq)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}
