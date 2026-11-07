// SPDX-License-Identifier: GPL-3.0-only

use super::OrderExecutionPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::{ensure, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::{AccountTopic, MessageScope};
use sigbot_types::order::execution::{OrderState, OrderStatusFact};
use sigbot_types::order::intent::TradeIntent;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

pub struct OrderStatusPostgresRepository {
    execution: OrderExecutionPostgresRepository,
}

impl OrderStatusPostgresRepository {
    pub fn new(
        storage: &OrderPostgresRepository,
        scope: MessageScope,
        node_id: i64,
        source_node_ids: Vec<i64>,
        workflow_id: Uuid,
    ) -> Result<Self> {
        Ok(Self {
            execution: OrderExecutionPostgresRepository::new(storage, scope, node_id, source_node_ids, workflow_id)?,
        })
    }

    pub async fn process_status(&self, message: &MessageEnvelope<OrderStatusFact>) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        self.execution.validate_source(input, fact.account_id)?;
        ensure!(input.event_time == fact.occurred_at, "status event time mismatch");
        let topic = AccountTopic::OrderEvent.topic(&self.execution.scope, fact.account_id)?;
        let consumer = OrderPostgresRepository::fact_owner(
            "status",
            &self.execution.scope,
            self.execution.workflow_id,
            self.execution.node_id,
        );
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let mut tx = self.execution.pool.begin().await?;
        let order = self
            .execution
            .lock_order(
                &mut tx,
                fact.wallet_id,
                fact.account_id,
                fact.reservation_node_id,
                &input.run_id,
                fact.reservation_seq,
            )
            .await?;
        let intent: TradeIntent = serde_json::from_value(order.get("intent"))?;
        ensure!(
            intent.exchange == fact.exchange && fact.executed_quantity <= intent.quantity,
            "venue cumulative quantity exceeds intent"
        );
        let exchange_order_id: Option<String> = order.get("exchange_order_id");
        ensure!(
            exchange_order_id
                .as_deref()
                .is_none_or(|id| id == fact.receipt.exchange_order_id),
            "venue order identity changed"
        );
        if !MessagerPostgresRepository::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        let previous: String = order.get("state");
        let mut status = fact.clone();
        if previous == "filled" {
            status.receipt.state = OrderState::Filled;
            status.executed_quantity = intent.quantity;
        } else if previous == "cancelled" {
            status.receipt.state = OrderState::Cancelled;
        } else if previous == "partially_filled" && status.receipt.state == OrderState::Acknowledged {
            status.receipt.state = OrderState::PartiallyFilled;
        }
        // Status alone does not fabricate ord_execution rows or wallet postings.
        self.execution.record(&mut tx, &wire.durable, status).await?;
        MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(true)
    }
}

impl OrderExecutionPostgresRepository {
    pub(crate) async fn record(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<Value>,
        status: OrderStatusFact,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE order_info SET state=$7,exchange_order_id=$8,updated_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(status.wallet_id)
        .bind(status.reservation_node_id)
        .bind(&input.run_id)
        .bind(status.reservation_seq)
        .bind(status.receipt.state.as_str())
        .bind(&status.receipt.exchange_order_id)
        .execute(&mut **transaction)
        .await?;
        let sequence =
            MessagerPostgresRepository::next_sequence(transaction, &self.scope, self.node_id, &input.run_id).await?;
        let output = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: Some(status.account_id),
            node_id: self.node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq: sequence,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: Utc::now(),
            traceparent: MessageTrace::child("order.execution.publish", &input.traceparent).traceparent(),
            payload: serde_json::to_value(&status)?,
        };
        MessagerPostgresRepository::enqueue(
            transaction,
            &OrderPostgresRepository::owner(&self.scope, self.workflow_id, self.node_id),
            &AccountTopic::OrderEvent.topic(&self.scope, status.account_id)?,
            &output,
        )
        .await?;
        Ok(())
    }
}
