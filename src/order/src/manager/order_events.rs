// SPDX-License-Identifier: GPL-3.0-only
use anyhow::Result;
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    order::{execution::OrderState, intent::TradeIntent},
};
use sqlx::{Postgres, Transaction};

pub(crate) struct OrderEventWriter;
impl OrderEventWriter {
    pub fn owner(scope: &MessageScope, workflow_id: i64, node_id: i64) -> String {
        format!(
            "order:{}:{}:{}:{workflow_id}:{node_id}",
            scope.env, scope.tenant_id, scope.space_id
        )
    }
    pub fn fact_owner(kind: &str, scope: &MessageScope, workflow_id: i64, node_id: i64) -> String {
        format!(
            "order.{kind}:{}:{}:{}:{workflow_id}:{node_id}",
            scope.env, scope.tenant_id, scope.space_id
        )
    }
    pub async fn enqueue(
        tx: &mut Transaction<'_, Postgres>,
        producer: &str,
        node_id: i64,
        input: &DurableMessage<TradeIntent>,
        state: OrderState,
        exchange_order_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let seq = PostgresMessageStore::next_sequence(tx, &input.scope, node_id, &input.run_id).await?;
        let span = MessageTrace::child("order.publish", &input.traceparent);
        let message = DurableMessage {
            version: 2,
            scope: input.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: input.account_id,
            node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: span.traceparent(),
            payload: serde_json::json!({"state":state,"exchange_order_id":exchange_order_id,
                "wallet_id":input.payload.wallet_id,"reservation_node_id":input.node_id,"reservation_seq":input.seq}),
        };
        PostgresMessageStore::enqueue(
            tx,
            producer,
            &AccountTopic::OrderEvent.topic(&input.scope, input.payload.account_id)?,
            &message,
        )
        .await?;
        Ok(())
    }
}
