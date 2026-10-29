// SPDX-License-Identifier: GPL-3.0-only
//! A group never treats an uncertain order as failed or a receipt as a fill.
use super::order_events::OrderEventWriter;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::{MessageScope, WorkflowTopic},
    },
    order::intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent},
};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};

pub struct ExecutionGroups {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: i64,
    node_id: i64,
    producer: String,
    virtual_clock: bool,
}
struct GroupIdentity {
    producer_node_id: i64,
    run_id: String,
    seq: i64,
    workflow_id: i64,
    traceparent: String,
}
struct SettledLeg {
    index: i32,
    order_node_id: i64,
    intent: TradeIntent,
    filled: Decimal,
    state: String,
}

impl ExecutionGroups {
    pub fn new(
        pool: PgPool,
        scope: MessageScope,
        workflow_id: i64,
        node_id: i64,
        producer: String,
        virtual_clock: bool,
    ) -> Self {
        Self {
            pool,
            scope,
            workflow_id,
            node_id,
            producer,
            virtual_clock,
        }
    }

    /// One transaction and row lock select the configured coordinator's work.
    pub async fn advance(&self, now: DateTime<Utc>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let group=sqlx::query("SELECT * FROM execution_group WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND coordinator_node_id=$4 AND workflow_id=$5
            AND state NOT IN ('completed','held','compensated','compensation_failed') ORDER BY updated_at LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(self.node_id).bind(self.workflow_id).fetch_optional(&mut *tx).await?;
        let Some(group) = group else {
            tx.commit().await?;
            return Ok(());
        };
        let identity = GroupIdentity {
            producer_node_id: group.get("producer_node_id"),
            run_id: group.get("run_id"),
            seq: group.get("seq"),
            workflow_id: group.get("workflow_id"),
            traceparent: group.get("traceparent"),
        };
        let now = if self.virtual_clock {
            sqlx::query_scalar::<_,DateTime<Utc>>("SELECT virtual_time FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(&identity.run_id).fetch_one(&mut *tx).await?
        } else {
            now
        };
        let legs = sqlx::query(
            "SELECT * FROM execution_group_leg WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND producer_node_id=$4 AND run_id=$5 AND group_seq=$6 ORDER BY leg_index",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(self.scope.space_id)
        .bind(identity.producer_node_id)
        .bind(&identity.run_id)
        .bind(identity.seq)
        .fetch_all(&mut *tx)
        .await?;
        let prior: String = group.get("state");
        let next = if prior == "compensating" {
            self.compensation_state(&mut tx, &identity, &legs).await?
        } else {
            self.original_state(&mut tx, &identity, &group, &legs, now).await?
        };
        sqlx::query(
            "UPDATE execution_group SET state=$7,updated_at=NOW() WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND producer_node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(self.scope.space_id)
        .bind(identity.producer_node_id)
        .bind(&identity.run_id)
        .bind(identity.seq)
        .bind(next)
        .execute(&mut *tx)
        .await?;
        if next != prior {
            let seq = PostgresMessageStore::next_sequence(&mut tx, &self.scope, self.node_id, &identity.run_id).await?;
            let event = self.message(
                &identity,
                seq,
                None,
                None,
                now,
                json!({"event":"execution_group","producer_node_id":identity.producer_node_id,
                "group_seq":identity.seq,"state":next}),
            );
            PostgresMessageStore::enqueue(
                &mut tx,
                &self.producer,
                &WorkflowTopic::RuntimeEvent.topic(&self.scope, identity.workflow_id)?,
                &event,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn original_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        group: &PgRow,
        legs: &[PgRow],
        now: DateTime<Utc>,
    ) -> Result<&'static str> {
        let expired = now > group.get::<DateTime<Utc>, _>("deadline");
        let complete = legs.len() == usize::try_from(group.get::<i32, _>("leg_count"))?;
        if !complete && !expired {
            return Ok("pending");
        }
        let mut settled = Vec::new();
        for leg in legs {
            let intent: TradeIntent = serde_json::from_value(leg.get("intent"))?;
            let progress = self
                .settled(
                    tx,
                    identity,
                    &intent,
                    identity.producer_node_id,
                    leg.get("intent_seq"),
                    leg.get("order_node_id"),
                )
                .await?;
            let progress = if progress.is_none() && leg.get::<bool, _>("rejected") {
                Some(("rejected".into(), Decimal::ZERO))
            } else {
                progress
            };
            let Some((state, filled)) = progress else {
                return Ok(if expired { "reconciling" } else { "executing" });
            };
            settled.push(SettledLeg {
                index: leg.get("leg_index"),
                order_node_id: leg.get("order_node_id"),
                intent,
                filled,
                state,
            });
        }
        if complete && settled.iter().all(|leg| leg.state == "filled") {
            return Ok("completed");
        }
        if group.get::<String, _>("failure_policy") == "hold" {
            return Ok("held");
        }
        self.compensate(tx, identity, &settled, now).await
    }

    /// Require venue terminal cumulative quantity AND Wallet booking, not merely
    /// currently observed executions (which may both be missing a late fill).
    async fn settled(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        intent: &TradeIntent,
        producer_node: i64,
        seq: i64,
        order_node: i64,
    ) -> Result<Option<(String, Decimal)>> {
        let order = sqlx::query(
            "SELECT state,exchange_order_id FROM order_record WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND wallet_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id.to_string())
        .bind(self.scope.space_id)
        .bind(intent.wallet_id)
        .bind(producer_node)
        .bind(&identity.run_id)
        .bind(seq)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(order) = order else {
            return Ok(None);
        };
        let state: String = order.get("state");
        let expected=match state.as_str() {
            "filled"=>Some(intent.quantity),
            "rejected" if order.get::<Option<String>,_>("exchange_order_id").is_none()=>Some(Decimal::ZERO),
            "cancelled"|"rejected"=>sqlx::query_scalar::<_,Option<Decimal>>("SELECT MAX((payload->'payload'->>'executed_quantity')::numeric)
                FROM messager_outbox WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND node_id=$4 AND run_id=$5
                AND (payload->'payload'->>'wallet_id')::bigint=$6 AND (payload->'payload'->>'reservation_node_id')::bigint=$7
                AND (payload->'payload'->>'reservation_seq')::bigint=$8 AND payload->'payload'->'receipt'->>'state' IN ('cancelled','rejected')")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(order_node).bind(&identity.run_id)
                .bind(intent.wallet_id).bind(producer_node).bind(seq).fetch_one(&mut **tx).await?,
            _=>return Ok(None),
        };
        let Some(expected) = expected else {
            return Ok(None);
        };
        let booked:Decimal=sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wallet_journal WHERE wallet_id=$1
            AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
            .bind(intent.wallet_id).bind(&identity.run_id).bind(producer_node).bind(seq).fetch_one(&mut **tx).await?;
        let locked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3
            AND seq=$4 AND state='reserved')",
        )
        .bind(intent.wallet_id)
        .bind(producer_node)
        .bind(&identity.run_id)
        .bind(seq)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            booked <= intent.quantity,
            "group execution exceeded original intent quantity"
        );
        Ok((booked == expected && !locked).then_some((state, booked)))
    }

    async fn compensate(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        legs: &[SettledLeg],
        now: DateTime<Utc>,
    ) -> Result<&'static str> {
        let mut prepared = Vec::new();
        for leg in legs.iter().filter(|leg| leg.filled > Decimal::ZERO) {
            let row=sqlx::query("SELECT event_at,available_at,(payload->>'price')::numeric AS price FROM market_event WHERE env=$1 AND tenant_id=$2
                AND space_id=$3 AND source=$4 AND symbol=$5 AND event_type='market' AND event_at<=$6 AND available_at<=$6
                AND event_at>=$6-interval '30 seconds' ORDER BY event_at DESC,revision DESC LIMIT 1")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(&leg.intent.market.source)
                .bind(&leg.intent.symbol).bind(now).fetch_optional(&mut **tx).await?;
            let Some(row) = row else {
                return Ok("awaiting_market");
            };
            let mut intent = leg.intent.clone();
            intent.execution_group = None;
            intent.quantity = leg.filled;
            intent.side = if intent.side == OrderSide::Buy {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            intent.order_type = IntentOrderType::Market;
            intent.limit_price = None;
            intent.reduce_only = true;
            intent.market = MarketEvidence {
                source: intent.market.source.clone(),
                price: row.try_get("price")?,
                event_time: row.get("event_at"),
                available_time: row.get("available_at"),
            };
            intent.validate(now, 30_000)?;
            prepared.push((leg, intent));
        }
        if prepared.is_empty() {
            return Ok("compensated");
        }
        for (leg, intent) in prepared {
            let seq = PostgresMessageStore::next_sequence(tx, &self.scope, self.node_id, &identity.run_id).await?;
            let payload = serde_json::to_value(&intent)?;
            sqlx::query("UPDATE execution_group_leg SET compensation_seq=$8,compensation_intent=$9 WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                AND producer_node_id=$4 AND run_id=$5 AND group_seq=$6 AND leg_index=$7 AND compensation_seq IS NULL")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(identity.producer_node_id).bind(&identity.run_id)
                .bind(identity.seq).bind(leg.index).bind(seq).bind(&payload).execute(&mut **tx).await?;
            let output = self.message(
                identity,
                seq,
                Some(intent.account_id),
                Some(leg.order_node_id),
                now,
                payload,
            );
            PostgresMessageStore::enqueue(
                tx,
                &self.producer,
                &WorkflowTopic::StrategySignal.topic(&self.scope, identity.workflow_id)?,
                &output,
            )
            .await?;
        }
        Ok("compensating")
    }

    async fn compensation_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        legs: &[PgRow],
    ) -> Result<&'static str> {
        for leg in legs {
            let Some(seq) = leg.get::<Option<i64>, _>("compensation_seq") else {
                continue;
            };
            let intent: TradeIntent = serde_json::from_value(leg.get::<Value, _>("compensation_intent"))?;
            let Some((state, _)) = self
                .settled(tx, identity, &intent, self.node_id, seq, leg.get("order_node_id"))
                .await?
            else {
                // A hard-risk rejection has no order row but is durably quarantined.
                let rejected: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM messager_inbox WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                    AND node_id=$4 AND run_id=$5 AND seq=$6 AND state='quarantined' AND consumer=$7)",
                )
                .bind(&self.scope.env)
                .bind(self.scope.tenant_id)
                .bind(self.scope.space_id)
                .bind(self.node_id)
                .bind(&identity.run_id)
                .bind(seq)
                .bind(OrderEventWriter::owner(
                    &self.scope,
                    identity.workflow_id,
                    leg.get::<i64, _>("order_node_id"),
                ))
                .fetch_one(&mut **tx)
                .await?;
                return Ok(if rejected {
                    "compensation_failed"
                } else {
                    "compensating"
                });
            };
            if state != "filled" {
                return Ok("compensation_failed");
            }
        }
        Ok("compensated")
    }

    fn message(
        &self,
        identity: &GroupIdentity,
        seq: i64,
        account_id: Option<i64>,
        target_node_id: Option<i64>,
        now: DateTime<Utc>,
        payload: Value,
    ) -> DurableMessage<Value> {
        let span = MessageTrace::child("order.group.publish", &identity.traceparent);
        DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: Some(identity.workflow_id),
            account_id,
            node_id: self.node_id,
            target_node_id,
            run_id: identity.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: span.traceparent(),
            payload,
        }
    }
}
