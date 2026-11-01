// SPDX-License-Identifier: GPL-3.0-only

use super::{GroupIdentity, OrderGroupPostgresRepository, SettledLeg};
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::Result;
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageKind};
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use uuid::Uuid;

pub struct OrderCompensationPostgresRepository;

impl OrderCompensationPostgresRepository {
    pub(crate) async fn compensate(
        group: &OrderGroupPostgresRepository,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        legs: &[SettledLeg],
        now: DateTime<Utc>,
    ) -> Result<&'static str> {
        let mut prepared = Vec::new();
        for leg in legs.iter().filter(|leg| leg.filled > Decimal::ZERO) {
            let row=sqlx::query("SELECT event_at,available_at,(payload->>'price')::numeric AS price FROM mkt_event WHERE env=$1 AND tenant_id=$2
                AND run_id=$3 AND source=$4 AND symbol=$5 AND event_type='market' AND event_at<=$6 AND available_at<=$6
                AND event_at>=$6-interval '30 seconds' ORDER BY event_at DESC,revision DESC LIMIT 1")
                .bind(&group.scope.env).bind(group.scope.tenant_id).bind(&identity.run_id).bind(&leg.intent.market.source)
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
            let seq =
                MessagerPostgresRepository::next_sequence(tx, &group.scope, group.node_id, &identity.run_id).await?;
            let payload = serde_json::to_value(&intent)?;
            sqlx::query(
                "UPDATE ord_execution_leg SET compensation_seq=$7,compensation_intent=$8 WHERE env=$1 AND tenant_id=$2
                AND producer_node_id=$3 AND run_id=$4 AND group_seq=$5 AND leg_index=$6 AND compensation_seq IS NULL",
            )
            .bind(&group.scope.env)
            .bind(group.scope.tenant_id)
            .bind(identity.producer_node_id)
            .bind(&identity.run_id)
            .bind(identity.seq)
            .bind(leg.index)
            .bind(seq)
            .bind(&payload)
            .execute(&mut **tx)
            .await?;
            let output = Self::message(
                group,
                identity,
                seq,
                Some(intent.account_id),
                Some(leg.order_node_id),
                now,
                payload,
            );
            MessagerPostgresRepository::enqueue(
                tx,
                &group.producer,
                &WorkflowTopic::StrategySignal.topic(&group.scope, identity.workflow_id)?,
                &output,
            )
            .await?;
        }
        Ok("compensating")
    }

    pub(crate) async fn compensation_state(
        group: &OrderGroupPostgresRepository,
        tx: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        legs: &[PgRow],
    ) -> Result<&'static str> {
        for leg in legs {
            let Some(seq) = leg.get::<Option<i64>, _>("compensation_seq") else {
                continue;
            };
            let intent: TradeIntent = serde_json::from_value(leg.get::<Value, _>("compensation_intent"))?;
            let Some((state, _)) = group
                .settled(tx, identity, &intent, group.node_id, seq, leg.get("order_node_id"))
                .await?
            else {
                // A hard-risk rejection has no order row but is durably quarantined.
                let rejected: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM msg_inbox WHERE env=$1 AND tenant_id=$2
                    AND node_id=$3 AND run_id=$4 AND seq=$5 AND state='quarantined' AND consumer=$6)",
                )
                .bind(&group.scope.env)
                .bind(group.scope.tenant_id)
                .bind(group.node_id)
                .bind(&identity.run_id)
                .bind(seq)
                .bind(OrderPostgresRepository::owner(
                    &group.scope,
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

    pub(crate) fn message(
        group: &OrderGroupPostgresRepository,
        identity: &GroupIdentity,
        seq: i64,
        account_id: Option<Uuid>,
        target_node_id: Option<i64>,
        now: DateTime<Utc>,
        payload: Value,
    ) -> DurableMessage<Value> {
        let span = MessageTrace::child("order.group.publish", &identity.traceparent);
        DurableMessage {
            version: 2,
            scope: group.scope.clone(),
            workflow_id: Some(identity.workflow_id),
            account_id,
            node_id: group.node_id,
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
