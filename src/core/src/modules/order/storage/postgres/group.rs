// SPDX-License-Identifier: GPL-3.0-only
//! A group never treats an uncertain order as failed or a receipt as a fill.

use super::OrderCompensationPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::{ensure, Result};
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::intent::TradeIntent;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

pub struct OrderGroupPostgresRepository {
    pub(crate) pool: PgPool,
    pub(crate) scope: MessageScope,
    pub(crate) workflow_id: Uuid,
    pub(crate) node_id: i64,
    pub(crate) producer: String,
    virtual_clock: bool,
}
pub(crate) struct GroupIdentity {
    pub(crate) producer_node_id: i64,
    pub(crate) run_id: Uuid,
    pub(crate) seq: i64,
    pub(crate) workflow_id: Uuid,
    pub(crate) traceparent: String,
}
pub(crate) struct SettledLeg {
    pub(crate) index: i32,
    pub(crate) order_node_id: i64,
    pub(crate) intent: TradeIntent,
    pub(crate) filled: Decimal,
    pub(crate) state: String,
}

impl OrderGroupPostgresRepository {
    pub fn new(
        storage: &OrderPostgresRepository,
        scope: MessageScope,
        workflow_id: Uuid,
        node_id: i64,
        producer: String,
        virtual_clock: bool,
    ) -> Self {
        Self {
            pool: storage.pool.clone(),
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
        let group=sqlx::query("SELECT * FROM ord_execution_group WHERE env=$1 AND tenant_id=$2 AND coordinator_node_id=$3 AND workflow_id=$4
            AND state NOT IN ('completed','held','compensated','compensation_failed') ORDER BY updated_at LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.node_id).bind(self.workflow_id).fetch_optional(&mut *tx).await?;
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
            sqlx::query_scalar::<_, DateTime<Utc>>(
                "SELECT virtual_time FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2 AND run_id=$3",
            )
            .bind(&self.scope.env)
            .bind(self.scope.tenant_id)
            .bind(&identity.run_id)
            .fetch_one(&mut *tx)
            .await?
        } else {
            now
        };
        let legs = sqlx::query(
            "SELECT * FROM ord_execution_leg WHERE env=$1 AND tenant_id=$2
            AND producer_node_id=$3 AND run_id=$4 AND group_seq=$5 ORDER BY leg_index",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(identity.producer_node_id)
        .bind(&identity.run_id)
        .bind(identity.seq)
        .fetch_all(&mut *tx)
        .await?;
        let prior: String = group.get("state");
        let next = if prior == "compensating" {
            OrderCompensationPostgresRepository::compensation_state(self, &mut tx, &identity, &legs).await?
        } else {
            self.original_state(&mut tx, &identity, &group, &legs, now).await?
        };
        sqlx::query(
            "UPDATE ord_execution_group SET state=$6,updated_at=NOW() WHERE env=$1 AND tenant_id=$2
            AND producer_node_id=$3 AND run_id=$4 AND seq=$5",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(identity.producer_node_id)
        .bind(&identity.run_id)
        .bind(identity.seq)
        .bind(next)
        .execute(&mut *tx)
        .await?;
        if next != prior {
            let seq =
                MessagerPostgresRepository::next_sequence(&mut tx, &self.scope, self.node_id, &identity.run_id).await?;
            let event = OrderCompensationPostgresRepository::message(
                self,
                &identity,
                seq,
                None,
                None,
                now,
                json!({"event":"ord_execution_group","producer_node_id":identity.producer_node_id,
                "group_seq":identity.seq,"state":next}),
            );
            MessagerPostgresRepository::enqueue(
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
        transaction: &mut Transaction<'_, Postgres>,
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
                    transaction,
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
        OrderCompensationPostgresRepository::compensate(self, transaction, identity, &settled, now).await
    }

    pub(crate) async fn settled(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        identity: &GroupIdentity,
        intent: &TradeIntent,
        producer_node: i64,
        sequence: i64,
        order_node: i64,
    ) -> Result<Option<(String, Decimal)>> {
        let order = sqlx::query(
            "SELECT state,exchange_order_id FROM order_info WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(intent.wallet_id)
        .bind(producer_node)
        .bind(&identity.run_id)
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await?;
        let Some(order) = order else {
            return Ok(None);
        };
        let state: String = order.get("state");
        let expected=match state.as_str() {
            "filled"=>Some(intent.quantity),
            "rejected" if order.get::<Option<String>,_>("exchange_order_id").is_none()=>Some(Decimal::ZERO),
            "cancelled"|"rejected"=>sqlx::query_scalar::<_,Option<Decimal>>("SELECT MAX((payload->'payload'->>'executed_quantity')::numeric)
                FROM msg_outbox WHERE env=$1 AND tenant_id=$2 AND node_id=$3 AND run_id=$4
                AND (payload->'payload'->>'wallet_id')::uuid=$5 AND (payload->'payload'->>'reservation_node_id')::bigint=$6
                AND (payload->'payload'->>'reservation_seq')::bigint=$7 AND payload->'payload'->'receipt'->>'state' IN ('cancelled','rejected')")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(order_node).bind(&identity.run_id)
                .bind(intent.wallet_id).bind(producer_node).bind(sequence).fetch_one(&mut **transaction).await?,
            _=>return Ok(None),
        };
        let Some(expected) = expected else {
            return Ok(None);
        };
        let booked:Decimal=sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wlt_journal WHERE wallet_id=$1
            AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
            .bind(intent.wallet_id).bind(&identity.run_id).bind(producer_node).bind(sequence).fetch_one(&mut **transaction).await?;
        let locked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3
            AND seq=$4 AND state='reserved')",
        )
        .bind(intent.wallet_id)
        .bind(producer_node)
        .bind(&identity.run_id)
        .bind(sequence)
        .fetch_one(&mut **transaction)
        .await?;
        ensure!(
            booked <= intent.quantity,
            "group execution exceeded original intent quantity"
        );
        Ok((booked == expected && !locked).then_some((state, booked)))
    }
}
