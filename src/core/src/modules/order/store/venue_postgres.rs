// SPDX-License-Identifier: GPL-3.0-only
//! Leased read-only venue polling and immutable observations, owned by Order.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::modules::{
    messager::{envelope::DurableMessage, topic::MessageScope},
    order::{
        execution::{OrderReceipt, OrderState},
        intent::TradeIntent,
    },
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct VenuePoll {
    pub input: DurableMessage<TradeIntent>,
    pub receipt: OrderReceipt,
    pub prepared: Option<Value>,
    pub cursor: Option<String>,
    pub version: i64,
}
pub struct VenuePostgresRepository;
impl VenuePostgresRepository {
    pub async fn claim(
        pool: &PgPool,
        scope: &MessageScope,
        workflow_id: i64,
        account_id: i64,
        consumer: &str,
    ) -> Result<Option<VenuePoll>> {
        let mut tx = pool.begin().await?;
        let candidate=sqlx::query("SELECT i.payload,o.state,o.exchange_order_id,o.prepared_submission FROM order_record o
            JOIN messager_inbox i ON i.env=o.env AND i.tenant_id::text=o.tenant_id AND i.space_id=o.space_id
                AND i.node_id=o.producer_node_id AND i.run_id=o.run_id AND i.seq=o.seq AND i.consumer=$4
            LEFT JOIN order_venue_checkpoint c ON c.wallet_id=o.wallet_id AND c.producer_node_id=o.producer_node_id AND c.run_id=o.run_id AND c.seq=o.seq
            WHERE o.env=$1 AND o.tenant_id=$2 AND o.space_id=$3 AND (i.payload->>'workflow_id')::bigint=$5
                AND (i.payload->>'account_id')::bigint=$6 AND o.exchange_order_id IS NOT NULL
                AND o.state IN ('acknowledged','partially_filled','filled','cancelled','rejected')
                AND COALESCE(c.complete,FALSE)=FALSE AND COALESCE(c.next_poll_at,'-infinity')<=NOW()
            ORDER BY COALESCE(c.next_poll_at,o.updated_at) LIMIT 1 FOR UPDATE OF o SKIP LOCKED")
            .bind(&scope.env).bind(scope.tenant_id.to_string()).bind(scope.space_id).bind(consumer).bind(workflow_id).bind(account_id)
            .fetch_optional(&mut *tx).await?;
        let Some(candidate) = candidate else {
            tx.commit().await?;
            return Ok(None);
        };
        let input: DurableMessage<TradeIntent> = serde_json::from_value(candidate.get("payload"))?;
        sqlx::query("INSERT INTO order_venue_checkpoint(wallet_id,producer_node_id,run_id,seq) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(input.payload.wallet_id).bind(input.node_id).bind(&input.run_id).bind(input.seq).execute(&mut *tx).await?;
        let lease = sqlx::query(
            "UPDATE order_venue_checkpoint SET version=version+1,next_poll_at=NOW()+interval '150 seconds'
            WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 AND NOT complete AND next_poll_at<=NOW()
            RETURNING version,cursor",
        )
        .bind(input.payload.wallet_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_optional(&mut *tx)
        .await?;
        let result = lease
            .map(|lease| -> Result<VenuePoll> {
                Ok(VenuePoll {
                    input,
                    receipt: OrderReceipt {
                        exchange_order_id: candidate.get("exchange_order_id"),
                        state: serde_json::from_value(Value::String(candidate.get("state")))?,
                    },
                    prepared: candidate.get("prepared_submission"),
                    cursor: lease.get("cursor"),
                    version: lease.get("version"),
                })
            })
            .transpose()?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn owns(tx: &mut Transaction<'_, Postgres>, poll: &VenuePoll) -> Result<bool> {
        let current: i64 = sqlx::query_scalar(
            "SELECT version FROM order_venue_checkpoint WHERE wallet_id=$1 AND producer_node_id=$2
            AND run_id=$3 AND seq=$4 FOR UPDATE",
        )
        .bind(poll.input.payload.wallet_id)
        .bind(poll.input.node_id)
        .bind(&poll.input.run_id)
        .bind(poll.input.seq)
        .fetch_one(&mut **tx)
        .await?;
        Ok(current == poll.version)
    }

    pub async fn observe(
        tx: &mut Transaction<'_, Postgres>,
        poll: &VenuePoll,
        kind: &str,
        reference: &str,
        payload: &Value,
    ) -> Result<bool> {
        let inserted = sqlx::query(
            "INSERT INTO order_venue_observation(wallet_id,exchange,kind,venue_reference,exchange_order_id,payload)
            VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
        )
        .bind(poll.input.payload.wallet_id)
        .bind(&poll.input.payload.exchange)
        .bind(kind)
        .bind(reference)
        .bind(&poll.receipt.exchange_order_id)
        .bind(payload)
        .execute(&mut **tx)
        .await?
        .rows_affected()
            == 1;
        if !inserted {
            let mut prior:Value=sqlx::query_scalar("SELECT payload FROM order_venue_observation WHERE wallet_id=$1 AND exchange=$2 AND kind=$3 AND venue_reference=$4")
                .bind(poll.input.payload.wallet_id).bind(&poll.input.payload.exchange).bind(kind).bind(reference).fetch_one(&mut **tx).await?;
            let mut current = payload.clone();
            // Repeated venue snapshots can report the same cumulative state at a
            // later observation time. Keep the first durable observation.
            if kind == "status" {
                prior
                    .as_object_mut()
                    .context("stored venue status must be an object")?
                    .remove("occurred_at");
                current
                    .as_object_mut()
                    .context("venue status must be an object")?
                    .remove("occurred_at");
            }
            ensure!(prior == current, "venue fact identity reused with different contents");
        }
        Ok(inserted)
    }

    pub async fn finish(
        tx: &mut Transaction<'_, Postgres>,
        poll: &VenuePoll,
        cursor: Option<&str>,
        has_more: bool,
        terminal: Option<(OrderState, Decimal)>,
    ) -> Result<()> {
        let quantity: Decimal = sqlx::query_scalar(
            "SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM order_venue_observation
            WHERE wallet_id=$1 AND exchange=$2 AND exchange_order_id=$3 AND kind='execution'",
        )
        .bind(poll.input.payload.wallet_id)
        .bind(&poll.input.payload.exchange)
        .bind(&poll.receipt.exchange_order_id)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            quantity <= poll.input.payload.quantity,
            "venue overfill requires reconciliation"
        );
        let complete = !has_more
            && terminal.is_some_and(|(state, expected)| {
                matches!(state, OrderState::Filled | OrderState::Cancelled | OrderState::Rejected)
                    && quantity == expected
            });
        sqlx::query(
            "UPDATE order_venue_checkpoint SET cursor=$5,complete=$6,next_poll_at=NOW()+interval '2 seconds'
            WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(poll.input.payload.wallet_id)
        .bind(poll.input.node_id)
        .bind(&poll.input.run_id)
        .bind(poll.input.seq)
        .bind(cursor)
        .bind(complete)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn retry(pool: &PgPool, poll: &VenuePoll, after: DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE order_venue_checkpoint SET next_poll_at=$6 WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 AND version=$5")
            .bind(poll.input.payload.wallet_id).bind(poll.input.node_id).bind(&poll.input.run_id).bind(poll.input.seq).bind(poll.version).bind(after).execute(pool).await?;
        Ok(())
    }
}
