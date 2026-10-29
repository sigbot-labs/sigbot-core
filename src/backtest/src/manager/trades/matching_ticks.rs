// SPDX-License-Identifier: GPL-3.0-only
//! Resting limit orders consume persisted, available ticks in a fixed order.
use super::simulated_exchange::SimulatedExchange;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    datafeed::event::DatafeedEvent,
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::WorkflowTopic,
    },
    order::intent::TradeIntent,
};
use sqlx::{Postgres, Row, Transaction};
use std::str::FromStr;

impl SimulatedExchange {
    pub async fn match_tick(&self, wire: &MessageEnvelope<DatafeedEvent>, clock: DateTime<Utc>) -> Result<usize> {
        wire.validate()?;
        let input = &wire.durable;
        let topic = WorkflowTopic::FeedMarket
            .topic(&self.config.scope, input.workflow_id.context("tick workflow required")?)?;
        let consumer = format!("{}.market", self.producer());
        let value = serde_json::from_value(serde_json::to_value(wire)?)?;
        let mut tx = self.pool.begin().await?;
        let matches = if PostgresMessageStore::accept(&mut tx, &consumer, &topic, &value).await? {
            let matched = self.match_tick_in(&mut tx, input, clock).await?;
            PostgresMessageStore::complete(&mut tx, &consumer, &value.durable).await?;
            matched
        } else {
            0
        };
        tx.commit().await?;
        Ok(matches)
    }

    /// Replay calls this inside the history-admission transaction. Imported
    /// market facts, fills, Outbox and the virtual clock become visible together.
    pub async fn match_tick_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<DatafeedEvent>,
        clock: DateTime<Utc>,
    ) -> Result<usize> {
        input.validate()?;
        let fact = &input.payload;
        ensure!(
            input.scope == self.config.scope
                && input.kind == MessageKind::Event
                && input.target_node_id.is_none_or(|node| node == self.config.node_id)
                && fact.event_type == "market"
                && fact.symbol == self.config.instrument.symbol
                && fact.event_time == input.event_time
                && fact.available_time == input.available_time
                && fact.event_time <= fact.available_time
                && fact.available_time <= clock
                && fact.revision >= 0,
            "matching requires an available market tick in the configured scope/instrument"
        );
        let workflow = input.workflow_id.context("tick workflow required")?;
        let stored = sqlx::query("SELECT payload,event_at,available_at,seq FROM market_event m
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND source=$4 AND source_event_id=$5 AND revision=$6
            AND symbol=$7 AND event_type='market' AND (run_id=$8 OR EXISTS(SELECT 1 FROM market_event_delivery d
                WHERE d.env=m.env AND d.tenant_id=m.tenant_id AND d.space_id=m.space_id AND d.source=m.source
                AND d.source_event_id=m.source_event_id AND d.revision=m.revision AND d.run_id=$8 AND d.workflow_id=$9))")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(&fact.source)
            .bind(&fact.source_event_id).bind(fact.revision).bind(&fact.symbol).bind(&input.run_id).bind(workflow)
            .fetch_one(&mut **tx).await.context("tick must first be persisted by Datafeed")?;
        ensure!(
            stored.get::<Value, _>("payload") == fact.raw
                && stored.get::<DateTime<Utc>, _>("event_at").timestamp_micros() == fact.event_time.timestamp_micros()
                && stored.get::<DateTime<Utc>, _>("available_at").timestamp_micros()
                    == fact.available_time.timestamp_micros()
                && stored.get::<i64, _>("seq") == fact.seq,
            "tick differs from its durable source fact"
        );
        let raw_price = fact
            .raw
            .get("price")
            .or_else(|| fact.raw.get("lastPrice"))
            .context("tick price missing")?;
        let price = Decimal::from_str(
            &raw_price
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| raw_price.to_string()),
        )?;
        ensure!(price > Decimal::ZERO, "invalid tick price");
        let mut cursor: Option<(DateTime<Utc>, i64, i64, i64)> = None;
        let mut scanned = 0;
        let mut matched = 0;
        loop {
            let rows = sqlx::query("SELECT accepted_at,producer_node_id,seq,account_id,exchange_order_id,original_message,matching_configuration
                FROM simulated_order WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND matching_node_id=$4 AND run_id=$5
                AND state='accepted' AND accepted_at<=$6 AND intent->>'order_type'='LIMIT'
                AND intent->>'exchange'=$7 AND intent->>'symbol'=$8 AND intent->'market'->>'source'=$9
                AND (original_message->>'workflow_id')::bigint=$10
                AND ($11::timestamptz IS NULL OR (accepted_at,producer_node_id,seq,account_id)>($11,$12,$13,$14))
                ORDER BY accepted_at,producer_node_id,seq,account_id LIMIT 128 FOR UPDATE")
                .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(self.config.node_id)
                .bind(&input.run_id).bind(fact.event_time).bind(&self.config.instrument.exchange)
                .bind(&self.config.instrument.symbol).bind(&fact.source).bind(workflow)
                .bind(cursor.map(|cursor|cursor.0)).bind(cursor.map(|cursor|cursor.1))
                .bind(cursor.map(|cursor|cursor.2)).bind(cursor.map(|cursor|cursor.3))
                .fetch_all(&mut **tx).await?;
            if rows.is_empty() {
                break;
            }
            scanned += rows.len();
            ensure!(
                scanned <= 100_000,
                "resting-order scan exceeded the per-tick safety bound"
            );
            for row in rows {
                cursor = Some((
                    row.get("accepted_at"),
                    row.get("producer_node_id"),
                    row.get("seq"),
                    row.get("account_id"),
                ));
                ensure!(
                    row.get::<Value, _>("matching_configuration") == self.matching_configuration()?,
                    "resting order matching configuration changed after acceptance"
                );
                let order: DurableMessage<TradeIntent> = serde_json::from_value(row.get("original_message"))?;
                if let Some(price) = self.crossing_price(&order.payload, price)? {
                    self.fill_in(
                        tx,
                        &order,
                        row.get::<String, _>("exchange_order_id").as_str(),
                        price,
                        fact.event_time,
                        fact.available_time,
                        clock,
                    )
                    .await?;
                    matched += 1;
                }
            }
        }
        Ok(matched)
    }
}
