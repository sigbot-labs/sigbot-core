// SPDX-License-Identifier: GPL-3.0-only
//! Deterministic spot matching at the supplied available market time.
use anyhow::{ensure, Context, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    order::{
        execution::{ExecutionFact, OrderReceipt, OrderState},
        intent::{IntentOrderType, OrderSide, TradeIntent},
    },
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchingConfig {
    pub scope: MessageScope,
    pub node_id: i64,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub instrument: InstrumentSpec,
    pub token_env: String,
    #[serde(default)]
    pub virtual_clock: bool,
}

pub struct SimulatedExchange {
    pub(super) pool: PgPool,
    pub(super) config: MatchingConfig,
    token: String,
}

impl SimulatedExchange {
    pub fn new(pool: PgPool, config: MatchingConfig) -> Result<Self> {
        config.scope.validate()?;
        config.instrument.validate()?;
        ensure!(
            !["live", "production"].contains(&config.scope.env.as_str()),
            "simulation requires an isolated environment"
        );
        ensure!(
            config.node_id > 0
                && config.fee_bps >= Decimal::ZERO
                && config.slippage_bps >= Decimal::ZERO
                && config.slippage_bps < Decimal::from(10_000),
            "invalid deterministic matching configuration"
        );
        let token = std::env::var(&config.token_env)?;
        ensure!(!token.is_empty(), "simulation access token cannot be empty");
        Ok(Self { pool, config, token })
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/orders", post(Self::submit_http))
            .route("/orders/reconcile", post(Self::reconcile_http))
            .with_state(self)
    }

    pub fn config(&self) -> &MatchingConfig {
        &self.config
    }

    fn authorize(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let presented = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if presented == self.token {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }

    async fn submit_http(
        State(this): State<Arc<Self>>,
        headers: HeaderMap,
        Json(message): Json<DurableMessage<TradeIntent>>,
    ) -> Result<Json<OrderReceipt>, StatusCode> {
        this.authorize(&headers)?;
        let clock = if this.config.virtual_clock {
            sqlx::query_scalar("SELECT virtual_time FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND state='running'")
                .bind(&this.config.scope.env).bind(this.config.scope.tenant_id).bind(this.config.scope.space_id).bind(&message.run_id)
                .fetch_one(&this.pool).await.map_err(|_|StatusCode::CONFLICT)?
        } else {
            message.available_time
        };
        this.submit(&message, clock).await.map(Json).map_err(|error| {
            tracing::warn!(error=%error, "simulation rejected intent");
            StatusCode::UNPROCESSABLE_ENTITY
        })
    }

    async fn reconcile_http(
        State(this): State<Arc<Self>>,
        headers: HeaderMap,
        Json(message): Json<DurableMessage<TradeIntent>>,
    ) -> Result<Json<OrderReceipt>, StatusCode> {
        this.authorize(&headers)?;
        this.reconcile(&message)
            .await
            .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?
            .map(Json)
            .ok_or(StatusCode::NOT_FOUND)
    }

    pub async fn reconcile(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        message.validate()?;
        ensure!(message.scope == self.config.scope, "replay scope mismatch");
        let row = sqlx::query(
            "SELECT exchange_order_id,intent FROM simulated_order WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND account_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.scope.space_id)
        .bind(message.payload.account_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            ensure!(
                row.get::<Value, _>("intent") == serde_json::to_value(&message.payload)?,
                "replay order identity collision"
            );
            Ok(OrderReceipt {
                exchange_order_id: row.get("exchange_order_id"),
                state: OrderState::Acknowledged,
            })
        })
        .transpose()
    }

    pub async fn submit(&self, input: &DurableMessage<TradeIntent>, clock: DateTime<Utc>) -> Result<OrderReceipt> {
        input.validate()?;
        ensure!(
            input.scope == self.config.scope && input.account_id == Some(input.payload.account_id),
            "replay scope/account mismatch"
        );
        input.payload.validate(clock, 300_000)?;
        let intent = &input.payload;
        ensure!(
            intent.exchange == self.config.instrument.exchange && intent.symbol == self.config.instrument.symbol,
            "simulated intent differs from configured instrument"
        );
        ensure!(
            intent.quantity % self.config.instrument.quantity_step == Decimal::ZERO
                && intent
                    .limit_price
                    .is_none_or(|price| price % self.config.instrument.price_tick == Decimal::ZERO),
            "simulated intent violates instrument precision"
        );
        let exchange_order_id = format!(
            "sim:{}:{}:{}:{}",
            intent.account_id, input.node_id, input.run_id, input.seq
        );
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query("INSERT INTO simulated_order(env,tenant_id,space_id,account_id,run_id,seq,
            exchange_order_id,intent,state,accepted_at,producer_node_id,original_message,matching_node_id,matching_configuration)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,'accepted',$9,$10,$11,$12,$13)
            ON CONFLICT DO NOTHING")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(intent.account_id)
            .bind(&input.run_id).bind(input.seq).bind(&exchange_order_id).bind(serde_json::to_value(intent)?)
            .bind(clock).bind(input.node_id).bind(serde_json::to_value(input)?).bind(self.config.node_id)
            .bind(self.matching_configuration()?).execute(&mut *tx).await?.rows_affected() == 1;
        if !inserted {
            tx.rollback().await?;
            return self
                .reconcile(input)
                .await?
                .ok_or_else(|| anyhow::anyhow!("order disappeared during replay"));
        }
        if let Some(price) = self.crossing_price(intent, intent.market.price)? {
            self.fill_in(&mut tx, input, &exchange_order_id, price, clock, clock, clock)
                .await?;
        }
        tx.commit().await?;
        Ok(OrderReceipt {
            exchange_order_id,
            state: OrderState::Acknowledged,
        })
    }

    pub(super) fn matching_configuration(&self) -> Result<Value> {
        Ok(
            json!({"instrument":self.config.instrument,"fee_bps":self.config.fee_bps,"slippage_bps":self.config.slippage_bps}),
        )
    }

    pub(super) fn crossing_price(&self, intent: &TradeIntent, observed: Decimal) -> Result<Option<Decimal>> {
        let adjustment = self.config.slippage_bps / Decimal::from(10_000);
        let multiplier = if intent.side == OrderSide::Buy {
            Decimal::ONE + adjustment
        } else {
            Decimal::ONE - adjustment
        };
        let ticks = observed
            .checked_mul(multiplier)
            .and_then(|price| price.checked_div(self.config.instrument.price_tick))
            .context("simulated price overflow")?;
        let ticks = if intent.side == OrderSide::Buy {
            ticks.ceil()
        } else {
            ticks.floor()
        };
        let price = ticks
            .checked_mul(self.config.instrument.price_tick)
            .context("simulated tick overflow")?;
        ensure!(price > Decimal::ZERO, "simulated execution price must be positive");
        let crosses = intent.order_type == IntentOrderType::Market
            || intent.limit_price.is_some_and(|limit| {
                if intent.side == OrderSide::Buy {
                    price <= limit
                } else {
                    price >= limit
                }
            });
        Ok(crosses.then_some(price))
    }

    pub(super) async fn fill_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<TradeIntent>,
        exchange_order_id: &str,
        price: Decimal,
        event_time: DateTime<Utc>,
        available_time: DateTime<Utc>,
        clock: DateTime<Utc>,
    ) -> Result<()> {
        let intent = &input.payload;
        let quote_quantity = price
            .checked_mul(intent.quantity)
            .context("simulated notional overflow")?;
        let fee = quote_quantity
            .checked_mul(self.config.fee_bps)
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .context("simulated fee overflow")?;
        let fact = ExecutionFact {
            reservation_node_id: input.node_id,
            reservation_seq: input.seq,
            wallet_id: intent.wallet_id,
            account_id: intent.account_id,
            exchange: intent.exchange.clone(),
            exchange_order_id: exchange_order_id.to_owned(),
            exchange_trade_id: format!("{exchange_order_id}:0"),
            symbol: intent.symbol.clone(),
            base_asset: self.config.instrument.base_asset.clone(),
            quote_asset: self.config.instrument.quote_asset.clone(),
            side: intent.side,
            price,
            quantity: intent.quantity,
            quote_quantity: Some(quote_quantity),
            fee,
            fee_asset: self.config.instrument.quote_asset.clone(),
            executed_at: event_time,
            order_complete: true,
        };
        fact.validate()?;
        let seq = PostgresMessageStore::next_sequence(tx, &input.scope, self.config.node_id, &input.run_id).await?;
        let producer_span = MessageTrace::child("backtest.execution", &input.traceparent);
        let output = DurableMessage {
            version: 2,
            scope: input.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: input.account_id,
            node_id: self.config.node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time,
            available_time,
            produced_at: clock,
            traceparent: producer_span.traceparent(),
            payload: serde_json::to_value(fact)?,
        };
        PostgresMessageStore::enqueue(
            tx,
            &self.producer(),
            &AccountTopic::ExecutionEvent.topic(&input.scope, intent.account_id)?,
            &output,
        )
        .await?;
        let changed = sqlx::query(
                "UPDATE simulated_order SET state='filled',matched_at=$7 WHERE env=$1 AND tenant_id=$2
                AND space_id=$3 AND account_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$8 AND state='accepted'",
            )
            .bind(&input.scope.env)
            .bind(input.scope.tenant_id)
            .bind(input.scope.space_id)
            .bind(intent.account_id)
            .bind(&input.run_id)
            .bind(input.seq)
            .bind(event_time)
            .bind(input.node_id)
            .execute(&mut **tx)
            .await?.rows_affected();
        ensure!(changed == 1, "simulated order no longer owns its pending fill");
        Ok(())
    }

    pub fn producer(&self) -> String {
        let scope = &self.config.scope;
        format!(
            "backtest:{}:{}:{}:{}",
            scope.env, scope.tenant_id, scope.space_id, self.config.node_id
        )
    }
}
