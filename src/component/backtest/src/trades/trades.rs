// SPDX-License-Identifier: GPL-3.0-only

use crate::backtest::IBacktest;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Json;
use axum::Router;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::backtest::handler::backtest::{BacktestHandler, BacktestTransaction, RestingOrderCursor};
use sigbot_messager::messager::IMessager;
use sigbot_types::backtest::BacktestProvider;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchingFeedConfig {
    workflow_id: Uuid,
    run_id: Uuid,
    input_node_ids: Vec<i64>,
}

struct FillTimes {
    event: DateTime<Utc>,
    available: DateTime<Utc>,
    clock: DateTime<Utc>,
}

pub struct TradesBacktest {
    properties: HashMap<String, String>,
    messager: Arc<dyn IMessager>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    history: Mutex<Option<Arc<crate::history::history::HistoryBacktest>>>,
}
impl TradesBacktest {
    pub fn new(properties: HashMap<String, String>, messager: Arc<dyn IMessager>) -> Arc<Self> {
        Arc::new(Self {
            properties,
            messager,
            tasks: Mutex::new(Vec::new()),
            history: Mutex::new(None),
        })
    }
}
#[async_trait::async_trait]
impl IProvider for TradesBacktest {
    async fn init(&self) -> Result<()> {
        let properties = &self.properties;
        let messager = self.messager.clone();
        common_telemetry::readiness::RuntimeReadiness::business("backtest.execution", false);
        let config: MatchingConfig =
            serde_json::from_str(properties.get("matching_config").context("matching_config required")?)?;
        let bind_addr = properties.get("bind_addr").context("execution bind_addr required")?;
        let database_url = get_config().storage.postgres.connection_url()?;
        let exchange = Arc::new(BacktestExchange::connect(&database_url, config).await?);
        let producer = exchange.producer();
        if let Some(raw) = properties.get("matching_feed_config") {
            let feed: MatchingFeedConfig = serde_json::from_str(raw)?;
            ensure!(
                !exchange.config().virtual_clock
                    && feed.workflow_id.get_version_num() == 7
                    && feed.run_id.get_version_num() == 7
                    && !feed.input_node_ids.is_empty()
                    && feed.input_node_ids.iter().all(|id| *id > 0),
                "Paper matching requires explicit source edges; Replay drives its own clock"
            );
            let topic = WorkflowTopic::FeedMarket.topic(&exchange.config().scope, feed.workflow_id)?;
            let matcher = exchange.clone();
            let routing_handler = exchange.handler.clone();
            let expected_topic = topic.clone();
            messager
                .subscribe(
                    &topic,
                    Arc::new(move |bytes| {
                        let matcher = matcher.clone();
                        let handler = routing_handler.clone();
                        let topic = expected_topic.clone();
                        let run = feed.run_id.clone();
                        let owners = feed.input_node_ids.clone();
                        Box::pin(async move {
                            let wire: MessageEnvelope<DatafeedEvent> = serde_json::from_slice(&bytes)?;
                            wire.validate()?;
                            wire.durable.validate_topic(&topic)?;
                            if wire.durable.run_id != run
                                || !owners.contains(&wire.durable.node_id)
                                || wire
                                    .durable
                                    .target_node_id
                                    .is_some_and(|id| id != matcher.config().node_id)
                            {
                                let value: MessageEnvelope<Value> = serde_json::from_slice(&bytes)?;
                                let consumer = format!("{}.routing:{run}", matcher.producer());
                                handler.quarantine_route(&consumer, &topic, &value).await?;
                                return Ok("outside matching route".into());
                            }
                            matcher.match_tick(&wire, Utc::now()).await?;
                            Ok("committed".into())
                        })
                    }),
                )
                .await?;
        }
        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.listener");
        let server_exchange = exchange.clone();
        self.tasks.lock().await.push(tokio::spawn(async move {
            readiness.set(true);
            if let Err(error) = axum::serve(listener, server_exchange.router()).await {
                tracing::error!(%error, "backtest execution listener stopped");
            }
        }));
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.outbox");
        self.tasks.lock().await.push(tokio::spawn(async move {
            readiness.set(true);
            let store = exchange.handler.messager();
            loop {
                if let Err(error) = sigbot_messager::messager::OutboxRelay::dispatch_pending(
                    &store,
                    &producer,
                    messager.as_ref(),
                    32,
                    &|parent| Ok(MessageTrace::child("backtest.retry", parent).traceparent()),
                )
                .await
                {
                    tracing::warn!(%error, "backtest execution relay pending");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }));
        common_telemetry::readiness::RuntimeReadiness::business("backtest.execution", true);
        if properties.contains_key("replay_config") {
            *self.history.lock().await =
                Some(crate::history::history::HistoryBacktest::start(properties, self.messager.clone()).await?);
        }
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        if let Some(history) = self.history.lock().await.take() {
            history.close();
        }
        for task in self.tasks.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
        common_telemetry::readiness::RuntimeReadiness::business("backtest.execution", false);
        Ok(())
    }
}
#[async_trait::async_trait]
impl IBacktest for TradesBacktest {
    fn provider(&self) -> BacktestProvider {
        BacktestProvider::Trades
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchingConfig {
    pub scope: MessageScope,
    pub node_id: i64,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub instrument: InstrumentSpec,
    pub token_secret: String,
    #[serde(default)]
    pub virtual_clock: bool,
}

pub struct BacktestExchange {
    pub(super) handler: BacktestHandler,
    pub(super) config: MatchingConfig,
    token: String,
}

impl BacktestExchange {
    pub async fn connect(database_url: &str, config: MatchingConfig) -> Result<Self> {
        config.scope.validate()?;
        config.instrument.validate()?;
        ensure!(
            config.node_id > 0
                && config.fee_bps >= Decimal::ZERO
                && config.slippage_bps >= Decimal::ZERO
                && config.slippage_bps < Decimal::from(10_000),
            "invalid deterministic matching configuration"
        );
        let token = get_config().secrets.require(&config.token_secret)?.to_owned();
        ensure!(!token.is_empty(), "simulation access token cannot be empty");
        let handler = BacktestHandler::connect(database_url, config.scope.clone(), config.node_id).await?;
        Ok(Self { handler, config, token })
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
            this.handler
                .virtual_time(&message.run_id)
                .await
                .map_err(|_| StatusCode::CONFLICT)?
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
        Ok(self
            .handler
            .reconcile(message)
            .await?
            .map(|exchange_order_id| OrderReceipt {
                exchange_order_id,
                state: OrderState::Acknowledged,
            }))
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
            "backtest intent differs from configured instrument"
        );
        ensure!(
            intent.quantity % self.config.instrument.quantity_step == Decimal::ZERO
                && intent
                    .limit_price
                    .is_none_or(|price| price % self.config.instrument.price_tick == Decimal::ZERO),
            "backtest intent violates instrument precision"
        );
        let exchange_order_id = format!(
            "sim:{}:{}:{}:{}",
            intent.account_id, input.node_id, input.run_id, input.seq
        );
        let mut transaction = self.handler.begin().await?;
        let inserted = self
            .handler
            .insert_order(
                &mut transaction,
                input,
                &exchange_order_id,
                clock,
                &self.matching_configuration()?,
            )
            .await?;
        if !inserted {
            transaction.rollback().await?;
            return self
                .reconcile(input)
                .await?
                .ok_or_else(|| anyhow::anyhow!("order disappeared during replay"));
        }
        if let Some(price) = self.crossing_price(intent, intent.market.price)? {
            self.fill_in(
                &mut transaction,
                input,
                &exchange_order_id,
                price,
                FillTimes {
                    event: clock,
                    available: clock,
                    clock,
                },
            )
            .await?;
        }
        transaction.commit().await?;
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
            .context("backtest price overflow")?;
        let ticks = if intent.side == OrderSide::Buy {
            ticks.ceil()
        } else {
            ticks.floor()
        };
        let price = ticks
            .checked_mul(self.config.instrument.price_tick)
            .context("backtest tick overflow")?;
        ensure!(price > Decimal::ZERO, "backtest execution price must be positive");
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

    async fn fill_in(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<TradeIntent>,
        exchange_order_id: &str,
        price: Decimal,
        times: FillTimes,
    ) -> Result<()> {
        let FillTimes {
            event: event_time,
            available: available_time,
            clock,
        } = times;
        let intent = &input.payload;
        let quote_quantity = price
            .checked_mul(intent.quantity)
            .context("backtest notional overflow")?;
        let fee = quote_quantity
            .checked_mul(self.config.fee_bps)
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .context("backtest fee overflow")?;
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
        let seq = self.handler.next_sequence(transaction, &input.run_id).await?;
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
        self.handler
            .fill(
                transaction,
                &self.producer(),
                &AccountTopic::ExecutionEvent.topic(&input.scope, intent.account_id)?,
                input,
                event_time,
                &output,
            )
            .await
    }

    pub fn producer(&self) -> String {
        let scope = &self.config.scope;
        format!("backtest:{}:{}:{}", scope.env, scope.tenant_id, self.config.node_id)
    }
}

impl BacktestExchange {
    pub async fn match_tick(&self, wire: &MessageEnvelope<DatafeedEvent>, clock: DateTime<Utc>) -> Result<usize> {
        wire.validate()?;
        let input = &wire.durable;
        let topic = WorkflowTopic::FeedMarket
            .topic(&self.config.scope, input.workflow_id.context("tick workflow required")?)?;
        let consumer = format!("{}.market", self.producer());
        let value = serde_json::from_value(serde_json::to_value(wire)?)?;
        let Some(mut transaction) = self.handler.accept_market(&consumer, &topic, &value).await? else {
            return Ok(0);
        };
        let matches = self.match_tick_in(&mut transaction, input, clock).await?;
        self.handler
            .complete_market(transaction, &consumer, &value.durable)
            .await?;
        Ok(matches)
    }

    /// Replay calls this inside the history-admission transaction. Imported
    /// market facts, fills, Outbox and the virtual clock become visible together.
    pub async fn match_tick_in(
        &self,
        transaction: &mut BacktestTransaction,
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
        self.handler.verify_tick(transaction, input).await?;
        input.workflow_id.context("tick workflow required")?;
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
        let mut cursor: Option<RestingOrderCursor> = None;
        let mut scanned = 0;
        let mut matched = 0;
        loop {
            let rows = self
                .handler
                .resting_orders(
                    transaction,
                    input,
                    cursor.as_ref(),
                    &self.config.instrument.exchange,
                    &self.config.instrument.symbol,
                )
                .await?;
            if rows.is_empty() {
                break;
            }
            scanned += rows.len();
            ensure!(
                scanned <= 100_000,
                "resting-order scan exceeded the per-tick safety bound"
            );
            for row in rows {
                cursor = Some(row.cursor);
                ensure!(
                    row.matching_configuration == self.matching_configuration()?,
                    "resting order matching configuration changed after acceptance"
                );
                let order = row.message;
                if let Some(price) = self.crossing_price(&order.payload, price)? {
                    self.fill_in(
                        transaction,
                        &order,
                        &row.exchange_order_id,
                        price,
                        FillTimes {
                            event: fact.event_time,
                            available: fact.available_time,
                            clock,
                        },
                    )
                    .await?;
                    matched += 1;
                }
            }
        }
        Ok(matched)
    }
}
