// SPDX-License-Identifier: GPL-3.0-only

use crate::exchange::ExecutionMode;
use crate::exchange::ExecutionPage;
use crate::exchange::IExecutionAdapter;
use crate::exchange::PreparedSubmission;
use crate::exchange::SubmissionIdentity;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use binance_sdk::config::ConfigurationRestApi;
use binance_sdk::spot::rest_api::GetOrderParams;
use binance_sdk::spot::rest_api::MyTradesParams;
use binance_sdk::spot::rest_api::NewOrderNewOrderRespTypeEnum;
use binance_sdk::spot::rest_api::NewOrderParams;
use binance_sdk::spot::rest_api::NewOrderSideEnum;
use binance_sdk::spot::rest_api::NewOrderTimeInForceEnum;
use binance_sdk::spot::rest_api::NewOrderTypeEnum;
use binance_sdk::spot::rest_api::RestApi;
use binance_sdk::spot::SpotRestApi;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use serde::Deserialize;
use sigbot_core::config::config::get_config;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceExecutionConfig {
    pub scope: MessageScope,
    pub account_id: Uuid,
    pub api_key_secret: String,
    pub api_secret_secret: String,
    pub min_request_interval_ms: u64,
}

pub struct BinanceSpotExecution {
    scope: MessageScope,
    account_id: Uuid,
    client: RestApi,
    next_request: Mutex<Instant>,
    interval: Duration,
}

impl BinanceSpotExecution {
    pub fn new(config: &BinanceExecutionConfig, timeout_seconds: u64) -> Result<Arc<Self>> {
        let BinanceExecutionConfig {
            scope,
            account_id,
            api_key_secret,
            api_secret_secret,
            min_request_interval_ms,
        } = config;
        scope.validate()?;
        ensure!(
            account_id.get_version_num() == 7 && (50..=60_000).contains(min_request_interval_ms),
            "invalid live account/rate configuration"
        );
        let app_config = get_config();
        let settings = ConfigurationRestApi::builder()
            .api_key(app_config.secrets.require(api_key_secret)?.to_owned())
            .api_secret(app_config.secrets.require(api_secret_secret)?.to_owned())
            .timeout(timeout_seconds * 1000)
            .retries(0)
            .build()?;
        Ok(Arc::new(Self {
            scope: scope.clone(),
            account_id: *account_id,
            client: SpotRestApi::from_config(settings),
            next_request: Mutex::new(Instant::now()),
            interval: Duration::from_millis(*min_request_interval_ms),
        }))
    }

    /// The venue requires a <=36-character client order ID. This is only a deterministic
    /// encoding of the existing business key, never a separate correlation identity.
    pub fn client_order_id(message: &DurableMessage<TradeIntent>) -> Result<String> {
        SubmissionIdentity::venue_reference(message)
    }

    async fn authorize(&self, message: &DurableMessage<TradeIntent>) -> Result<()> {
        message.validate()?;
        ensure!(
            message.scope == self.scope
                && message.account_id == Some(self.account_id)
                && message.payload.account_id == self.account_id
                && message.payload.exchange.eq_ignore_ascii_case("binance"),
            "exchange client scope/account/provider mismatch"
        );
        let mut next = self.next_request.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.interval;
        Ok(())
    }

    fn receipt(id: Option<i64>, status: Option<&str>) -> Result<OrderReceipt> {
        let state = match status.unwrap_or("NEW") {
            "NEW" | "PENDING_NEW" => OrderState::Acknowledged,
            "PARTIALLY_FILLED" => OrderState::PartiallyFilled,
            "FILLED" => OrderState::Filled,
            "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => OrderState::Cancelled,
            "REJECTED" => OrderState::Rejected,
            _ => OrderState::Unknown,
        };
        Ok(OrderReceipt {
            exchange_order_id: id.context("Binance order receipt lacks ID")?.to_string(),
            state,
        })
    }
}

#[async_trait]
impl IExecutionAdapter for BinanceSpotExecution {
    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<()> {
        instrument.validate()?;
        ensure!(
            instrument.exchange.eq_ignore_ascii_case("binance")
                && instrument.symbol == format!("{}{}", instrument.base_asset, instrument.quote_asset),
            "Binance spot instrument assets differ from its symbol"
        );
        Ok(())
    }
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Live
    }

    async fn read_executions(
        &self,
        input: &DurableMessage<TradeIntent>,
        receipt: &OrderReceipt,
        _prepared: Option<&PreparedSubmission>,
        instrument: &InstrumentSpec,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ExecutionPage> {
        self.authorize(input).await?;
        instrument.validate()?;
        ensure!(
            (1..=1000).contains(&limit)
                && instrument.exchange == input.payload.exchange
                && instrument.symbol == input.payload.symbol,
            "invalid Binance execution query instrument/limit"
        );
        let order_id: i64 = receipt
            .exchange_order_id
            .parse()
            .context("invalid Binance venue order ID")?;
        let after: Option<i64> = cursor
            .map(str::parse)
            .transpose()
            .context("invalid Binance trade cursor")?;
        let from_id = Some(
            after
                .map(|id| id.checked_add(1).context("Binance trade cursor overflow"))
                .transpose()?
                .unwrap_or(0),
        );
        let params = MyTradesParams::builder(instrument.symbol.clone())
            .order_id(Some(order_id))
            .from_id(from_id)
            .limit(Some(i32::from(limit)))
            .build()?;
        let trades = self
            .client
            .my_trades(params)
            .await
            .map_err(|_| anyhow::anyhow!("Binance trade reconciliation unavailable"))?
            .data()
            .await
            .map_err(|_| anyhow::anyhow!("Binance trade response unavailable"))?;
        ensure!(
            trades.len() <= usize::from(limit),
            "Binance trade response exceeds limit"
        );
        let has_more = trades.len() == usize::from(limit);
        let mut next_cursor = cursor.map(str::to_owned);
        let mut facts = Vec::with_capacity(trades.len());
        for trade in trades {
            let trade_id = trade.id.context("Binance trade ID missing")?;
            ensure!(
                trade.order_id == Some(order_id)
                    && trade.symbol.as_deref() == Some(&instrument.symbol)
                    && after.is_none_or(|after| trade_id > after),
                "Binance trade identity/cursor mismatch"
            );
            let side = if trade.is_buyer.context("Binance trade side missing")? {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            ensure!(side == input.payload.side, "Binance trade side differs from intent");
            let fact = ExecutionFact {
                reservation_node_id: input.node_id,
                reservation_seq: input.seq,
                wallet_id: input.payload.wallet_id,
                account_id: self.account_id,
                exchange: input.payload.exchange.clone(),
                exchange_order_id: receipt.exchange_order_id.clone(),
                exchange_trade_id: trade_id.to_string(),
                symbol: instrument.symbol.clone(),
                base_asset: instrument.base_asset.clone(),
                quote_asset: instrument.quote_asset.clone(),
                side,
                price: trade.price.context("Binance trade price missing")?.parse()?,
                quantity: trade.qty.context("Binance trade quantity missing")?.parse()?,
                quote_quantity: Some(
                    trade
                        .quote_qty
                        .context("Binance gross quote quantity missing")?
                        .parse()?,
                ),
                fee: trade.commission.context("Binance trade commission missing")?.parse()?,
                fee_asset: trade.commission_asset.context("Binance commission asset missing")?,
                order_complete: false,
                executed_at: DateTime::from_timestamp_millis(trade.time.context("Binance execution time missing")?)
                    .context("invalid Binance execution timestamp")?,
            };
            fact.validate()?;
            next_cursor = Some(
                next_cursor
                    .as_ref()
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(-1)
                    .max(trade_id)
                    .to_string(),
            );
            facts.push(fact);
        }
        facts.sort_by(|left, right| {
            left.executed_at
                .cmp(&right.executed_at)
                .then(left.exchange_trade_id.cmp(&right.exchange_trade_id))
        });
        self.authorize(input).await?;
        let order = self
            .client
            .get_order(
                GetOrderParams::builder(instrument.symbol.clone())
                    .order_id(Some(order_id))
                    .build()?,
            )
            .await
            .map_err(|_| anyhow::anyhow!("Binance order status unavailable"))?
            .data()
            .await
            .map_err(|_| anyhow::anyhow!("Binance order status body unavailable"))?;
        ensure!(
            order.order_id == Some(order_id)
                && order.client_order_id.as_deref() == Some(&Self::client_order_id(input)?),
            "Binance status does not match persisted intent"
        );
        let receipt = Self::receipt(order.order_id, order.status.as_deref())?;
        let status = if receipt.state == OrderState::Unknown {
            None
        } else {
            let status = OrderStatusFact {
                reservation_node_id: input.node_id,
                reservation_seq: input.seq,
                wallet_id: input.payload.wallet_id,
                account_id: self.account_id,
                exchange: input.payload.exchange.clone(),
                receipt,
                executed_quantity: order
                    .executed_qty
                    .context("Binance cumulative executed quantity missing")?
                    .parse()?,
                occurred_at: Utc::now(),
            };
            status.validate()?;
            Some(status)
        };
        Ok(ExecutionPage {
            facts,
            status,
            next_cursor,
            has_more,
        })
    }

    async fn submit(&self, message: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        self.authorize(message).await?;
        let intent = &message.payload;
        let side = if intent.side == OrderSide::Buy {
            NewOrderSideEnum::Buy
        } else {
            NewOrderSideEnum::Sell
        };
        let kind = match intent.order_type {
            IntentOrderType::Market => NewOrderTypeEnum::Market,
            IntentOrderType::Limit => NewOrderTypeEnum::Limit,
        };
        let mut params = NewOrderParams::builder(intent.symbol.clone(), side, kind)
            .quantity(Some(intent.quantity))
            .new_client_order_id(Some(Self::client_order_id(message)?))
            .new_order_resp_type(Some(NewOrderNewOrderRespTypeEnum::Ack));
        if intent.order_type == IntentOrderType::Limit {
            params = params
                .price(intent.limit_price)
                .time_in_force(Some(NewOrderTimeInForceEnum::Gtc));
        }
        // SDK retry is disabled: even an HTTP timeout can mean venue acceptance.
        let reply = self
            .client
            .new_order(params.build()?)
            .await
            .map_err(|_| anyhow::anyhow!("Binance submission result uncertain; reconcile by business key"))?
            .data()
            .await
            .map_err(|_| anyhow::anyhow!("Binance acceptance body unavailable; reconcile"))?;
        Self::receipt(reply.order_id, reply.status.as_deref())
    }

    async fn reconcile(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        self.authorize(message).await?;
        let params = GetOrderParams::builder(message.payload.symbol.clone())
            .orig_client_order_id(Some(Self::client_order_id(message)?))
            .build()?;
        let reply = self
            .client
            .get_order(params)
            .await
            .map_err(|_| anyhow::anyhow!("Binance reconciliation unavailable; preserve unknown order and reservation"))?
            .data()
            .await
            .map_err(|_| anyhow::anyhow!("Binance reconciliation body unavailable"))?;
        Ok(Some(Self::receipt(reply.order_id, reply.status.as_deref())?))
    }
}

#[async_trait::async_trait]
impl IProvider for BinanceSpotExecution {
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
