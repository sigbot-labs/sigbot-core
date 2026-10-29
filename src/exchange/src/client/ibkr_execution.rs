// SPDX-License-Identifier: GPL-3.0-only
//! TWS / IB Gateway execution through rust-ibapi's typed, bounded async API.
use super::{
    execution_adapter::{ExecutionMode, ExecutionPage, IExecutionAdapter, PreparedSubmission, SubmissionIdentity},
    prepared_payload::{PreparedKeyConfig, PreparedPayloadCipher},
};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use ibapi::{
    contracts::{Contract, SecurityType},
    orders::{
        order_builder, Action, CommissionReport, ExecutionData, ExecutionFilter, ExecutionSide, Executions, Order,
        OrderData, OrderStatusKind, Orders, PlaceOrder,
    },
    subscriptions::SubscriptionItem,
    Client,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{envelope::DurableMessage, topic::MessageScope},
    order::{
        execution::{ExecutionFact, OrderReceipt, OrderState, OrderStatusFact},
        intent::{IntentOrderType, OrderSide, TradeIntent},
    },
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use time_tz::PrimitiveDateTimeExt;
use tokio::{sync::Mutex, time::Instant};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IbkrExecutionConfig {
    pub scope: MessageScope,
    pub account_id: i64,
    pub broker_account_env: String,
    pub gateway_address: String,
    /// Exclusive API client ID for this tenant/account/environment owner.
    pub client_id: i32,
    pub min_request_interval_ms: u64,
    /// Symbols map directly to the SDK's contract, with an explicit conId.
    pub contracts: BTreeMap<String, Contract>,
    pub prepared_keys: PreparedKeyConfig,
}

pub struct IbkrExecution {
    config: IbkrExecutionConfig,
    account: String,
    cipher: PreparedPayloadCipher,
    client: Mutex<Option<Arc<Client>>>,
    next_request: Mutex<Instant>,
    timeout: Duration,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedIbkrOrder {
    contract: Contract,
    order: Order,
}

impl IbkrExecution {
    pub(crate) fn new(config: &IbkrExecutionConfig, timeout_seconds: u64) -> Result<Self> {
        config.scope.validate()?;
        ensure!(
            config.account_id > 0 && config.client_id > 0,
            "invalid IBKR scoped account/client ID"
        );
        ensure!(
            (50..=60_000).contains(&config.min_request_interval_ms),
            "invalid IBKR request interval"
        );
        ensure!(
            !config.contracts.is_empty() && config.contracts.len() <= 1024,
            "IBKR contracts must be bounded"
        );
        ensure!(
            config.contracts.values().all(|contract| contract.contract_id > 0),
            "IBKR requires resolved contract IDs"
        );
        ensure!(
            config
                .contracts
                .values()
                .all(|contract| contract.security_type == SecurityType::Stock),
            "IBKR execution currently supports stock/ETF contracts with unit notional only"
        );
        let account = std::env::var(&config.broker_account_env).context("IBKR account environment variable missing")?;
        ensure!(!account.is_empty(), "IBKR broker account is empty");
        Ok(Self {
            config: config.clone(),
            account,
            cipher: PreparedPayloadCipher::new(&config.prepared_keys)?,
            client: Mutex::new(None),
            next_request: Mutex::new(Instant::now()),
            timeout: Duration::from_secs(timeout_seconds),
        })
    }

    async fn authorize(&self, message: &DurableMessage<TradeIntent>) -> Result<()> {
        message.validate()?;
        ensure!(
            message.scope == self.config.scope
                && message.account_id == Some(self.config.account_id)
                && message.payload.account_id == self.config.account_id
                && message.payload.exchange.eq_ignore_ascii_case("ibkr"),
            "IBKR client scope/account/provider mismatch"
        );
        let mut next = self.next_request.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + Duration::from_millis(self.config.min_request_interval_ms);
        Ok(())
    }

    async fn client(&self) -> Result<Arc<Client>> {
        let mut slot = self.client.lock().await;
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }
        // A disconnected submission is never replayed by the SDK. A later reconciliation
        // may establish a fresh connection, using the persisted venue order identity.
        let client = tokio::time::timeout(
            self.timeout,
            Client::builder()
                .address(&self.config.gateway_address)
                .client_id(self.config.client_id)
                .max_reconnect_attempts(0)
                .one_shot_timeout(self.timeout)
                .connect(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("IBKR gateway connection timed out"))?
        .map_err(|_| anyhow::anyhow!("IBKR gateway connection failed"))?;
        let accounts = tokio::time::timeout(self.timeout, client.managed_accounts())
            .await
            .map_err(|_| anyhow::anyhow!("IBKR account verification timed out"))?
            .map_err(|_| anyhow::anyhow!("IBKR account verification failed"))?;
        ensure!(
            accounts.contains(&self.account),
            "IBKR gateway does not manage configured account"
        );
        let client = Arc::new(client);
        *slot = Some(client.clone());
        Ok(client)
    }

    pub fn exact_sdk_number(value: Decimal) -> Result<f64> {
        let number = value.to_f64().context("IBKR numeric conversion failed")?;
        ensure!(
            number.is_finite() && number.to_string().parse::<Decimal>()? == value,
            "IBKR SDK numeric precision would change the order"
        );
        Ok(number)
    }

    fn restore(
        &self,
        message: &DurableMessage<TradeIntent>,
        prepared: &PreparedSubmission,
    ) -> Result<PreparedIbkrOrder> {
        let value: PreparedIbkrOrder =
            self.cipher
                .open("ibkr", message, &prepared.venue_order_id, &prepared.payload)?;
        ensure!(
            value.order.client_id == self.config.client_id
                && value.order.account == self.account
                && value.order.order_ref == SubmissionIdentity::venue_reference(message)?
                && prepared.venue_order_id == format!("ibkr:{}:{}", self.config.client_id, value.order.order_id),
            "IBKR prepared order account/identity mismatch"
        );
        Ok(value)
    }

    pub fn state(status: &OrderStatusKind, filled: f64, remaining: Option<f64>) -> OrderState {
        match status {
            OrderStatusKind::Filled => OrderState::Filled,
            OrderStatusKind::Cancelled => OrderState::Cancelled,
            OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted if filled > 0.0 => {
                if remaining == Some(0.0) {
                    OrderState::Filled
                } else {
                    OrderState::PartiallyFilled
                }
            }
            OrderStatusKind::Submitted | OrderStatusKind::PreSubmitted => OrderState::Acknowledged,
            // Inactive is ambiguous (rejected OR cancelled); retain the reservation.
            _ => OrderState::Unknown,
        }
    }

    fn matching_data(&self, data: &OrderData, expected: &PreparedIbkrOrder) -> bool {
        data.order_id == expected.order.order_id
            && data.order.client_id == self.config.client_id
            && data.order.account == self.account
            && data.order.order_ref == expected.order.order_ref
            && data.contract.contract_id == expected.contract.contract_id
    }

    pub fn execution_timestamp(value: &str, gateway_zone: Option<&'static time_tz::Tz>) -> Result<DateTime<Utc>> {
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
            return Ok(timestamp.with_timezone(&Utc));
        }
        let parts = value.split_whitespace().collect::<Vec<_>>();
        let (date, zone) = if parts.first().is_some_and(|part| part.contains('-')) {
            ensure!(parts.len() <= 2, "invalid IBKR execution timestamp");
            let format = time::macros::format_description!("[year][month][day]-[hour]:[minute]:[second]");
            let date = time::PrimitiveDateTime::parse(parts[0], format)?;
            if parts.len() == 1 {
                let instant = date.assume_utc();
                return DateTime::from_timestamp(instant.unix_timestamp(), instant.nanosecond())
                    .context("invalid IBKR UTC timestamp");
            }
            (
                date,
                time_tz::timezones::get_by_name(parts[1]).context("unknown IBKR execution timezone")?,
            )
        } else {
            ensure!(matches!(parts.len(), 2 | 3), "IBKR execution date/time required");
            let format = time::macros::format_description!("[year][month][day] [hour]:[minute]:[second]");
            let date = time::PrimitiveDateTime::parse(&format!("{} {}", parts[0], parts[1]), format)?;
            let zone = if parts.len() == 3 {
                time_tz::timezones::get_by_name(parts[2]).context("unknown IBKR execution timezone")?
            } else {
                gateway_zone.context("IBKR gateway timezone unavailable")?
            };
            (date, zone)
        };
        let instant = date
            .assume_timezone(zone)
            .take()
            .context("ambiguous or nonexistent IBKR local execution time")?;
        DateTime::from_timestamp(instant.unix_timestamp(), instant.nanosecond())
            .context("invalid IBKR execution timestamp")
    }

    fn decimal(value: f64) -> Result<Decimal> {
        ensure!(value.is_finite(), "IBKR fact contains an unset/nonfinite number");
        Ok(value.to_string().parse()?)
    }

    async fn execution_page(
        &self,
        input: &DurableMessage<TradeIntent>,
        receipt: &OrderReceipt,
        prepared: &PreparedSubmission,
        instrument: &InstrumentSpec,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ExecutionPage> {
        let expected = self.restore(input, prepared)?;
        ensure!(
            receipt.exchange_order_id == prepared.venue_order_id
                && expected.contract.currency.as_str() == instrument.quote_asset,
            "IBKR execution instrument/currency mismatch"
        );
        let client = self.client().await?;
        let filter = ExecutionFilter {
            client_id: Some(self.config.client_id),
            account_code: self.account.clone(),
            symbol: expected.contract.symbol.to_string(),
            last_n_days: 7,
            ..Default::default()
        };
        let mut stream = client.executions(filter).await?;
        let mut executions = BTreeMap::<String, ExecutionData>::new();
        let mut commissions = BTreeMap::<String, CommissionReport>::new();
        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            count += 1;
            ensure!(count <= 20_000, "IBKR execution snapshot exceeds bounded limit");
            match item? {
                SubscriptionItem::Data(Executions::ExecutionData(data)) => {
                    let execution = &data.execution;
                    if execution.order_id != expected.order.order_id
                        || execution.client_id != self.config.client_id
                        || execution.account_number != self.account
                        || execution.order_reference != expected.order.order_ref
                    {
                        continue;
                    }
                    ensure!(
                        data.contract.contract_id == expected.contract.contract_id,
                        "IBKR execution contract mismatch"
                    );
                    ensure!(
                        !execution.pending_price_revision,
                        "IBKR execution price revision is pending"
                    );
                    ensure!(
                        execution.execution_id.rsplit('.').next() == Some("01"),
                        "IBKR execution correction requires an explicit compensating journal fact"
                    );
                    executions.insert(execution.execution_id.clone(), data);
                }
                SubscriptionItem::Data(Executions::CommissionReport(report)) => {
                    commissions.insert(report.execution_id.clone(), report);
                }
                SubscriptionItem::Notice(notice) if notice.is_error() => bail!("IBKR execution snapshot rejected"),
                _ => {}
            }
        }
        let selected = executions
            .iter()
            .filter(|(id, _)| cursor.is_none_or(|cursor| id.as_str() > cursor))
            .collect::<Vec<_>>();
        let has_more = selected.len() > usize::from(limit);
        let mut facts = Vec::new();
        for (id, data) in selected.into_iter().take(usize::from(limit)) {
            let execution = &data.execution;
            let commission = commissions
                .get(id)
                .context("IBKR execution commission not yet available; cursor unchanged")?;
            let side = if execution.side == ExecutionSide::Bought {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            ensure!(side == input.payload.side, "IBKR execution side mismatch");
            let fact = ExecutionFact {
                reservation_node_id: input.node_id,
                reservation_seq: input.seq,
                wallet_id: input.payload.wallet_id,
                account_id: self.config.account_id,
                exchange: input.payload.exchange.clone(),
                exchange_order_id: receipt.exchange_order_id.clone(),
                exchange_trade_id: id.clone(),
                symbol: instrument.symbol.clone(),
                base_asset: instrument.base_asset.clone(),
                quote_asset: instrument.quote_asset.clone(),
                side,
                price: Self::decimal(execution.price)?,
                quantity: Self::decimal(execution.shares)?,
                quote_quantity: None,
                fee: Self::decimal(commission.commission)?,
                fee_asset: commission.currency.clone(),
                order_complete: false,
                executed_at: Self::execution_timestamp(&execution.time, client.time_zone())?,
            };
            fact.validate()?;
            facts.push(fact);
        }
        // Reset after each finite snapshot so a late execution with an older ID is
        // replayed on the next poll. Order's durable venue-fact key deduplicates it.
        let next_cursor = if has_more {
            facts.last().map(|fact| fact.exchange_trade_id.clone())
        } else {
            None
        };
        let status = self.execution_status(&client, input, &expected, receipt).await?;
        Ok(ExecutionPage {
            facts,
            status,
            next_cursor,
            has_more,
        })
    }

    async fn execution_status(
        &self,
        client: &Client,
        input: &DurableMessage<TradeIntent>,
        expected: &PreparedIbkrOrder,
        receipt: &OrderReceipt,
    ) -> Result<Option<OrderStatusFact>> {
        for completed in [false, true] {
            let mut stream = if completed {
                client.completed_orders(true).await?
            } else {
                client.open_orders().await?
            };
            let mut matched = None;
            let mut order_status = None;
            let mut count = 0usize;
            while let Some(item) = stream.next().await {
                count += 1;
                ensure!(count <= 10_000, "IBKR status snapshot exceeds limit");
                match item? {
                    SubscriptionItem::Data(Orders::OrderData(data)) if self.matching_data(&data, expected) => {
                        matched = Some(data)
                    }
                    SubscriptionItem::Data(Orders::OrderStatus(status))
                        if status.order_id == expected.order.order_id && status.client_id == self.config.client_id =>
                    {
                        order_status = Some(status)
                    }
                    SubscriptionItem::Notice(notice) if notice.is_error() => bail!("IBKR status snapshot rejected"),
                    _ => {}
                }
            }
            if let Some(data) = matched {
                let (state, filled) = if let Some(status) = order_status {
                    (
                        Self::state(&status.status, status.filled, Some(status.remaining)),
                        status.filled,
                    )
                } else if completed {
                    (
                        Self::state(&data.order_state.status, data.order.filled_quantity, None),
                        data.order.filled_quantity,
                    )
                } else {
                    continue;
                };
                if state == OrderState::Unknown || (state == OrderState::Filled && filled <= 0.0) {
                    return Ok(None);
                }
                let status = OrderStatusFact {
                    reservation_node_id: input.node_id,
                    reservation_seq: input.seq,
                    wallet_id: input.payload.wallet_id,
                    account_id: self.config.account_id,
                    exchange: input.payload.exchange.clone(),
                    receipt: OrderReceipt {
                        exchange_order_id: receipt.exchange_order_id.clone(),
                        state,
                    },
                    executed_quantity: Self::decimal(filled)?,
                    occurred_at: Utc::now(),
                };
                status.validate()?;
                return Ok(Some(status));
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl IExecutionAdapter for IbkrExecution {
    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<()> {
        instrument.validate()?;
        let contract = self
            .config
            .contracts
            .get(&instrument.symbol)
            .context("IBKR instrument contract missing")?;
        ensure!(
            instrument.exchange.eq_ignore_ascii_case("ibkr")
                && contract.security_type == SecurityType::Stock
                && contract.symbol.as_str() == instrument.base_asset
                && contract.currency.as_str() == instrument.quote_asset,
            "IBKR instrument assets differ from the resolved stock contract"
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
        prepared: Option<&PreparedSubmission>,
        instrument: &InstrumentSpec,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ExecutionPage> {
        self.authorize(input).await?;
        instrument.validate()?;
        ensure!(
            (1..=1000).contains(&limit)
                && cursor.is_none_or(|cursor| cursor.len() <= 256)
                && instrument.exchange == input.payload.exchange
                && instrument.symbol == input.payload.symbol,
            "invalid IBKR execution query"
        );
        let prepared = prepared.context("IBKR execution query requires persisted preparation")?;
        tokio::time::timeout(
            self.timeout,
            self.execution_page(input, receipt, prepared, instrument, cursor, limit),
        )
        .await
        .map_err(|_| anyhow::anyhow!("IBKR execution/commission query timed out; cursor unchanged"))?
        .map_err(|_| anyhow::anyhow!("IBKR execution/commission/status unavailable; cursor unchanged"))
    }

    async fn prepare(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<PreparedSubmission>> {
        self.authorize(message).await?;
        let contract = self
            .config
            .contracts
            .get(&message.payload.symbol)
            .context("IBKR instrument is not configured")?
            .clone();
        let client = self.client().await?;
        let action = if message.payload.side == OrderSide::Buy {
            Action::Buy
        } else {
            Action::Sell
        };
        let quantity = Self::exact_sdk_number(message.payload.quantity)?;
        let mut order = match message.payload.order_type {
            IntentOrderType::Market => order_builder::market_order(action, quantity),
            IntentOrderType::Limit => order_builder::limit_order(
                action,
                quantity,
                Self::exact_sdk_number(message.payload.limit_price.context("IBKR limit price required")?)?,
            ),
        };
        order.account = self.account.clone();
        order.client_id = self.config.client_id;
        order.order_id = client.next_order_id();
        ensure!(order.order_id > 0, "IBKR returned an invalid order ID");
        order.order_ref = SubmissionIdentity::venue_reference(message)?;
        let venue_order_id = format!("ibkr:{}:{}", self.config.client_id, order.order_id);
        let payload = self
            .cipher
            .seal("ibkr", message, &venue_order_id, &PreparedIbkrOrder { contract, order })?;
        Ok(Some(PreparedSubmission {
            venue_order_id,
            payload,
        }))
    }

    async fn submit_prepared(
        &self,
        message: &DurableMessage<TradeIntent>,
        prepared: &PreparedSubmission,
    ) -> Result<OrderReceipt> {
        self.authorize(message).await?;
        let value = self.restore(message, prepared)?;
        let client = self.client().await?;
        let operation = async {
            let mut events = client
                .place_order(value.order.order_id, &value.contract, &value.order)
                .await?;
            while let Some(event) = events.next().await {
                let state = match event? {
                    SubscriptionItem::Data(PlaceOrder::OrderStatus(status))
                        if status.order_id == value.order.order_id && status.client_id == self.config.client_id =>
                    {
                        Self::state(&status.status, status.filled, Some(status.remaining))
                    }
                    SubscriptionItem::Data(PlaceOrder::OpenOrder(data)) if self.matching_data(&data, &value) => {
                        Self::state(&data.order_state.status, data.order.filled_quantity, None)
                    }
                    SubscriptionItem::Notice(notice) if notice.is_order_rejection() => OrderState::Rejected,
                    _ => continue,
                };
                if state != OrderState::Unknown {
                    return Ok::<_, anyhow::Error>(OrderReceipt {
                        exchange_order_id: prepared.venue_order_id.clone(),
                        state,
                    });
                }
            }
            bail!("IBKR submission ended without authoritative acknowledgment")
        };
        match tokio::time::timeout(self.timeout, operation).await {
            Ok(Ok(receipt)) => Ok(receipt),
            _ => {
                *self.client.lock().await = None;
                bail!("IBKR submission result uncertain; reconcile persisted order ID")
            }
        }
    }

    async fn reconcile_prepared(
        &self,
        message: &DurableMessage<TradeIntent>,
        prepared: &PreparedSubmission,
    ) -> Result<Option<OrderReceipt>> {
        self.authorize(message).await?;
        let expected = self.restore(message, prepared)?;
        let client = self.client().await?;
        let operation = async {
            // The two finite SDK snapshots cover active and completed orders. Missing
            // history remains Unknown; it never authorizes another place_order call.
            for completed in [false, true] {
                let mut stream = if completed {
                    client.completed_orders(true).await?
                } else {
                    client.open_orders().await?
                };
                let mut count = 0usize;
                while let Some(item) = stream.next().await {
                    count += 1;
                    ensure!(count <= 10_000, "IBKR reconciliation exceeds bounded result limit");
                    if let SubscriptionItem::Data(Orders::OrderData(data)) = item? {
                        if self.matching_data(&data, &expected) {
                            return Ok::<_, anyhow::Error>(Some(OrderReceipt {
                                exchange_order_id: prepared.venue_order_id.clone(),
                                state: Self::state(&data.order_state.status, data.order.filled_quantity, None),
                            }));
                        }
                    }
                }
            }
            Ok(None)
        };
        match tokio::time::timeout(self.timeout, operation).await {
            Ok(Ok(receipt)) => Ok(receipt),
            _ => {
                *self.client.lock().await = None;
                bail!("IBKR reconciliation unavailable; preserve unknown order")
            }
        }
    }

    async fn submit(&self, _: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        bail!("IBKR requires a persisted prepared submission")
    }
    async fn reconcile(&self, _: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        bail!("IBKR reconciliation requires persisted venue identity")
    }
}
