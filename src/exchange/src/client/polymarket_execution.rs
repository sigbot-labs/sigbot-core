// SPDX-License-Identifier: GPL-3.0-only
//! Official Polymarket V2/V3 SDK; a signed order is sealed before its single POST.
use super::{
    execution_adapter::{ExecutionMode, ExecutionPage, IExecutionAdapter, PreparedSubmission},
    polymarket_settlement::{PolymarketSettlement, SettlementConfig, SettlementOrder},
    prepared_payload::{PreparedKeyConfig, PreparedPayloadCipher},
};
use alloy::{
    signers::local::PrivateKeySigner,
    sol_types::{Eip712Domain, SolStruct, SolValue},
};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Credentials, Normal, Signer, Uuid},
    clob::{
        types::{
            Amount, OrderPayload, OrderSignature, OrderStatusType, OrderType, OrderV2, Side, SignatureType, SignedOrder,
        },
        Client, Config,
    },
    contract_config,
    types::{Address, Signature, B256, U256},
    POLYGON,
};
use serde::{Deserialize, Serialize};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{envelope::DurableMessage, topic::MessageScope},
    order::{
        execution::{OrderReceipt, OrderState, OrderStatusFact},
        intent::{IntentOrderType, OrderSide, TradeIntent},
    },
};
use std::{borrow::Cow, str::FromStr, time::Duration};
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolymarketExecutionConfig {
    pub scope: MessageScope,
    pub account_id: i64,
    pub signer_address: Address,
    pub private_key_env: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub api_passphrase_env: String,
    #[serde(default)]
    pub signature_type: SignatureType,
    pub funder: Option<Address>,
    pub min_request_interval_ms: u64,
    pub prepared_keys: PreparedKeyConfig,
    pub settlement: SettlementConfig,
}

pub struct PolymarketExecution {
    config: PolymarketExecutionConfig,
    cipher: PreparedPayloadCipher,
    client: OnceCell<Client<Authenticated<Normal>>>,
    next_request: Mutex<Instant>,
    timeout: Duration,
    settlement: PolymarketSettlement,
}

/// SDK Solidity order bytes round-trip via its ABI codec. No locally redefined
/// signing fields, salts, rounding rules, or authentication implementation.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedPolymarketOrder {
    version: u8,
    sdk_order_abi: String,
    expiration: U256,
    signature: String,
    order_type: OrderType,
    owner: Uuid,
    post_only: Option<bool>,
    defer_exec: Option<bool>,
    verifying_contract: Address,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    settlement_from_block: Option<u64>,
}

impl PreparedPolymarketOrder {
    pub fn from_signed(signed: SignedOrder, verifying_contract: Address) -> Result<Self> {
        let (version, order) = match &signed.payload {
            OrderPayload::V2(value) => (2u8, value),
            OrderPayload::V3(value) => (3u8, value),
            _ => bail!("legacy Polymarket order version is not accepted"),
        };
        Ok(Self {
            version,
            sdk_order_abi: hex::encode(order.order.abi_encode()),
            expiration: order.expiration,
            signature: signed.signature.to_string(),
            order_type: signed.order_type,
            owner: signed.owner,
            post_only: signed.post_only,
            defer_exec: signed.defer_exec,
            verifying_contract,
            settlement_from_block: None,
        })
    }

    pub fn venue_order_id(&self) -> Result<String> {
        let bytes = hex::decode(&self.sdk_order_abi).context("invalid Polymarket SDK order encoding")?;
        let order = OrderV2::abi_decode(&bytes).context("invalid Polymarket SDK order")?;
        Ok(format!("{:#x}", order.eip712_signing_hash(&self.domain())))
    }

    fn domain(&self) -> Eip712Domain {
        Eip712Domain {
            name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
            version: Some(Cow::Owned(self.version.to_string())),
            chain_id: Some(U256::from(POLYGON)),
            verifying_contract: Some(self.verifying_contract),
            ..Default::default()
        }
    }

    pub fn restore(&self, venue_id: &str) -> Result<SignedOrder> {
        ensure!(matches!(self.version, 2 | 3), "unsupported Polymarket order version");
        let bytes = hex::decode(&self.sdk_order_abi).context("invalid Polymarket SDK order encoding")?;
        let order = OrderV2::abi_decode(&bytes).context("invalid Polymarket SDK order")?;
        ensure!(
            format!("{:#x}", order.eip712_signing_hash(&self.domain())) == venue_id,
            "Polymarket prepared order hash mismatch"
        );
        let signature = if order.signatureType == SignatureType::Poly1271 as u8 {
            OrderSignature::Wrapped(self.signature.clone())
        } else {
            OrderSignature::Ecdsa(
                Signature::from_str(&self.signature).context("invalid Polymarket signature encoding")?,
            )
        };
        let payload = if self.version == 3 {
            OrderPayload::new_v3(order, self.expiration)
        } else {
            OrderPayload::new(order, self.expiration)
        };
        Ok(SignedOrder::builder()
            .payload(payload)
            .signature(signature)
            .order_type(self.order_type.clone())
            .owner(self.owner)
            .maybe_post_only(self.post_only)
            .maybe_defer_exec(self.defer_exec)
            .build())
    }

    fn settlement_order(&self, venue_id: &str) -> Result<SettlementOrder> {
        self.restore(venue_id)?;
        let bytes = hex::decode(&self.sdk_order_abi).context("invalid Polymarket SDK order encoding")?;
        let order = OrderV2::abi_decode(&bytes).context("invalid Polymarket SDK order")?;
        Ok(SettlementOrder {
            exchange: self.verifying_contract,
            order_hash: B256::from_str(venue_id).context("invalid prepared order hash")?,
            maker: order.maker,
            token_id: order.tokenId,
            side: match order.side {
                0 => OrderSide::Buy,
                1 => OrderSide::Sell,
                _ => bail!("invalid prepared order side"),
            },
            first_block: self
                .settlement_from_block
                .context("legacy prepared order lacks a settlement scan boundary; reconcile manually")?,
        })
    }
}

impl PolymarketExecution {
    pub(crate) fn new(config: &PolymarketExecutionConfig, timeout_seconds: u64) -> Result<Self> {
        config.scope.validate()?;
        ensure!(
            config.account_id > 0 && config.signer_address != Address::ZERO,
            "invalid Polymarket scoped account"
        );
        ensure!(
            (50..=60_000).contains(&config.min_request_interval_ms),
            "invalid Polymarket request interval"
        );
        Ok(Self {
            config: config.clone(),
            cipher: PreparedPayloadCipher::new(&config.prepared_keys)?,
            settlement: PolymarketSettlement::new(&config.settlement)?,
            client: OnceCell::new(),
            next_request: Mutex::new(Instant::now()),
            timeout: Duration::from_secs(timeout_seconds),
        })
    }

    fn secret(name: &str) -> Result<String> {
        std::env::var(name).map_err(|_| anyhow::anyhow!("Polymarket credential environment variable missing"))
    }

    fn signer(&self) -> Result<PrivateKeySigner> {
        let signer = PrivateKeySigner::from_str(&Self::secret(&self.config.private_key_env)?)
            .map_err(|_| anyhow::anyhow!("invalid Polymarket signing key"))?
            .with_chain_id(Some(POLYGON));
        ensure!(
            signer.address() == self.config.signer_address,
            "Polymarket signing account mismatch"
        );
        Ok(signer)
    }

    async fn client(&self) -> Result<&Client<Authenticated<Normal>>> {
        self.client
            .get_or_try_init(|| async {
                let signer = self.signer()?;
                let credentials = Credentials::new(
                    Uuid::parse_str(&Self::secret(&self.config.api_key_env)?)
                        .map_err(|_| anyhow::anyhow!("invalid Polymarket API key"))?,
                    Self::secret(&self.config.api_secret_env)?,
                    Self::secret(&self.config.api_passphrase_env)?,
                );
                // Existing L2 credentials avoid creating API keys during order preparation.
                // The official SDK executes one order POST; its follow-up polling is GET-only.
                let client = Client::new("https://clob-v2.polymarket.com", Config::default())
                    .map_err(|_| anyhow::anyhow!("Polymarket SDK initialization failed"))?;
                let mut builder = client
                    .authentication_builder(&signer)
                    .credentials(credentials)
                    .signature_type(self.config.signature_type);
                if let Some(funder) = self.config.funder {
                    builder = builder.funder(funder);
                }
                builder
                    .authenticate()
                    .await
                    .map_err(|_| anyhow::anyhow!("Polymarket SDK authentication failed"))
            })
            .await
    }

    async fn authorize(&self, message: &DurableMessage<TradeIntent>) -> Result<()> {
        message.validate()?;
        ensure!(
            message.scope == self.config.scope
                && message.account_id == Some(self.config.account_id)
                && message.payload.account_id == self.config.account_id
                && message.payload.exchange.eq_ignore_ascii_case("polymarket"),
            "Polymarket client scope/account/provider mismatch"
        );
        let mut next = self.next_request.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + Duration::from_millis(self.config.min_request_interval_ms);
        Ok(())
    }

    async fn prepare_order(&self, message: &DurableMessage<TradeIntent>) -> Result<PreparedSubmission> {
        let client = self.client().await?;
        let signer = self.signer()?;
        let token_id = U256::from_str(&message.payload.symbol)
            .context("Polymarket symbol must be an official token/position ID")?;
        let side = if message.payload.side == OrderSide::Buy {
            Side::Buy
        } else {
            Side::Sell
        };
        let signable = match message.payload.order_type {
            IntentOrderType::Limit => {
                client
                    .limit_order()
                    .token_id(token_id)
                    .side(side)
                    .price(message.payload.limit_price.context("Polymarket limit price required")?)
                    .size(message.payload.quantity)
                    .order_type(OrderType::GTC)
                    .build()
                    .await
            }
            // The risk-checked market evidence supplies a hard price ceiling/floor.
            // FOK preserves the requested share quantity and bounds reserved cash.
            IntentOrderType::Market => {
                client
                    .market_order()
                    .token_id(token_id)
                    .side(side)
                    .amount(Amount::shares(message.payload.quantity)?)
                    .price(message.payload.market.price)
                    .order_type(OrderType::FOK)
                    .build()
                    .await
            }
        }
        .map_err(|_| anyhow::anyhow!("Polymarket SDK order validation failed"))?;
        let signed = client
            .sign(&signer, signable)
            .await
            .map_err(|_| anyhow::anyhow!("Polymarket SDK signing failed"))?;
        let version = match &signed.payload {
            OrderPayload::V2(_) => 2u8,
            OrderPayload::V3(_) => 3u8,
            _ => bail!("legacy Polymarket order version is not accepted"),
        };
        let neg_risk = if version == 3 {
            false
        } else {
            client
                .neg_risk(token_id)
                .await
                .map_err(|_| anyhow::anyhow!("Polymarket risk metadata unavailable"))?
                .neg_risk
        };
        let contracts =
            contract_config(POLYGON, neg_risk).context("Polymarket official contract configuration missing")?;
        let verifying_contract = if version == 3 {
            contracts.exchange_v3
        } else {
            contracts.exchange_v2
        }
        .context("Polymarket official exchange contract missing")?;
        let mut value = PreparedPolymarketOrder::from_signed(signed, verifying_contract)?;
        value.settlement_from_block = Some(self.settlement.preflight(verifying_contract).await.map_err(|_| {
            anyhow::anyhow!("Polymarket finalized settlement observations unavailable; no order submitted")
        })?);
        let venue_order_id = value.venue_order_id()?;
        // Exercise the SDK ABI decoder before any financial record can be committed.
        value.restore(&venue_order_id)?;
        let payload = self.cipher.seal("polymarket", message, &venue_order_id, &value)?;
        Ok(PreparedSubmission {
            venue_order_id,
            payload,
        })
    }

    pub fn state(status: &OrderStatusType, partially_filled: bool) -> OrderState {
        match status {
            OrderStatusType::Canceled => OrderState::Cancelled,
            OrderStatusType::Live if partially_filled => OrderState::PartiallyFilled,
            OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Matched => OrderState::Acknowledged,
            // CLOB matching is not on-chain settlement. Confirmed execution facts
            // must independently reach Wallet before balances/positions change.
            _ => OrderState::Unknown,
        }
    }
}

#[async_trait]
impl IExecutionAdapter for PolymarketExecution {
    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<()> {
        instrument.validate()?;
        let token =
            U256::from_str(&instrument.symbol).context("Polymarket instrument requires an official token ID")?;
        ensure!(
            instrument.exchange.eq_ignore_ascii_case("polymarket")
                && token > U256::ZERO
                && instrument.base_asset == format!("outcome:{token}")
                && instrument.quote_asset == "pUSD",
            "Polymarket instrument must identify its outcome token and pUSD collateral"
        );
        Ok(())
    }
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Live
    }
    async fn prepare(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<PreparedSubmission>> {
        self.authorize(message).await?;
        tokio::time::timeout(self.timeout, self.prepare_order(message))
            .await
            .map_err(|_| anyhow::anyhow!("Polymarket preparation timed out; no order was submitted"))?
            .map(Some)
    }

    async fn submit_prepared(
        &self,
        message: &DurableMessage<TradeIntent>,
        prepared: &PreparedSubmission,
    ) -> Result<OrderReceipt> {
        self.authorize(message).await?;
        let value: PreparedPolymarketOrder =
            self.cipher
                .open("polymarket", message, &prepared.venue_order_id, &prepared.payload)?;
        let signed = value.restore(&prepared.venue_order_id)?;
        let settlement_order = value.settlement_order(&prepared.venue_order_id)?;
        tokio::time::timeout(self.timeout, self.settlement.preflight(settlement_order.exchange))
            .await
            .map_err(|_| anyhow::anyhow!("Polymarket settlement preflight timed out; no order submitted"))?
            .map_err(|_| anyhow::anyhow!("Polymarket settlement observations unavailable; no order submitted"))?;
        let client = self.client().await?;
        ensure!(
            signed.owner == client.credentials().key(),
            "Polymarket prepared credential owner mismatch"
        );
        let reply = tokio::time::timeout(self.timeout, client.post_order(signed))
            .await
            .map_err(|_| anyhow::anyhow!("Polymarket submission timed out; reconcile persisted order hash"))?
            .map_err(|_| anyhow::anyhow!("Polymarket submission result uncertain; reconcile persisted order hash"))?;
        if !reply.success {
            return Ok(OrderReceipt {
                exchange_order_id: prepared.venue_order_id.clone(),
                state: OrderState::Rejected,
            });
        }
        ensure!(
            reply.order_id.eq_ignore_ascii_case(&prepared.venue_order_id),
            "Polymarket accepted order hash mismatch; reconcile"
        );
        Ok(OrderReceipt {
            exchange_order_id: prepared.venue_order_id.clone(),
            state: Self::state(&reply.status, false),
        })
    }

    async fn reconcile_prepared(
        &self,
        message: &DurableMessage<TradeIntent>,
        prepared: &PreparedSubmission,
    ) -> Result<Option<OrderReceipt>> {
        self.authorize(message).await?;
        let value: PreparedPolymarketOrder =
            self.cipher
                .open("polymarket", message, &prepared.venue_order_id, &prepared.payload)?;
        let signed = value.restore(&prepared.venue_order_id)?;
        let client = self.client().await?;
        ensure!(
            signed.owner == client.credentials().key(),
            "Polymarket prepared credential owner mismatch"
        );
        let reply = tokio::time::timeout(self.timeout, client.order(&prepared.venue_order_id))
            .await
            .map_err(|_| anyhow::anyhow!("Polymarket reconciliation timed out"))?
            .map_err(|_| anyhow::anyhow!("Polymarket reconciliation unavailable; preserve unknown order"))?;
        ensure!(
            reply.id.eq_ignore_ascii_case(&prepared.venue_order_id) && reply.owner == signed.owner,
            "Polymarket reconciliation identity mismatch"
        );
        Ok(Some(OrderReceipt {
            exchange_order_id: prepared.venue_order_id.clone(),
            state: Self::state(&reply.status, reply.size_matched > rust_decimal::Decimal::ZERO),
        }))
    }

    async fn submit(&self, _: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        bail!("Polymarket requires a persisted prepared submission")
    }
    async fn reconcile(&self, _: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        bail!("Polymarket reconciliation requires persisted order hash")
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
        let prepared = prepared.context("Polymarket settlement requires persisted order hash")?;
        ensure!(
            receipt.exchange_order_id == prepared.venue_order_id,
            "Polymarket receipt and persisted order differ"
        );
        let value: PreparedPolymarketOrder =
            self.cipher
                .open("polymarket", input, &prepared.venue_order_id, &prepared.payload)?;
        let signed = value.restore(&prepared.venue_order_id)?;
        let order = value.settlement_order(&prepared.venue_order_id)?;
        tokio::time::timeout(self.timeout, async {
            let client = self.client().await?;
            ensure!(
                signed.owner == client.credentials().key(),
                "Polymarket prepared credential owner mismatch"
            );
            let mut page = self.settlement.read(input, &order, instrument, cursor, limit).await?;
            let observed = client.order(&prepared.venue_order_id).await?;
            ensure!(
                observed.id.eq_ignore_ascii_case(&prepared.venue_order_id)
                    && observed.owner == signed.owner
                    && observed.maker_address == order.maker
                    && observed.asset_id == order.token_id
                    && observed.original_size == input.payload.quantity,
                "Polymarket observed order identity mismatch"
            );
            ensure!(
                observed.size_matched >= rust_decimal::Decimal::ZERO && observed.size_matched <= observed.original_size,
                "Polymarket cumulative matched amount invalid"
            );
            let state =
                if observed.status == OrderStatusType::Matched && observed.size_matched == observed.original_size {
                    OrderState::Filled
                } else {
                    Self::state(&observed.status, observed.size_matched > rust_decimal::Decimal::ZERO)
                };
            if state != OrderState::Unknown {
                let status = OrderStatusFact {
                    reservation_node_id: input.node_id,
                    reservation_seq: input.seq,
                    wallet_id: input.payload.wallet_id,
                    account_id: input.payload.account_id,
                    exchange: input.payload.exchange.clone(),
                    receipt: OrderReceipt {
                        exchange_order_id: prepared.venue_order_id.clone(),
                        state,
                    },
                    executed_quantity: observed.size_matched,
                    occurred_at: chrono::Utc::now(),
                };
                status.validate()?;
                page.status = Some(status);
            }
            Ok::<_, anyhow::Error>(page)
        })
        .await
        .map_err(|_| anyhow::anyhow!("Polymarket settlement query timed out; cursor unchanged"))?
        .map_err(|_| anyhow::anyhow!("Polymarket canonical settlement or status unavailable; cursor unchanged"))
    }
}
