// SPDX-License-Identifier: GPL-3.0-only

use crate::exchange::ExecutionPage;
use alloy::primitives::Address;
use alloy::primitives::B256;
use alloy::primitives::U256;
use alloy::providers::DynProvider;
use alloy::providers::Provider;
use alloy::providers::ProviderBuilder;
use alloy::rpc::types::BlockNumberOrTag;
use alloy::rpc::types::Filter;
use alloy::rpc::types::Log;
use alloy::rpc::types::TransactionReceipt;
use alloy::rpc::types::TransactionRequest;
use alloy::sol;
use alloy::sol_types::SolEvent;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use polymarket_client_sdk_v2::clob::utilities::USDC_DECIMALS;
use polymarket_client_sdk_v2::contract_config;
use polymarket_client_sdk_v2::POLYGON;
use rust_decimal::Decimal;
use serde::Deserialize;
use sigbot_core::config::config::get_config;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::str::FromStr;

// Official V2 ABI: Polymarket/ctf-exchange-v2 @ ccc0596074f4dfd62c944fbca4de252893b82b4b,
// src/exchange/interfaces/ITrading.sol. V3 uses the identical event signature:
// Polymarket/polymarket-v2-external @ 741f8bbe88c3f29a1c55d1248a3dd411f622f111.
// Side's ABI type is uint8. Alloy generates the decoder; fee is collateral units.
sol! {
    event OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker,
        uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled,
        uint256 fee, bytes32 builder, bytes32 metadata);
}

#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementConfig {
    pub rpc_url_secret: String,
    /// Also require the RPC's finalized block; depth alone never permits a receipt.
    pub min_confirmations: u64,
}

pub struct SettlementOrder {
    pub exchange: Address,
    pub order_hash: B256,
    pub maker: Address,
    pub token_id: U256,
    pub side: OrderSide,
    pub first_block: u64,
}

pub struct SettlementProof<'a> {
    pub log: &'a Log,
    pub receipt: &'a TransactionReceipt,
    pub canonical_hash: B256,
    pub finalized_height: u64,
    pub executed_at: DateTime<Utc>,
}

pub struct PolymarketSettlement {
    provider: DynProvider,
    min_confirmations: u64,
}

impl PolymarketSettlement {
    pub fn new(config: &SettlementConfig) -> Result<Self> {
        ensure!(
            (16..=8192).contains(&config.min_confirmations),
            "invalid settlement confirmation depth"
        );
        ensure!(
            !config.rpc_url_secret.is_empty()
                && config
                    .rpc_url_secret
                    .bytes()
                    .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == b'_'),
            "settlement RPC must use a central secret key"
        );
        let app_config = get_config();
        let raw = app_config.secrets.require(&config.rpc_url_secret)?;
        let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid settlement RPC URL"))?;
        ensure!(
            url.scheme() == "https"
                || (url.scheme() == "http" && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))),
            "settlement RPC requires HTTPS"
        );
        Ok(Self {
            provider: ProviderBuilder::new().connect_http(url).erased(),
            min_confirmations: config.min_confirmations,
        })
    }

    pub fn validate_exchange(exchange: Address) -> Result<()> {
        ensure!(
            [false, true].iter().any(|negative| contract_config(POLYGON, *negative)
                .is_some_and(|config| config.exchange_v2 == Some(exchange) || config.exchange_v3 == Some(exchange))),
            "settlement exchange is not an official Polygon V2/V3 contract"
        );
        Ok(())
    }

    /// Before any order POST, verify observation capability and persist this height.
    pub async fn preflight(&self, exchange: Address) -> Result<u64> {
        Self::validate_exchange(exchange)?;
        ensure!(
            self.provider.get_chain_id().await? == POLYGON,
            "settlement RPC chain mismatch"
        );
        ensure!(
            !self.provider.get_code_at(exchange).await?.is_empty(),
            "official exchange contract unavailable"
        );
        let collateral = contract_config(POLYGON, false)
            .context("official collateral missing")?
            .collateral;
        // ERC-20 decimals() selector; result decoded as an ABI integer, never guessed.
        let call = TransactionRequest::default()
            .to(collateral)
            .input(vec![0x31, 0x3c, 0xe5, 0x67].into());
        let decimals = self.provider.call(call).await?;
        ensure!(
            decimals.len() == 32 && U256::from_be_slice(&decimals) == U256::from(USDC_DECIMALS),
            "collateral precision differs from the official SDK"
        );
        self.finalized_height().await?;
        Ok(self.provider.get_block_number().await?)
    }

    async fn finalized_height(&self) -> Result<u64> {
        ensure!(
            self.provider.get_chain_id().await? == POLYGON,
            "settlement RPC chain mismatch"
        );
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await?
            .context("RPC does not expose finalized blocks")?;
        let head = self.provider.get_block_number().await?;
        let depth_height = head
            .checked_sub(self.min_confirmations - 1)
            .context("chain lacks required confirmation depth")?;
        Ok(block.header.number.min(depth_height))
    }

    pub async fn read(
        &self,
        input: &DurableMessage<TradeIntent>,
        order: &SettlementOrder,
        instrument: &InstrumentSpec,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ExecutionPage> {
        Self::validate_exchange(order.exchange)?;
        instrument.validate()?;
        ensure!((1..=1000).contains(&limit), "invalid settlement page size");
        // Each fill can require a receipt and canonical block lookup. Return fewer
        // than requested so a large order progresses within the host RPC timeout.
        let limit = limit.min(32);
        ensure!(
            instrument.exchange.eq_ignore_ascii_case("polymarket")
                && instrument.symbol == input.payload.symbol
                && instrument.symbol == order.token_id.to_string()
                && instrument.base_asset == format!("outcome:{}", order.token_id)
                && instrument.quote_asset == "pUSD",
            "Polymarket instrument must identify its outcome token and pUSD collateral"
        );
        let (start, after_log) = Self::cursor(cursor, order.first_block)?;
        let finalized = self.finalized_height().await?;
        if start > finalized {
            return Ok(ExecutionPage {
                facts: vec![],
                status: None,
                next_cursor: cursor.map(str::to_owned),
                has_more: false,
            });
        }
        let end = start.saturating_add(511).min(finalized);
        let filter = Filter::new()
            .address(order.exchange)
            .event_signature(OrderFilled::SIGNATURE_HASH)
            .topic1(order.order_hash)
            .from_block(start)
            .to_block(end);
        let mut logs = self.provider.get_logs(&filter).await?;
        ensure!(logs.len() <= 20_000, "settlement RPC exceeded bounded log result");
        logs.sort_by_key(|log| (log.block_number, log.log_index));
        let mut receipts = HashMap::<B256, TransactionReceipt>::new();
        let mut blocks = BTreeMap::<u64, (B256, DateTime<Utc>)>::new();
        let mut facts = Vec::with_capacity(usize::from(limit));
        let mut next_cursor = format!("{}:0", end.checked_add(1).context("settlement cursor overflow")?);
        let mut has_more = end < finalized;
        for log in logs {
            let height = log.block_number.context("settlement log block missing")?;
            let index = log.log_index.context("settlement log index missing")?;
            ensure!(
                height >= start && height <= end,
                "settlement RPC returned a log outside the requested range"
            );
            if height == start && index < after_log {
                continue;
            }
            if facts.len() == usize::from(limit) {
                next_cursor = format!("{height}:{index}");
                has_more = true;
                break;
            }
            let tx_hash = log.transaction_hash.context("settlement transaction hash missing")?;
            if let std::collections::hash_map::Entry::Vacant(e) = receipts.entry(tx_hash) {
                let receipt = self
                    .provider
                    .get_transaction_receipt(tx_hash)
                    .await?
                    .context("settlement receipt unavailable")?;
                e.insert(receipt);
            }
            if let std::collections::btree_map::Entry::Vacant(e) = blocks.entry(height) {
                let block = self
                    .provider
                    .get_block_by_number(height.into())
                    .await?
                    .context("settlement block unavailable")?;
                let time = DateTime::from_timestamp(i64::try_from(block.header.timestamp)?, 0)
                    .context("invalid settlement block time")?;
                e.insert((block.header.hash, time));
            }
            let (hash, time) = blocks[&height];
            facts.push(Self::decode(
                input,
                order,
                instrument,
                SettlementProof {
                    log: &log,
                    receipt: &receipts[&tx_hash],
                    canonical_hash: hash,
                    finalized_height: finalized,
                    executed_at: time,
                },
            )?);
        }
        // A second canonical lookup prevents committing a range changed while reading.
        for (height, (hash, _)) in blocks {
            let current = self
                .provider
                .get_block_by_number(height.into())
                .await?
                .context("settlement canonical block unavailable")?;
            ensure!(current.header.hash == hash, "settlement block changed while reading");
        }
        Ok(ExecutionPage {
            facts,
            status: None,
            next_cursor: Some(next_cursor),
            has_more,
        })
    }

    fn cursor(value: Option<&str>, first_block: u64) -> Result<(u64, u64)> {
        let Some(value) = value else {
            return Ok((first_block, 0));
        };
        ensure!(value.len() <= 42, "invalid settlement cursor");
        let (height, index) = value.split_once(':').context("invalid settlement cursor")?;
        let height = height.parse::<u64>()?;
        ensure!(height >= first_block, "settlement cursor predates prepared order");
        Ok((height, index.parse()?))
    }

    /// Pure decoder also used by explicit external-receipt fixtures; it never submits.
    pub fn decode(
        input: &DurableMessage<TradeIntent>,
        order: &SettlementOrder,
        instrument: &InstrumentSpec,
        proof: SettlementProof<'_>,
    ) -> Result<ExecutionFact> {
        let SettlementProof {
            log,
            receipt,
            canonical_hash,
            finalized_height,
            executed_at,
        } = proof;
        Self::validate_exchange(order.exchange)?;
        ensure!(
            !log.removed && log.address() == order.exchange && receipt.status(),
            "settlement reverted, removed, or from another exchange"
        );
        let height = log.block_number.context("settlement block number missing")?;
        let tx_hash = log.transaction_hash.context("settlement transaction hash missing")?;
        let index = log.log_index.context("settlement log index missing")?;
        ensure!(
            height >= order.first_block
                && height <= finalized_height
                && log.block_hash == Some(canonical_hash)
                && receipt.block_hash == Some(canonical_hash)
                && receipt.block_number == Some(height)
                && receipt.transaction_hash == tx_hash,
            "settlement receipt is not canonical and finalized"
        );
        ensure!(
            receipt.logs().iter().any(|candidate| candidate.inner == log.inner
                && candidate.log_index == Some(index)
                && candidate.transaction_hash == Some(tx_hash)
                && candidate.block_hash == Some(canonical_hash)),
            "settlement log is absent from its receipt"
        );
        let event = log
            .log_decode::<OrderFilled>()
            .context("invalid official OrderFilled event")?
            .inner
            .data;
        let side = match event.side {
            0 => OrderSide::Buy,
            1 => OrderSide::Sell,
            _ => anyhow::bail!("invalid settlement side"),
        };
        ensure!(
            event.orderHash == order.order_hash
                && event.maker == order.maker
                && event.tokenId == order.token_id
                && side == order.side
                && side == input.payload.side,
            "settlement order identity mismatch"
        );
        let (shares, cash) = if side == OrderSide::Buy {
            (event.takerAmountFilled, event.makerAmountFilled)
        } else {
            (event.makerAmountFilled, event.takerAmountFilled)
        };
        let quantity = Self::amount(shares)?;
        let quote_quantity = Self::amount(cash)?;
        ensure!(
            quantity > Decimal::ZERO && quote_quantity > Decimal::ZERO,
            "nonpositive settled fill"
        );
        let fact = ExecutionFact {
            reservation_node_id: input.node_id,
            reservation_seq: input.seq,
            wallet_id: input.payload.wallet_id,
            account_id: input.payload.account_id,
            exchange: input.payload.exchange.clone(),
            exchange_order_id: format!("{:#x}", order.order_hash),
            exchange_trade_id: format!("{tx_hash:#x}:{index}"),
            symbol: instrument.symbol.clone(),
            base_asset: instrument.base_asset.clone(),
            quote_asset: instrument.quote_asset.clone(),
            side,
            price: quote_quantity
                .checked_div(quantity)
                .context("settlement price overflow")?,
            quantity,
            quote_quantity: Some(quote_quantity),
            fee: Self::amount(event.fee)?,
            fee_asset: instrument.quote_asset.clone(),
            order_complete: false,
            executed_at,
        };
        fact.validate()?;
        Ok(fact)
    }

    fn amount(value: U256) -> Result<Decimal> {
        Decimal::from_str(&value.to_string())
            .context("settlement amount exceeds Decimal capacity")?
            .checked_div(Decimal::from(10u64.pow(USDC_DECIMALS)))
            .context("settlement scale overflow")
    }
}
