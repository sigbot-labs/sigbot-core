// SPDX-License-Identifier: GPL-3.0-only

use anyhow::ensure;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use common_context::ApplicationContext;
use common_context::IProvider;
use openssl::rand::rand_bytes;
use openssl::symm::decrypt_aead;
use openssl::symm::encrypt_aead;
use openssl::symm::Cipher;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use sigbot_core::config::config::get_config;
use sigbot_types::exchange::exchange::ExchangeProvider;
use sigbot_types::exchange::instrument::InstrumentSpec;
pub use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::exchange::models::trade_market::KlineModel;
use sigbot_types::exchange::models::trade_market::OrderInfo;
use sigbot_types::exchange::models::trade_market::PriceModel;
use sigbot_types::exchange::models::trade_market::SymbolInfo;
use sigbot_types::exchange::models::trade_position::ExitTradePosition;
use sigbot_types::exchange::models::trade_position::PlaceTradeSignal;
use sigbot_types::exchange::models::trade_position::TradeResult;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::order::intent::TradeIntent;
use std::collections::BTreeMap;
use std::sync::Arc;

#[async_trait]
pub trait IExchange: IProvider {
    fn provider(&self) -> ExchangeProvider;

    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error>;
    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        position: &ExitTradePosition,
    ) -> Result<TradeResult, Error>;
    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        position: &ExitTradePosition,
    ) -> Result<TradeResult, Error>;
    /// Search for symbols/stocks by query string
    /// Returns a list of matching symbols with their contract IDs and metadata
    async fn search_symbols(&self, query: &str, sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error>;
    /// Get current order list from the exchange
    /// Used for syncing historical orders to wallet manager's orders table
    async fn get_orders(&self, account_id: Option<&str>, filters: Option<&str>) -> Result<Vec<OrderInfo>, Error>;
}

/// Retained only while the Python strategy package is frozen.
pub struct ExchangeFactory;
impl ExchangeFactory {
    pub async fn get_implementation(name: impl AsRef<str>) -> anyhow::Result<Arc<dyn IExchange>> {
        ApplicationContext::global().get::<dyn IExchange>(name.as_ref()).await
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedKeyConfig {
    pub active_key: String,
    /// Key version -> central secret key containing a 32-byte hex key.
    pub keys: BTreeMap<String, String>,
}

pub struct PreparedPayloadCipher {
    active_key: String,
    keys: BTreeMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedPayload {
    version: u8,
    key_id: String,
    nonce: String,
    ciphertext: String,
    tag: String,
}

impl PreparedPayloadCipher {
    pub fn new(config: &PreparedKeyConfig) -> Result<Self> {
        ensure!(
            !config.keys.is_empty() && config.keys.len() <= 8,
            "invalid submission keyring size"
        );
        ensure!(
            config.keys.contains_key(&config.active_key),
            "active submission key is missing"
        );
        let mut keys = BTreeMap::new();
        let app_config = get_config();
        for (id, secret_key) in &config.keys {
            ensure!(!id.is_empty() && id.len() <= 64, "invalid submission key version");
            ensure!(
                !secret_key.is_empty()
                    && secret_key.len() <= 128
                    && secret_key
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
                "submission key references must be central secret keys"
            );
            let raw = app_config.secrets.require(secret_key)?;
            let key = hex::decode(raw.trim()).map_err(|_| anyhow::anyhow!("invalid submission encryption key"))?;
            ensure!(key.len() == 32, "submission key must contain 32 bytes");
            keys.insert(id.clone(), key);
        }
        Ok(Self {
            active_key: config.active_key.clone(),
            keys,
        })
    }

    pub fn seal<T: Serialize>(
        &self,
        provider: &str,
        message: &DurableMessage<TradeIntent>,
        venue_id: &str,
        value: &T,
    ) -> Result<serde_json::Value> {
        let mut nonce = [0u8; 12];
        rand_bytes(&mut nonce)?;
        let mut tag = [0u8; 16];
        let aad = Self::aad(provider, message, venue_id, &self.active_key)?;
        let mut plaintext = serde_json::to_vec(value)?;
        ensure!(plaintext.len() <= 128 * 1024, "prepared SDK payload exceeds limit");
        let result = encrypt_aead(
            Cipher::aes_256_gcm(),
            &self.keys[&self.active_key],
            Some(&nonce),
            &aad,
            &plaintext,
            &mut tag,
        );
        plaintext.fill(0);
        let ciphertext = result.map_err(|_| anyhow::anyhow!("submission payload encryption failed"))?;
        Ok(serde_json::to_value(SealedPayload {
            version: 1,
            key_id: self.active_key.clone(),
            nonce: hex::encode(nonce),
            ciphertext: hex::encode(ciphertext),
            tag: hex::encode(tag),
        })?)
    }

    pub fn open<T: DeserializeOwned>(
        &self,
        provider: &str,
        message: &DurableMessage<TradeIntent>,
        venue_id: &str,
        value: &serde_json::Value,
    ) -> Result<T> {
        let sealed: SealedPayload =
            serde_json::from_value(value.clone()).context("invalid sealed submission format")?;
        ensure!(
            sealed.version == 1 && sealed.ciphertext.len() <= 256 * 1024,
            "unsupported sealed submission format"
        );
        let key = self
            .keys
            .get(&sealed.key_id)
            .context("submission encryption key version unavailable")?;
        let nonce = hex::decode(&sealed.nonce).context("invalid submission nonce")?;
        let tag = hex::decode(&sealed.tag).context("invalid submission tag")?;
        ensure!(
            nonce.len() == 12 && tag.len() == 16,
            "invalid submission authentication material"
        );
        let ciphertext = hex::decode(&sealed.ciphertext).context("invalid submission ciphertext")?;
        let aad = Self::aad(provider, message, venue_id, &sealed.key_id)?;
        let mut plaintext = decrypt_aead(Cipher::aes_256_gcm(), key, Some(&nonce), &aad, &ciphertext, &tag)
            .map_err(|_| anyhow::anyhow!("submission payload authentication failed"))?;
        let result = serde_json::from_slice(&plaintext).map_err(|_| anyhow::anyhow!("invalid prepared SDK payload"));
        plaintext.fill(0);
        result
    }

    fn aad(provider: &str, message: &DurableMessage<TradeIntent>, venue_id: &str, key_id: &str) -> Result<Vec<u8>> {
        // Trace/attempt are deliberately excluded: retries retain the same business identity.
        Ok(serde_json::to_vec(&(
            1,
            provider,
            &message.scope,
            message.account_id,
            message.workflow_id,
            message.node_id,
            &message.run_id,
            message.seq,
            &message.payload,
            venue_id,
            key_id,
        ))?)
    }
}

impl Drop for PreparedPayloadCipher {
    fn drop(&mut self) {
        for key in self.keys.values_mut() {
            key.fill(0);
        }
    }
}

#[async_trait]
pub trait IOrderBookExchange: IExchange {
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error>;
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineModel>, Error>;
}

#[async_trait]
pub trait ICentralizedExchange: IOrderBookExchange + Send + Sync {}

#[async_trait]
pub trait IDecentralizedExchange: IOrderBookExchange + Send + Sync {}

#[async_trait]
pub trait IPredictionExchange: IExchange + Send + Sync {}

#[async_trait]
pub trait IStockExchange: IOrderBookExchange {
    /// Get current stock order list from the exchange
    /// Used for syncing historical orders to wallet manager's orders table
    async fn get_stock_orders(&self, account_id: Option<&str>, filters: Option<&str>) -> Result<Vec<OrderInfo>, Error>;
}

// SPDX-License-Identifier: GPL-3.0-only
// Execution mode selects an adapter; exchange production URLs are never rewritten.

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAdapterConfig {
    pub mode: ExecutionMode,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub token_secret: String,
    pub timeout_seconds: u64,
    #[serde(default)]
    pub live: Option<LiveExecutionConfig>,
}

/// A venue's immutable submission identity and encrypted SDK payload. The order
/// repository persists this before claiming the single submission attempt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreparedSubmission {
    pub venue_order_id: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPage {
    pub facts: Vec<ExecutionFact>,
    pub status: Option<OrderStatusFact>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub struct SubmissionIdentity;
impl SubmissionIdentity {
    /// Deterministic venue encoding of the existing business key (<= 36 bytes).
    pub fn venue_reference(message: &DurableMessage<TradeIntent>) -> Result<String> {
        let key = serde_json::to_vec(&(
            &message.scope,
            message.payload.account_id,
            message.node_id,
            &message.run_id,
            message.seq,
        ))?;
        Ok(format!("sg{}", &hex::encode(Sha256::digest(key))[..32]))
    }
}

#[async_trait]
pub trait IExecutionAdapter: IProvider {
    fn mode(&self) -> ExecutionMode;
    /// Validate supported asset semantics before the order runtime accepts intents.
    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<()> {
        instrument.validate()
    }
    /// Read confirmed venue facts. Cursor advancement and publication are owned
    /// by Order's transaction, never by the SDK or an in-memory checkpoint.
    async fn read_executions(
        &self,
        _input: &DurableMessage<TradeIntent>,
        _receipt: &OrderReceipt,
        _prepared: Option<&PreparedSubmission>,
        _instrument: &InstrumentSpec,
        _cursor: Option<&str>,
        _limit: u16,
    ) -> Result<ExecutionPage> {
        anyhow::bail!("authoritative execution/fee polling is unavailable for this adapter")
    }
    /// Local signing / venue ID allocation only; never sends a trading request.
    async fn prepare(&self, _intent: &DurableMessage<TradeIntent>) -> Result<Option<PreparedSubmission>> {
        Ok(None)
    }
    async fn submit_prepared(
        &self,
        _intent: &DurableMessage<TradeIntent>,
        _prepared: &PreparedSubmission,
    ) -> Result<OrderReceipt> {
        anyhow::bail!("this exchange adapter does not accept prepared submissions")
    }
    async fn reconcile_prepared(
        &self,
        _intent: &DurableMessage<TradeIntent>,
        _prepared: &PreparedSubmission,
    ) -> Result<Option<OrderReceipt>> {
        anyhow::bail!("this exchange adapter does not reconcile prepared submissions")
    }
    async fn submit(&self, intent: &DurableMessage<TradeIntent>) -> Result<OrderReceipt>;
    /// None means the venue has no authoritative answer yet, never permission to resubmit.
    async fn reconcile(&self, intent: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>>;
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveExecutionConfig {
    Ibkr(crate::ibkr::orders::IbkrExecutionConfig),
    Polymarket(crate::polymarket::orders::PolymarketExecutionConfig),
    BinanceSpot(crate::binance::orders::BinanceExecutionConfig),
}
