// SPDX-License-Identifier: GPL-3.0-only
//! Public, read-only SDK adapters. Datafeed alone persists and publishes observations.
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use binance_sdk::{
    config::ConfigurationRestApi,
    spot::{
        rest_api::{GetTradesParams, HistoricalTradesResponseInner, RestApi},
        SpotRestApi,
    },
};
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, stream, StreamExt, TryStreamExt};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, InfoRequest, RecentTradesResponse};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sigbot_types::modules::datafeed::event::DatafeedEvent;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Mutex, time::Instant};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublicMarketProvider {
    BinanceSpot,
    HyperliquidSpot,
    HyperliquidPerpetual,
}

impl PublicMarketProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSpot => "binance_spot",
            Self::HyperliquidSpot => "hyperliquid_spot",
            Self::HyperliquidPerpetual => "hyperliquid_perpetual",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicMarketSourceConfig {
    pub source: String,
    pub provider: PublicMarketProvider,
    /// Explicit query symbol -> venue symbol. No inferred asset or quote conversion.
    pub symbols: BTreeMap<String, String>,
    #[serde(default = "PublicMarketSourceConfig::default_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "PublicMarketSourceConfig::default_interval")]
    pub min_request_interval_ms: u64,
}

impl PublicMarketSourceConfig {
    fn default_timeout() -> u64 {
        5_000
    }
    fn default_interval() -> u64 {
        1_000
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.source.is_empty()
                && self.source.len() <= 64
                && self
                    .source
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')),
            "invalid public market source"
        );
        ensure!(
            !self.symbols.is_empty() && self.symbols.len() <= 256,
            "invalid public market symbol count"
        );
        ensure!(
            (100..=10_000).contains(&self.timeout_ms) && (1_000..=60_000).contains(&self.min_request_interval_ms),
            "invalid public market timeout/rate"
        );
        for (symbol, venue) in &self.symbols {
            ensure!(
                !symbol.is_empty()
                    && symbol.len() <= 64
                    && symbol
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'/' | b'.' | b':')),
                "invalid public market query symbol"
            );
            let valid = match self.provider {
                PublicMarketProvider::BinanceSpot => {
                    !venue.is_empty()
                        && venue.len() <= 32
                        && venue.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                }
                PublicMarketProvider::HyperliquidSpot => {
                    venue == "PURR/USDC"
                        || venue.strip_prefix('@').is_some_and(|index| {
                            !index.is_empty() && index.len() <= 8 && index.bytes().all(|b| b.is_ascii_digit())
                        })
                }
                PublicMarketProvider::HyperliquidPerpetual => {
                    !venue.is_empty()
                        && venue.len() <= 64
                        && venue
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'-' | b'_'))
                }
            };
            ensure!(valid, "invalid public market venue symbol for provider");
        }
        Ok(())
    }
}

#[async_trait]
pub trait IPublicMarketAdapter: Send + Sync {
    fn config(&self) -> &PublicMarketSourceConfig;
    async fn snapshot(&self, symbol: &str) -> Result<DatafeedEvent>;
}

/// Runtime-local factory: clients/configuration are never shared across tenant nodes.
pub struct PublicMarketAdapterFactory {
    adapters: BTreeMap<String, Arc<dyn IPublicMarketAdapter>>,
}

impl PublicMarketAdapterFactory {
    /// SDK construction is local. AS_OF can configure adapters without any HTTP request.
    pub async fn create(configs: Vec<PublicMarketSourceConfig>) -> Result<Self> {
        ensure!(
            !configs.is_empty() && configs.len() <= 32,
            "invalid active source count"
        );
        let mut adapters: BTreeMap<String, Arc<dyn IPublicMarketAdapter>> = BTreeMap::new();
        for config in configs {
            config.validate()?;
            ensure!(!adapters.contains_key(&config.source), "duplicate active source");
            let source = config.source.clone();
            let rate = PublicMarketRate::new(&config);
            let adapter: Arc<dyn IPublicMarketAdapter> = match config.provider {
                PublicMarketProvider::BinanceSpot => {
                    let settings = ConfigurationRestApi::builder()
                        .base_path("https://data-api.binance.vision")
                        .timeout(config.timeout_ms)
                        .retries(0)
                        .build()?;
                    Arc::new(BinancePublicMarket {
                        client: SpotRestApi::from_config(settings),
                        config,
                        rate,
                    })
                }
                PublicMarketProvider::HyperliquidSpot | PublicMarketProvider::HyperliquidPerpetual => {
                    Arc::new(HyperliquidPublicMarket {
                        client: InfoClient::new(None, Some(BaseUrl::Mainnet)).await?,
                        config,
                        rate,
                    })
                }
            };
            adapters.insert(source, adapter);
        }
        Ok(Self { adapters })
    }

    /// Every requested point must come from a configured provider; no stale-cache fallback.
    pub async fn snapshot(&self, pairs: &[(String, String)]) -> Result<Vec<DatafeedEvent>> {
        ensure!(
            !pairs.is_empty() && pairs.len() <= 32,
            "active snapshot exceeds 32 point bound"
        );
        let mut unique = HashSet::new();
        let mut jobs: Vec<BoxFuture<'static, Result<DatafeedEvent>>> = Vec::with_capacity(pairs.len());
        for (source, symbol) in pairs {
            ensure!(unique.insert((source, symbol)), "duplicate active snapshot point");
            let adapter = self
                .adapters
                .get(source)
                .context("snapshot source is not actively configured")?;
            ensure!(
                adapter.config().symbols.contains_key(symbol),
                "snapshot instrument is not configured"
            );
            let adapter = adapter.clone();
            let symbol = symbol.clone();
            jobs.push(Box::pin(async move { adapter.snapshot(&symbol).await }));
        }
        // Futures own their adapter and symbol. Borrowed stream items otherwise leak
        // higher-ranked lifetimes into the MQTT handler's Send future.
        let fetch = stream::iter(jobs).buffered(4).try_collect();
        tokio::time::timeout(Duration::from_secs(15), fetch)
            .await
            .context("active snapshot deadline exceeded")?
    }
}

struct PublicMarketRate {
    next: Mutex<Instant>,
    interval: Duration,
}

impl PublicMarketRate {
    fn new(config: &PublicMarketSourceConfig) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            interval: Duration::from_millis(config.min_request_interval_ms),
        }
    }

    async fn wait(&self) {
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.interval;
    }
}

struct BinancePublicMarket {
    client: RestApi,
    config: PublicMarketSourceConfig,
    rate: PublicMarketRate,
}

#[async_trait]
impl IPublicMarketAdapter for BinancePublicMarket {
    fn config(&self) -> &PublicMarketSourceConfig {
        &self.config
    }

    async fn snapshot(&self, symbol: &str) -> Result<DatafeedEvent> {
        let venue = self.config.symbols.get(symbol).context("unconfigured Binance symbol")?;
        self.rate.wait().await;
        let params = GetTradesParams::builder(venue.clone()).limit(1).build()?;
        let trades = self.client.get_trades(params).await?.data().await?;
        let trade = trades
            .into_iter()
            .max_by_key(|row| (row.time, row.id))
            .context("Binance returned no trade")?;
        PublicMarketNormalizer::binance(&self.config, symbol, trade, Utc::now())
    }
}

struct HyperliquidPublicMarket {
    client: InfoClient,
    config: PublicMarketSourceConfig,
    rate: PublicMarketRate,
}

/// SDK 0.6 omits `tid`; compose its model to retain the venue's actual trade identity.
#[derive(Debug, Deserialize)]
pub struct HyperliquidPublicTrade {
    #[serde(flatten)]
    pub trade: RecentTradesResponse,
    pub tid: u64,
}

#[async_trait]
impl IPublicMarketAdapter for HyperliquidPublicMarket {
    fn config(&self) -> &PublicMarketSourceConfig {
        &self.config
    }

    async fn snapshot(&self, symbol: &str) -> Result<DatafeedEvent> {
        let venue = self
            .config
            .symbols
            .get(symbol)
            .context("unconfigured Hyperliquid symbol")?;
        self.rate.wait().await;
        let request = serde_json::to_string(&InfoRequest::RecentTrades { coin: venue.clone() })?;
        // Reuse the official SDK request enum, HTTP transport and response model.
        let body = tokio::time::timeout(
            Duration::from_millis(self.config.timeout_ms),
            self.client.http_client.post("/info", request),
        )
        .await
        .context("Hyperliquid snapshot timeout")??;
        ensure!(
            body.len() <= 1_048_576,
            "Hyperliquid public trade response exceeds bound"
        );
        let trades: Vec<HyperliquidPublicTrade> = serde_json::from_str(&body)?;
        ensure!(trades.len() <= 2_000, "Hyperliquid public trade count exceeds bound");
        let trade = trades
            .into_iter()
            .max_by_key(|row| (row.trade.time, row.tid))
            .context("Hyperliquid returned no trade")?;
        PublicMarketNormalizer::hyperliquid(&self.config, symbol, trade, Utc::now())
    }
}

pub struct PublicMarketNormalizer;

impl PublicMarketNormalizer {
    pub fn binance(
        config: &PublicMarketSourceConfig,
        symbol: &str,
        trade: HistoricalTradesResponseInner,
        available_time: DateTime<Utc>,
    ) -> Result<DatafeedEvent> {
        ensure!(
            config.provider == PublicMarketProvider::BinanceSpot,
            "incorrect Binance normalizer provider"
        );
        let id = trade.id.context("Binance trade id missing")?;
        let time = trade.time.context("Binance trade time missing")?;
        let price = trade
            .price
            .as_deref()
            .context("Binance trade price missing")?
            .parse::<Decimal>()?;
        Self::event(
            config,
            symbol,
            id,
            time,
            price,
            serde_json::to_value(trade)?,
            available_time,
        )
    }

    pub fn hyperliquid(
        config: &PublicMarketSourceConfig,
        symbol: &str,
        row: HyperliquidPublicTrade,
        available_time: DateTime<Utc>,
    ) -> Result<DatafeedEvent> {
        ensure!(
            matches!(
                config.provider,
                PublicMarketProvider::HyperliquidSpot | PublicMarketProvider::HyperliquidPerpetual
            ),
            "incorrect Hyperliquid normalizer provider"
        );
        let trade = row.trade;
        ensure!(
            config.symbols.get(symbol) == Some(&trade.coin),
            "Hyperliquid returned another instrument"
        );
        ensure!(
            matches!(trade.side.as_str(), "A" | "B"),
            "invalid Hyperliquid trade side"
        );
        let price = trade.px.parse::<Decimal>()?;
        Self::event(
            config,
            symbol,
            i64::try_from(row.tid)?,
            i64::try_from(trade.time)?,
            price,
            json!({"coin":trade.coin,"side":trade.side,"px":trade.px,"sz":trade.sz,
                "time":trade.time,"hash":trade.hash,"tid":row.tid}),
            available_time,
        )
    }

    fn event(
        config: &PublicMarketSourceConfig,
        symbol: &str,
        trade_id: i64,
        time: i64,
        price: Decimal,
        payload: serde_json::Value,
        available_time: DateTime<Utc>,
    ) -> Result<DatafeedEvent> {
        let venue = config
            .symbols
            .get(symbol)
            .context("unconfigured normalization symbol")?;
        ensure!(
            trade_id >= 0 && price > Decimal::ZERO,
            "invalid public trade identity or price"
        );
        let event_time = DateTime::from_timestamp_millis(time).context("invalid public trade timestamp")?;
        ensure!(event_time <= available_time, "public market event is in the future");
        Ok(DatafeedEvent {
            source: config.source.clone(),
            source_event_id: format!("{}:{venue}:trade:{trade_id}", config.provider.as_str()),
            symbol: symbol.to_owned(),
            event_type: "market".to_owned(),
            event_time,
            available_time,
            seq: trade_id,
            revision: 0,
            content: None,
            raw: json!({"price":price.to_string(),"price_type":"last_trade","provider":config.provider,
                "venue_symbol":venue,"source_payload":payload}),
        })
    }
}
