use binance_sdk::spot::rest_api::HistoricalTradesResponseInner;
use chrono::Duration;
use chrono::Utc;
use serde_json::json;
use sigbot_exchange::market::market::HyperliquidPublicTrade;
use sigbot_exchange::market::market::MarketExchanges;
use sigbot_exchange::market::market::PublicMarketNormalizer;
use sigbot_exchange::market::market::PublicMarketSourceConfig;

struct Sources;
impl Sources {
    fn binance() -> PublicMarketSourceConfig {
        serde_json::from_value(json!({"source":"binance","provider":"binance_spot",
            "symbols":{"HYPEUSDC":"HYPEUSDC"}}))
        .unwrap()
    }

    fn hyperliquid() -> PublicMarketSourceConfig {
        serde_json::from_value(json!({"source":"hyperliquid","provider":"hyperliquid_spot",
            "symbols":{"HYPEUSDC":"@107"}}))
        .unwrap()
    }
}

#[tokio::test]
async fn configuration_rejects_ambiguous_sources_and_unconfigured_symbols_before_network() -> anyhow::Result<()> {
    assert!(serde_json::from_value::<PublicMarketSourceConfig>(json!({
        "source":"binance","provider":"binance_spot","symbols":{"HYPEUSDC":"HYPEUSDC"},
        "api_key":"must-not-be-accepted"
    }))
    .is_err());
    let mut config = Sources::hyperliquid();
    config.symbols.insert("HYPEUSDC".to_owned(), "HYPEUSDC".to_owned());
    assert!(
        config.validate().is_err(),
        "spot must use the explicit venue asset index"
    );
    assert!(MarketExchanges::create(vec![Sources::binance(), Sources::binance()])
        .await
        .is_err());
    let exchanges = MarketExchanges::create(vec![Sources::binance(), Sources::hyperliquid()]).await?;
    assert!(exchanges
        .snapshot(&[("missing".to_owned(), "HYPEUSDC".to_owned())])
        .await
        .is_err());
    assert!(exchanges
        .snapshot(&[("binance".to_owned(), "BTCUSDT".to_owned())])
        .await
        .is_err());
    Ok(())
}

#[test]
fn native_trade_identity_is_stable_and_future_or_incomplete_facts_fail() -> anyhow::Result<()> {
    let now = Utc::now();
    let time = (now - Duration::seconds(1)).timestamp_millis();
    let payload = json!({"id":7,"price":"79.12340000","qty":"1","quoteQty":"79.12340000",
        "time":time,"isBuyerMaker":false,"isBestMatch":true});
    let trade: HistoricalTradesResponseInner = serde_json::from_value(payload.clone())?;
    let first = PublicMarketNormalizer::binance(&Sources::binance(), "HYPEUSDC", trade, now)?;
    let again = PublicMarketNormalizer::binance(
        &Sources::binance(),
        "HYPEUSDC",
        serde_json::from_value(payload.clone())?,
        now + Duration::seconds(1),
    )?;
    assert_eq!(first.source_event_id, "binance_spot:HYPEUSDC:trade:7");
    assert_eq!(first.source_event_id, again.source_event_id);
    assert_eq!(first.raw, again.raw);
    assert_eq!(first.seq, 7);
    assert_eq!(first.revision, 0);
    let mut other_instrument = Sources::binance();
    other_instrument.symbols.insert("BTCUSDC".into(), "BTCUSDC".into());
    let other = PublicMarketNormalizer::binance(
        &other_instrument,
        "BTCUSDC",
        serde_json::from_value(payload.clone())?,
        now,
    )?;
    assert_ne!(
        first.source_event_id, other.source_event_id,
        "venue trade IDs are symbol scoped"
    );
    let mut future = payload.clone();
    future["time"] = json!((now + Duration::seconds(1)).timestamp_millis());
    assert!(
        PublicMarketNormalizer::binance(&Sources::binance(), "HYPEUSDC", serde_json::from_value(future)?, now).is_err()
    );
    let mut missing = payload;
    missing.as_object_mut().unwrap().remove("id");
    assert!(
        PublicMarketNormalizer::binance(&Sources::binance(), "HYPEUSDC", serde_json::from_value(missing)?, now)
            .is_err()
    );
    Ok(())
}

#[test]
fn hyperliquid_uses_real_tid_even_when_hash_is_zero() -> anyhow::Result<()> {
    let now = Utc::now();
    let mut payload = json!({"coin":"@107","side":"B","px":"79.1234","sz":"1",
        "time":now.timestamp_millis(),"hash":"0x0000000000000000","tid":41});
    let first = PublicMarketNormalizer::hyperliquid(
        &Sources::hyperliquid(),
        "HYPEUSDC",
        serde_json::from_value::<HyperliquidPublicTrade>(payload.clone())?,
        now,
    )?;
    payload["tid"] = json!(42);
    let second = PublicMarketNormalizer::hyperliquid(
        &Sources::hyperliquid(),
        "HYPEUSDC",
        serde_json::from_value(payload.clone())?,
        now,
    )?;
    assert_ne!(first.source_event_id, second.source_event_id);
    assert_eq!(second.seq, 42);
    assert_eq!(second.raw["source_payload"]["tid"], 42);
    payload["coin"] = json!("BTC");
    assert!(PublicMarketNormalizer::hyperliquid(
        &Sources::hyperliquid(),
        "HYPEUSDC",
        serde_json::from_value(payload)?,
        now
    )
    .is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "read-only real Binance/Hyperliquid public SDK endpoints; needs network, no financial credentials"]
async fn two_real_public_venues_return_timestamped_native_trade_facts() -> anyhow::Result<()> {
    let exchanges = MarketExchanges::create(vec![Sources::binance(), Sources::hyperliquid()]).await?;
    let facts = exchanges
        .snapshot(&[
            ("binance".into(), "HYPEUSDC".into()),
            ("hyperliquid".into(), "HYPEUSDC".into()),
        ])
        .await?;
    assert_eq!(facts.len(), 2);
    for fact in facts {
        assert!(fact.event_time <= fact.available_time);
        assert!(
            (Utc::now() - fact.event_time).num_seconds() < 300,
            "actual venue data is stale"
        );
        assert_eq!(fact.raw["price_type"], "last_trade");
        assert!(fact.raw["source_payload"].is_object());
        assert_eq!(fact.revision, 0);
    }
    Ok(())
}
