use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use sigbot_order::manager::risk::{InstrumentRules, RiskLimits, RiskPolicy};
use sigbot_types::modules::order::intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent};

struct Fixture;
impl Fixture {
    fn policy() -> RiskPolicy {
        RiskPolicy {
            instrument: InstrumentRules {
                exchange: "binance".into(),
                symbol: "BTCUSDT".into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                quantity_step: Decimal::new(1, 3),
                price_tick: Decimal::new(1, 2),
                min_notional: Decimal::ONE,
            },
            limits: RiskLimits {
                max_order_notional: Decimal::from(1000),
                max_account_notional: Decimal::from(2000),
                max_position_quantity: Decimal::from(2),
                max_market_age_ms: 1000,
                fee_buffer_bps: Decimal::from(10),
            },
        }
    }
    fn intent() -> TradeIntent {
        let now = Utc::now();
        TradeIntent {
            execution_group: None,
            account_id: 1,
            wallet_id: 1,
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            side: OrderSide::Buy,
            quantity: Decimal::ONE,
            order_type: IntentOrderType::Market,
            limit_price: None,
            reduce_only: false,
            market: MarketEvidence {
                source: "binance".into(),
                event_time: now,
                available_time: now,
                price: Decimal::from(100),
            },
        }
    }
}

#[test]
fn rejects_unavailable_stale_and_invalid_precision_market() {
    let policy = Fixture::policy();
    let mut intent = Fixture::intent();
    let now = intent.market.available_time;
    assert!(policy
        .check(
            &intent,
            now - Duration::milliseconds(1),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO
        )
        .is_err());
    assert!(policy
        .check(
            &intent,
            now + Duration::seconds(2),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO
        )
        .is_err());
    intent.quantity = Decimal::new(10001, 4);
    assert!(policy
        .check(&intent, now, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
        .is_err());
}

#[test]
fn reserves_exact_decimal_and_counts_pending_reductions() {
    let policy = Fixture::policy();
    let mut intent = Fixture::intent();
    let now = intent.market.available_time;
    let (asset, amount) = policy
        .check(&intent, now, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
        .unwrap();
    assert_eq!(asset, "USDT");
    assert_eq!(amount, Decimal::new(1001, 1));
    intent.side = OrderSide::Sell;
    intent.reduce_only = true;
    assert!(policy
        .check(&intent, now, Decimal::ONE, Decimal::new(1, 1), Decimal::ZERO)
        .is_err());
    assert!(policy
        .check(&intent, now, Decimal::ONE, Decimal::ZERO, Decimal::ZERO)
        .is_ok());
}

#[test]
fn arithmetic_overflow_is_a_rejection_not_a_panic() {
    let mut policy = Fixture::policy();
    let intent = Fixture::intent();
    let now = intent.market.available_time;
    policy.limits.max_account_notional = Decimal::MAX;
    assert!(policy
        .check(&intent, now, Decimal::ZERO, Decimal::ZERO, Decimal::MAX)
        .is_err());
}

#[test]
fn closing_at_an_exposure_breach_reserves_only_the_delivered_asset() {
    let policy = Fixture::policy();
    let mut intent = Fixture::intent();
    intent.side = OrderSide::Sell;
    intent.reduce_only = true;
    let (asset, amount) = policy
        .check(
            &intent,
            intent.market.available_time,
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::from(2100),
        )
        .unwrap();
    assert_eq!(asset, "BTC");
    assert_eq!(amount, Decimal::ONE);
    intent.quantity = Decimal::from(2);
    assert!(policy
        .check(
            &intent,
            intent.market.available_time,
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::from(2100)
        )
        .is_err());
}
