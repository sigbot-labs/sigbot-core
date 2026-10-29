use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use sigbot_types::modules::strategy::runtime::{
    MarketRegime, RegimeProbabilities, SnapshotMode, SnapshotPoint, SnapshotQuery, SnapshotResult,
};

#[test]
fn regime_ties_are_deterministic_and_invalid_probabilities_fail_closed() {
    let mut probabilities = RegimeProbabilities {
        trend: 0.5,
        range: 0.5,
        stabilizing: 0.0,
        suppression: 0.0,
        unknown: 0.0,
    };
    assert_eq!(probabilities.dominant().unwrap(), MarketRegime::Trend);
    probabilities.unknown = f64::NAN;
    assert!(probabilities.validate().is_err());
}

#[test]
fn independent_snapshots_enforce_as_of_freshness_and_source_skew() {
    let now = Utc::now();
    let query = SnapshotQuery {
        sources: vec!["binance".into(), "ibkr".into()],
        symbols: vec!["BTCUSDT".into()],
        mode: SnapshotMode::AsOf(now),
        max_age_ms: 1000,
        max_skew_ms: 100,
        max_rows: 2,
    };
    let point = SnapshotPoint {
        source: "binance".into(),
        symbol: "BTCUSDT".into(),
        event_time: now - Duration::milliseconds(50),
        available_time: now,
        price: Decimal::ONE,
    };
    let mut result = SnapshotResult {
        points: vec![point.clone()],
    };
    query.validate_result(&result, now).unwrap();
    let mut other = point.clone();
    other.source = "ibkr".into();
    other.event_time -= Duration::milliseconds(200);
    result.points.push(other);
    assert!(query.validate_result(&result, now).is_err());
    result.points = vec![point];
    result.points[0].available_time += Duration::nanoseconds(1);
    assert!(query.validate_result(&result, now).is_err());
}
