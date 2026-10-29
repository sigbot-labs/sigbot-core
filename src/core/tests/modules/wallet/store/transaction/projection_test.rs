use rust_decimal::Decimal;
use sigbot_core::modules::wallet::store::transaction::projection::{PositionProjection, TradeProjection};
use sigbot_types::modules::order::{execution::ExecutionFact, intent::OrderSide};
use std::str::FromStr;

#[test]
fn partial_close_and_reversal_have_correct_basis_and_realized_pnl() {
    let mut position = PositionProjection::default();
    position.apply(true, Decimal::from(100), Decimal::ONE).unwrap();
    position
        .apply(false, Decimal::from(120), Decimal::from_str("0.4").unwrap())
        .unwrap();
    assert_eq!(position.size, Decimal::from_str("0.6").unwrap());
    assert_eq!(position.entry_price, Decimal::from(100));
    assert_eq!(position.realized_pnl, Decimal::from(8));
    position.apply(false, Decimal::from(90), Decimal::ONE).unwrap();
    assert_eq!(position.size, Decimal::from_str("-0.4").unwrap());
    assert_eq!(position.entry_price, Decimal::from(90));
    assert_eq!(position.realized_pnl, Decimal::from(2));
}

#[test]
fn execution_assets_are_authoritative_for_opaque_instruments_and_third_asset_fees() {
    let mut execution = ExecutionFact {
        reservation_node_id: 5,
        reservation_seq: 0,
        wallet_id: 1,
        account_id: 1,
        exchange: "polymarket".into(),
        exchange_order_id: "venue-order".into(),
        exchange_trade_id: "venue-fill".into(),
        symbol: "756905732061891".into(),
        base_asset: "outcome:756905732061891".into(),
        quote_asset: "USDC".into(),
        side: OrderSide::Buy,
        price: Decimal::new(7, 1),
        quantity: Decimal::from(10),
        quote_quantity: None,
        fee: Decimal::new(2, 2),
        fee_asset: "FEE_TOKEN".into(),
        order_complete: true,
        executed_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    };
    let balances = TradeProjection::from_execution(&execution)
        .unwrap()
        .balance_deltas()
        .unwrap();
    assert_eq!(balances["outcome:756905732061891"], Decimal::from(10));
    assert_eq!(balances["USDC"], Decimal::from(-7));
    assert_eq!(balances["FEE_TOKEN"], Decimal::new(-2, 2));
    // Even a familiar crypto symbol cannot restore missing v2 metadata by guessing.
    execution.symbol = "BTCUSDT".into();
    execution.base_asset.clear();
    assert!(TradeProjection::from_execution(&execution).is_err());
}

#[test]
fn actual_quote_amount_survives_recurring_price_rounding_and_separate_fee_postings() {
    let mut execution: ExecutionFact = serde_json::from_value(serde_json::json!({
        "reservation_node_id":5,"reservation_seq":0,"wallet_id":1,"account_id":1,
        "exchange":"polymarket","exchange_order_id":"observed-order","exchange_trade_id":"observed-fill",
        "symbol":"outcome-token","base_asset":"outcome-token","quote_asset":"USDC","side":"BUY",
        "price":"0.3333333333333333333333333333","quantity":"3","quote_quantity":"1",
        "fee":"0.01","fee_asset":"USDC","order_complete":false,"executed_at":"2026-10-28T00:00:00Z"
    }))
    .unwrap();
    let projection = TradeProjection::from_execution(&execution).unwrap();
    assert_ne!(execution.price * execution.quantity, Decimal::ONE);
    assert_eq!(projection.quote_quantity, Decimal::ONE);
    assert_eq!(
        projection.movements().unwrap(),
        vec![
            ("outcome-token".into(), Decimal::from(3), "clearing"),
            ("USDC".into(), -Decimal::ONE, "clearing"),
            ("USDC".into(), Decimal::new(-1, 2), "fee"),
        ]
    );
    assert_eq!(projection.balance_deltas().unwrap()["USDC"], Decimal::new(-101, 2));
    execution.side = OrderSide::Sell;
    assert_eq!(
        TradeProjection::from_execution(&execution)
            .unwrap()
            .balance_deltas()
            .unwrap()["USDC"],
        Decimal::new(99, 2)
    );
    execution.quote_quantity = None;
    assert_eq!(
        TradeProjection::from_execution(&execution).unwrap().quote_quantity,
        execution.price * execution.quantity
    );
    execution.quote_quantity = Some(Decimal::ZERO);
    assert!(TradeProjection::from_execution(&execution).is_err());
}

#[test]
fn multiple_openings_and_partial_closes_allocate_all_actual_cost_without_residue() {
    let third = Decimal::from_str("0.3333333333333333333333333333").unwrap();
    for buy in [true, false] {
        let mut position = PositionProjection::default();
        position
            .apply_quote(buy, third, Decimal::from(3), Decimal::ONE)
            .unwrap();
        position
            .apply_quote(buy, third, Decimal::from(3), Decimal::ONE)
            .unwrap();
        assert_eq!(position.cost_basis, Decimal::from(2));
        assert_eq!(position.size.abs(), Decimal::from(6));
        position
            .apply_quote(!buy, Decimal::new(5, 1), Decimal::ONE, Decimal::new(5, 1))
            .unwrap();
        position
            .apply_quote(!buy, Decimal::new(5, 1), Decimal::from(2), Decimal::ONE)
            .unwrap();
        position
            .apply_quote(!buy, Decimal::new(5, 1), Decimal::from(3), Decimal::new(15, 1))
            .unwrap();
        assert_eq!(position.size, Decimal::ZERO);
        assert_eq!(position.cost_basis, Decimal::ZERO);
        assert_eq!(position.entry_price, Decimal::ZERO);
        assert_eq!(position.realized_pnl, if buy { Decimal::ONE } else { -Decimal::ONE });
    }
}

#[test]
fn reversal_carries_the_unallocated_quote_amount_into_the_new_position() {
    let mut position = PositionProjection::default();
    let third = Decimal::from_str("0.3333333333333333333333333333").unwrap();
    position
        .apply_quote(true, third, Decimal::from(3), Decimal::ONE)
        .unwrap();
    let proceeds = Decimal::new(22, 1);
    let close_proceeds = proceeds * Decimal::from(3) / Decimal::from(7);
    position
        .apply_quote(false, proceeds / Decimal::from(7), Decimal::from(7), proceeds)
        .unwrap();
    assert_eq!(position.size, Decimal::from(-4));
    assert_eq!(position.cost_basis, proceeds - close_proceeds);
    position
        .apply_quote(true, Decimal::new(25, 2), Decimal::from(4), Decimal::ONE)
        .unwrap();
    assert_eq!(position.cost_basis, Decimal::ZERO);
    assert_eq!(position.size, Decimal::ZERO);
    assert_eq!(position.realized_pnl, Decimal::new(2, 1));
}
