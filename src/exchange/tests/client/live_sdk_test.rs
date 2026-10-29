use ibapi::orders::OrderStatusKind;
use polymarket_client_sdk_v2::{
    auth::Uuid,
    clob::types::{OrderPayload, OrderStatusType, OrderType, OrderV2, SignedOrder},
    types::{Address, Signature, U256},
};
use rust_decimal::Decimal;
use sigbot_exchange::client::{
    ibkr_execution::IbkrExecution,
    polymarket_execution::{PolymarketExecution, PreparedPolymarketOrder},
};
use sigbot_types::modules::order::execution::OrderState;
use std::str::FromStr;

#[test]
fn sdk_receipts_do_not_promote_pending_or_matching_into_settled_execution() {
    assert_eq!(
        IbkrExecution::state(&OrderStatusKind::PendingSubmit, 0.0, None),
        OrderState::Unknown
    );
    assert_eq!(
        IbkrExecution::state(&OrderStatusKind::Inactive, 0.0, None),
        OrderState::Unknown
    );
    assert_eq!(
        IbkrExecution::state(&OrderStatusKind::Submitted, 1.0, Some(2.0)),
        OrderState::PartiallyFilled
    );
    assert_eq!(
        PolymarketExecution::state(&OrderStatusType::Matched, false),
        OrderState::Acknowledged
    );
    assert_eq!(
        PolymarketExecution::state(&OrderStatusType::Live, true),
        OrderState::PartiallyFilled
    );
    assert!(IbkrExecution::exact_sdk_number(Decimal::from_str("9007199254740993").unwrap()).is_err());
    assert_eq!(
        IbkrExecution::exact_sdk_number(Decimal::from_str("0.125").unwrap()).unwrap(),
        0.125
    );
}

#[test]
fn official_signed_order_roundtrip_keeps_exact_hash_and_wire_body() {
    let mut order = OrderV2::default();
    order.salt = U256::from(128);
    order.maker = Address::from([1; 20]);
    order.signer = order.maker;
    order.tokenId = U256::from(555);
    order.makerAmount = U256::from(500_000);
    order.takerAmount = U256::from(1_000_000);
    order.timestamp = U256::from(1_700_000_000_000_u64);
    for version in [2, 3] {
        let payload = if version == 2 {
            OrderPayload::new(order.clone(), U256::ZERO)
        } else {
            OrderPayload::new_v3(order.clone(), U256::ZERO)
        };
        let signed = SignedOrder::builder()
            .payload(payload)
            .signature(Signature::new(U256::from(1), U256::from(2), false))
            .order_type(OrderType::GTC)
            .owner(Uuid::from_u128(7))
            .post_only(false)
            .defer_exec(true)
            .build();
        let expected = serde_json::to_value(&signed).unwrap();
        let prepared = PreparedPolymarketOrder::from_signed(signed, Address::from([2; 20])).unwrap();
        let hash = prepared.venue_order_id().unwrap();
        let persisted = serde_json::to_vec(&prepared).unwrap();
        let recovered: PreparedPolymarketOrder = serde_json::from_slice(&persisted).unwrap();
        assert_eq!(recovered.venue_order_id().unwrap(), hash);
        assert_eq!(
            serde_json::to_value(recovered.restore(&hash).unwrap()).unwrap(),
            expected
        );
        assert!(recovered.restore(&format!("0x{}", "00".repeat(32))).is_err());
    }
}

#[test]
fn ibkr_execution_time_uses_gateway_zone_and_rejects_dst_ambiguity() {
    let zone = time_tz::timezones::get_by_name("US/Eastern").unwrap();
    assert_eq!(
        IbkrExecution::execution_timestamp("20251022 14:56:24 US/Eastern", None)
            .unwrap()
            .to_rfc3339(),
        "2025-10-22T18:56:24+00:00"
    );
    assert_eq!(
        IbkrExecution::execution_timestamp("20251022 14:56:24", Some(zone))
            .unwrap()
            .to_rfc3339(),
        "2025-10-22T18:56:24+00:00"
    );
    assert_eq!(
        IbkrExecution::execution_timestamp("20251022-14:56:24", None)
            .unwrap()
            .to_rfc3339(),
        "2025-10-22T14:56:24+00:00"
    );
    assert!(IbkrExecution::execution_timestamp("20251102 01:30:00 US/Eastern", None).is_err());
    assert!(IbkrExecution::execution_timestamp("20250309 02:30:00 US/Eastern", None).is_err());
    assert!(IbkrExecution::execution_timestamp("20251022 14:56:24", None).is_err());
}
