use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_exchange::client::{
    execution_adapter::SubmissionIdentity,
    prepared_payload::{PreparedKeyConfig, PreparedPayloadCipher},
};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::MessageScope,
    },
    order::intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent},
};
use std::collections::BTreeMap;

struct Fixture;
impl Fixture {
    fn message() -> DurableMessage<TradeIntent> {
        let now = Utc::now();
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "simulated".into(),
                tenant_id: 81,
                space_id: 12,
            },
            workflow_id: Some(5),
            account_id: Some(19),
            node_id: 7,
            target_node_id: None,
            run_id: "prepared-restart".into(),
            seq: 1,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: TradeIntent {
                execution_group: None,
                account_id: 19,
                wallet_id: 20,
                exchange: "ibkr".into(),
                symbol: "AAPL".into(),
                side: OrderSide::Buy,
                quantity: Decimal::ONE,
                order_type: IntentOrderType::Limit,
                limit_price: Some(Decimal::from(100)),
                reduce_only: false,
                market: MarketEvidence {
                    source: "ibkr".into(),
                    event_time: now,
                    available_time: now,
                    price: Decimal::from(100),
                },
            },
        }
    }
}

#[test]
fn sealed_sdk_payload_survives_restart_but_rejects_scope_and_intent_changes() {
    std::env::set_var("SIGBOT_TEST_PREPARED_KEY", "12".repeat(32));
    let config = PreparedKeyConfig {
        active_key: "test-v1".into(),
        keys: BTreeMap::from([("test-v1".into(), "SIGBOT_TEST_PREPARED_KEY".into())]),
    };
    let input = Fixture::message();
    let expected = json!({"sdk_signature":"must-remain-encrypted", "venue_id": 1234});
    let cipher = PreparedPayloadCipher::new(&config).unwrap();
    let sealed = cipher.seal("ibkr", &input, "ibkr:8:1234", &expected).unwrap();
    assert!(!sealed.to_string().contains("must-remain-encrypted"));
    drop(cipher);
    let restarted = PreparedPayloadCipher::new(&config).unwrap();
    let mut retry = input.clone();
    retry.attempt_no += 1;
    retry.traceparent = "00-11111111111111111111111111111111-3333333333333333-01".into();
    assert_eq!(
        restarted
            .open::<serde_json::Value>("ibkr", &retry, "ibkr:8:1234", &sealed)
            .unwrap(),
        expected
    );
    assert_eq!(
        SubmissionIdentity::venue_reference(&input).unwrap(),
        SubmissionIdentity::venue_reference(&retry).unwrap()
    );
    for field in ["tenant", "space", "account", "quantity"] {
        let mut changed = input.clone();
        match field {
            "tenant" => changed.scope.tenant_id += 1,
            "space" => changed.scope.space_id += 1,
            "account" => changed.payload.account_id += 1,
            _ => changed.payload.quantity += Decimal::ONE,
        }
        assert!(restarted
            .open::<serde_json::Value>("ibkr", &changed, "ibkr:8:1234", &sealed)
            .is_err());
    }
    assert!(restarted
        .open::<serde_json::Value>("polymarket", &input, "ibkr:8:1234", &sealed)
        .is_err());
    assert!(restarted
        .open::<serde_json::Value>("ibkr", &input, "ibkr:8:1235", &sealed)
        .is_err());
    let mut damaged = sealed.clone();
    damaged["tag"] = json!("00".repeat(16));
    assert!(restarted
        .open::<serde_json::Value>("ibkr", &input, "ibkr:8:1234", &damaged)
        .is_err());
}
