use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_exchange::exchange::PreparedKeyConfig;
use sigbot_exchange::exchange::PreparedPayloadCipher;
use sigbot_exchange::exchange::SubmissionIdentity;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::MarketEvidence;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use std::collections::BTreeMap;

struct Fixture;
impl Fixture {
    fn message() -> DurableMessage<TradeIntent> {
        let now = Utc::now();
        let account_id = Uuid::now_v7();
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "test".into(),
                tenant_id: Uuid::now_v7(),
            },
            workflow_id: Some(Uuid::now_v7()),
            account_id: Some(account_id),
            node_id: 7,
            target_node_id: None,
            run_id: Uuid::now_v7(),
            seq: 1,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: TradeIntent {
                execution_group: None,
                account_id,
                wallet_id: Uuid::now_v7(),
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
    std::env::set_var("SIGBOT__SECRETS__VALUES__TEST_PREPARED_KEY", "12".repeat(32));
    sigbot_core::config::config::refresh_config().unwrap();
    let config = PreparedKeyConfig {
        active_key: "test-v1".into(),
        keys: BTreeMap::from([("test-v1".into(), "test_prepared_key".into())]),
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
    for field in ["tenant", "account", "quantity"] {
        let mut changed = input.clone();
        match field {
            "tenant" => changed.scope.tenant_id = Uuid::now_v7(),
            "account" => changed.payload.account_id = Uuid::now_v7(),
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
