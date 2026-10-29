use chrono::Utc;
use serde_json::Value;
use sigbot_messager::client::reply_pairing::ReplyPairingCache;
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageKind},
    topic::MessageScope,
};
use std::time::Duration;

#[test]
fn pairing_expires_is_bounded_and_never_changes_durable_payload() {
    let now = Utc::now();
    let mut response = DurableMessage {
        version: 2,
        scope: MessageScope {
            env: "paper".into(),
            tenant_id: 1,
            space_id: 2,
        },
        workflow_id: Some(3),
        account_id: None,
        node_id: 4,
        target_node_id: None,
        run_id: "run".into(),
        seq: 1,
        attempt_no: 0,
        kind: MessageKind::Result,
        event_time: now,
        available_time: now,
        produced_at: now,
        traceparent: "00-11111111111111111111111111111111-3333333333333333-01".into(),
        payload: Value::Null,
    };
    let cache = ReplyPairingCache::new(1, Duration::from_millis(20)).unwrap();
    cache.remember(&response, "2222222222222222").unwrap();
    assert_eq!(cache.get(&response).as_deref(), Some("2222222222222222"));
    assert!(!serde_json::to_string(&response).unwrap().contains("requestSpanId"));
    let first = response.clone();
    response.seq = 2;
    cache.remember(&response, "4444444444444444").unwrap();
    assert!(cache.get(&first).is_none());
    std::thread::sleep(Duration::from_millis(30));
    assert!(cache.get(&response).is_none());
}
