use chrono::{Duration, Utc};
use serde_json::{json, Value};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageEnvelope, MessageKind},
    topic::MessageScope,
};

struct Fixture;
impl Fixture {
    fn message() -> DurableMessage<Value> {
        let now = Utc::now();
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "paper".into(),
                tenant_id: 1,
                space_id: 2,
            },
            workflow_id: Some(3),
            account_id: None,
            node_id: 4,
            target_node_id: Some(5),
            run_id: "run-test".into(),
            seq: 0,
            attempt_no: 0,
            kind: MessageKind::Request,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: json!({"query":"LATEST"}),
        }
    }
}

#[test]
fn pairing_is_wire_only_and_request_uses_producer_span() {
    let wire = MessageEnvelope::from_durable(Fixture::message()).unwrap();
    assert_eq!(wire.request_span_id.as_deref(), Some("2222222222222222"));
    let json = serde_json::to_value(&wire).unwrap();
    assert_eq!(json["requestSpanId"], "2222222222222222");
    let roundtrip: MessageEnvelope<Value> = serde_json::from_value(json).unwrap();
    roundtrip.validate().unwrap();
    assert!(serde_json::to_value(&wire.durable)
        .unwrap()
        .get("requestSpanId")
        .is_none());
    let mut wrong = wire;
    wrong.request_span_id = Some("3333333333333333".into());
    assert!(wrong.validate().is_err());
}

#[test]
fn retry_keeps_business_identity_and_requires_a_new_span() {
    let mut message = Fixture::message();
    let later = message.produced_at + Duration::seconds(1);
    assert!(message.retry(message.traceparent.clone(), later).is_err());
    message
        .retry("00-11111111111111111111111111111111-3333333333333333-01".into(), later)
        .unwrap();
    assert_eq!(
        (message.run_id.as_str(), message.seq, message.attempt_no),
        ("run-test", 0, 1)
    );
    assert_eq!(
        MessageEnvelope::from_durable(message).unwrap().request_span_id.unwrap(),
        "3333333333333333"
    );
}

#[test]
fn payload_cannot_cross_tenant_or_workflow_boundaries() {
    let message = Fixture::message();
    assert!(message
        .validate_topic("internal/v2/paper/1/2/workflow/3/feed/snapshot/request")
        .is_ok());
    assert!(message
        .validate_topic("internal/v2/paper/8/2/workflow/3/feed/snapshot/request")
        .is_err());
    assert!(message
        .validate_topic("internal/v2/paper/1/2/workflow/9/feed/snapshot/request")
        .is_err());
}
