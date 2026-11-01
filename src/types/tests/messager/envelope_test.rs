use chrono::Duration;
use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;

struct Fixture;
impl Fixture {
    fn message() -> DurableMessage<Value> {
        let now = Utc::now();
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "paper".into(),
                tenant_id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000101").unwrap(),
            },
            workflow_id: Some(Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000102").unwrap()),
            account_id: None,
            node_id: 4,
            target_node_id: Some(5),
            run_id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000103").unwrap(),
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
    let run_id = message.run_id;
    let later = message.produced_at + Duration::seconds(1);
    assert!(message.retry(message.traceparent.clone(), later).is_err());
    message
        .retry("00-11111111111111111111111111111111-3333333333333333-01".into(), later)
        .unwrap();
    assert_eq!((message.run_id, message.seq, message.attempt_no), (run_id, 0, 1));
    assert_eq!(
        MessageEnvelope::from_durable(message).unwrap().request_span_id.unwrap(),
        "3333333333333333"
    );
}

#[test]
fn payload_cannot_cross_tenant_or_workflow_boundaries() {
    let message = Fixture::message();
    let accepted = format!(
        "internal/v2/paper/{}/workflow/{}/feed/snapshot/request",
        message.scope.tenant_id,
        message.workflow_id.unwrap()
    );
    assert!(message.validate_topic(&accepted).is_ok());
    let foreign_tenant = accepted.replace(&message.scope.tenant_id.to_string(), &Uuid::now_v7().to_string());
    assert!(message.validate_topic(&foreign_tenant).is_err());
    let foreign_workflow = accepted.replace(&message.workflow_id.unwrap().to_string(), &Uuid::now_v7().to_string());
    assert!(message.validate_topic(&foreign_workflow).is_err());
}
