use chrono::Utc;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use serde_json::json;
use serde_json::Value;
use sigbot_core::modules::messager::handler::messager::MessagerHandler;
use sigbot_messager::messager::IMessager;
use sigbot_messager::mqtt::mqtt::MqttMessager;
use sigbot_messager::mqtt::mqtt::MqttMessagerConfig;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::envelope::TraceContext;
use sigbot_types::messager::messager::MessagerConfig;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and authenticated EMQX; run through the marketpulse case"]
async fn real_broker_replay_preserves_identity_and_quarantine_ack_is_durable() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&std::env::var("SIGBOT_TEST_DATABASE_URL")?).await?;
    let scope: MessageScope = serde_json::from_str(&std::env::var("SIGBOT_TEST_MESSAGE_SCOPE")?)?;
    let configuration: MessagerConfig = serde_json::from_str(&std::env::var("SIGBOT_TEST_MQTT_CONFIG")?)?;
    let client = MqttMessager::new(Arc::new(MqttMessagerConfig::from_config(Arc::new(configuration))?)).await;
    client.init().await.unwrap();
    let workflow_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'messager replay contract')")
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(run_id)
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&pool)
        .await?;
    let topic = WorkflowTopic::RuntimeEvent.topic(&scope, workflow_id)?;
    let request_topic = WorkflowTopic::FeedSnapshotRequest.topic(&scope, workflow_id)?;
    let (sender, mut deliveries) = tokio::sync::mpsc::channel(8);
    for subscribed_topic in [&topic, &request_topic] {
        let sender = sender.clone();
        client
            .subscribe(
                subscribed_topic,
                Arc::new(move |bytes| {
                    let sender = sender.clone();
                    Box::pin(async move {
                        sender
                            .send(serde_json::from_slice::<MessageEnvelope<Value>>(&bytes)?)
                            .await?;
                        Ok("observed".into())
                    })
                }),
            )
            .await?;
    }
    let now = Utc::now();
    let producer = format!("replay-test:{}:{}:{}", scope.env, scope.tenant_id, std::process::id());
    let input = DurableMessage {
        version: 2,
        scope: scope.clone(),
        workflow_id: Some(workflow_id),
        account_id: None,
        node_id: 9001,
        target_node_id: None,
        run_id,
        seq: 1,
        attempt_no: 0,
        kind: MessageKind::Event,
        event_time: now,
        available_time: now,
        produced_at: now,
        traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
        payload: json!({"fact":"replay-test"}),
    };
    let store = MessagerHandler::new(pool.clone());
    assert!(store.enqueue(&producer, &topic, &input).await?);
    let trace = |parent: &str| Ok(MessageTrace::child("replay.test", parent).traceparent());
    assert_eq!(
        sigbot_messager::messager::OutboxRelay::dispatch_pending(&store, &producer, client.as_ref(), 1, &trace).await?,
        1
    );
    let first = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await?
        .unwrap();
    let mut wrong_scope = scope.clone();
    wrong_scope.tenant_id = Uuid::now_v7();
    assert_eq!(
        store
            .replay(&producer, &wrong_scope, input.node_id, &input.run_id, 1..=1)
            .await?,
        0
    );
    assert_eq!(
        store
            .replay("another-producer", &scope, input.node_id, &input.run_id, 1..=1)
            .await?,
        0
    );
    assert_eq!(
        store
            .replay(&producer, &scope, input.node_id + 1, &input.run_id, 1..=1)
            .await?,
        0
    );
    assert_eq!(
        store
            .replay(&producer, &scope, input.node_id, &Uuid::now_v7(), 1..=1)
            .await?,
        0
    );
    assert_eq!(
        store
            .replay(&producer, &scope, input.node_id, &input.run_id, 2..=2)
            .await?,
        0
    );
    assert_eq!(
        store
            .replay(&producer, &scope, input.node_id, &input.run_id, 1..=1)
            .await?,
        1
    );
    assert_eq!(
        sigbot_messager::messager::OutboxRelay::dispatch_pending(&store, &producer, client.as_ref(), 1, &trace).await?,
        1
    );
    let second = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await?
        .unwrap();
    assert_eq!(first.durable.run_id, second.durable.run_id);
    assert_eq!(first.durable.seq, second.durable.seq);
    assert_eq!(first.durable.node_id, second.durable.node_id);
    assert_eq!(second.durable.attempt_no, first.durable.attempt_no + 1);
    assert_ne!(
        TraceContext::parse(&first.durable.traceparent)?.span_id,
        TraceContext::parse(&second.durable.traceparent)?.span_id
    );
    assert!(store.accept(&producer, &topic, &first).await?);
    sqlx::query("UPDATE msg_inbox SET state='quarantined' WHERE consumer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    assert!(!store.accept(&producer, &topic, &second).await?);
    let mut conflict = second.clone();
    conflict.durable.payload = json!({"fact":"changed"});
    assert!(store.accept(&producer, &topic, &conflict).await.is_err());
    let payload: Value = sqlx::query_scalar("SELECT payload FROM msg_outbox WHERE producer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .fetch_one(&pool)
        .await?;
    assert!(payload.get("requestSpanId").is_none());
    let request = DurableMessage {
        seq: 2,
        target_node_id: Some(9002),
        kind: MessageKind::Request,
        payload: json!({"query":"latest"}),
        ..input.clone()
    };
    assert!(store.enqueue(&producer, &request_topic, &request).await?);
    assert_eq!(
        sigbot_messager::messager::OutboxRelay::dispatch_pending(&store, &producer, client.as_ref(), 1, &trace).await?,
        1
    );
    let delivered_request = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await?
        .unwrap();
    assert_eq!(delivered_request.durable.kind, MessageKind::Request);
    assert_eq!(
        delivered_request.request_span_id.as_deref(),
        Some(TraceContext::parse(&request.traceparent)?.span_id)
    );
    client.close().await.unwrap();
    // Remove only the records owned by this test run; no production identity is touched.
    sqlx::query("DELETE FROM msg_inbox WHERE consumer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM msg_outbox WHERE producer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM wf_run WHERE run_id=$1")
        .bind(run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM workflow_info WHERE id=$1")
        .bind(workflow_id)
        .execute(&pool)
        .await?;
    Ok(())
}
