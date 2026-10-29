use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use serde_json::{json, Value};
use sigbot_messager::{
    client::{
        messager_factory::ISigbotMessagerClient,
        messager_mqtt::{SigbotMqttMessagerClient, SigbotMqttMessagerClientConfig},
    },
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageEnvelope, MessageKind, TraceContext},
    messager::MessagerConfiguration,
    topic::{MessageScope, WorkflowTopic},
};
use std::{sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and authenticated EMQX; run through the marketpulse case"]
async fn real_broker_replay_preserves_identity_and_quarantine_ack_is_durable() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&std::env::var("SIGBOT_TEST_DATABASE_URL")?).await?;
    let scope: MessageScope = serde_json::from_str(&std::env::var("SIGBOT_TEST_MESSAGE_SCOPE")?)?;
    let configuration: MessagerConfiguration = serde_json::from_str(&std::env::var("SIGBOT_TEST_MQTT_CONFIG")?)?;
    let client = SigbotMqttMessagerClient::new(Arc::new(SigbotMqttMessagerClientConfig::from_config(Arc::new(
        configuration,
    ))))
    .await;
    client.init().await;
    let topic = WorkflowTopic::RuntimeEvent.topic(&scope, 9001)?;
    let (sender, mut deliveries) = tokio::sync::mpsc::channel(8);
    client
        .subscribe(
            &topic,
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
    let now = Utc::now();
    let producer = format!(
        "replay-test:{}:{}:{}:{}",
        scope.env,
        scope.tenant_id,
        scope.space_id,
        std::process::id()
    );
    let input = DurableMessage {
        version: 2,
        scope: scope.clone(),
        workflow_id: Some(9001),
        account_id: None,
        node_id: 9001,
        target_node_id: None,
        run_id: format!("replay-{}-{}", std::process::id(), now.timestamp_nanos_opt().unwrap()),
        seq: 1,
        attempt_no: 0,
        kind: MessageKind::Event,
        event_time: now,
        available_time: now,
        produced_at: now,
        traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
        payload: json!({"fact":"replay-test"}),
    };
    let store = PostgresMessageStore::new(pool.clone());
    let mut tx = pool.begin().await?;
    assert!(PostgresMessageStore::enqueue(&mut tx, &producer, &topic, &input).await?);
    tx.commit().await?;
    let trace = |parent: &str| Ok(MessageTrace::child("replay.test", parent).traceparent());
    assert_eq!(store.dispatch_pending(&producer, client.as_ref(), 1, &trace).await?, 1);
    let first = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await?
        .unwrap();
    let mut wrong_scope = scope.clone();
    wrong_scope.tenant_id += 1;
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
            .replay(&producer, &scope, input.node_id, "another-run", 1..=1)
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
    assert_eq!(store.dispatch_pending(&producer, client.as_ref(), 1, &trace).await?, 1);
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
    let mut tx = pool.begin().await?;
    assert!(PostgresMessageStore::accept(&mut tx, &producer, &topic, &first).await?);
    sqlx::query("UPDATE messager_inbox SET state='quarantined' WHERE consumer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let mut tx = pool.begin().await?;
    assert!(!PostgresMessageStore::accept(&mut tx, &producer, &topic, &second).await?);
    tx.commit().await?;
    let mut conflict = second.clone();
    conflict.durable.payload = json!({"fact":"changed"});
    let mut tx = pool.begin().await?;
    assert!(PostgresMessageStore::accept(&mut tx, &producer, &topic, &conflict)
        .await
        .is_err());
    tx.rollback().await?;
    let payload: Value = sqlx::query_scalar("SELECT payload FROM messager_outbox WHERE producer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .fetch_one(&pool)
        .await?;
    assert!(payload.get("requestSpanId").is_none());
    client.close().await;
    // Remove only the records owned by this test run; no production identity is touched.
    sqlx::query("DELETE FROM messager_inbox WHERE consumer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM messager_outbox WHERE producer=$1 AND run_id=$2")
        .bind(&producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    Ok(())
}
