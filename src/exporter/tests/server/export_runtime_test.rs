//! Real Kafka delivery and PostgreSQL checkpoints; only input rows are synthetic.
use anyhow::Result;
use chrono::Utc;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use serde_json::{json, Value};
use sigbot_exporter::{
    manager::sink::{ExportSinkConfig, ExportSinkFactory},
    server::export_runtime::ExportRuntime,
};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageEnvelope, MessageKind},
    topic::MessageScope,
};
use sqlx::PgPool;
use std::{collections::HashMap, time::Duration};

struct Fixture {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: i64,
    properties: HashMap<String, String>,
    consumer: StreamConsumer,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let brokers = std::env::var("MARKETPULSE_KAFKA_BROKERS")?;
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let scope = MessageScope {
            env: format!("export-{}", Utc::now().timestamp_micros()),
            tenant_id: 1,
            space_id: 1,
        };
        let workflow_id: i64 = sqlx::query_scalar(
            "INSERT INTO s_workflow(env,tenant_id,space_id,name) VALUES($1,1,1,'export owner fixture') RETURNING id",
        )
        .bind(&scope.env)
        .fetch_one(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state) VALUES($1,$2,$1,1,1,'running')",
        )
        .bind(&scope.env)
        .bind(workflow_id)
        .execute(&pool)
        .await?;
        let connection:i64 = sqlx::query_scalar("INSERT INTO connection(env,tenant_id,space_id,name,provider,transport,secret_ref) VALUES($1,1,1,'export contract','KAFKA','kafka','local-isolated-broker') RETURNING id")
            .bind(&scope.env).fetch_one(&pool).await?;
        let topic = scope.env.clone();
        let admin: AdminClient<DefaultClientContext> =
            ClientConfig::new().set("bootstrap.servers", &brokers).create()?;
        let created = admin
            .create_topics(
                &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(15))),
            )
            .await?;
        assert!(
            created.iter().all(Result::is_ok),
            "isolated Kafka topic creation failed"
        );
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &scope.env)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()?;
        consumer.subscribe(&[&topic])?;
        let config = json!({"scope":scope,"workflow_id":workflow_id,"node_id":9,"input_node_ids":[5],"connection_id":connection,
            "database_url_env":"MARKETPULSE_DATABASE_URL","sink":{"provider":"kafka","brokers":brokers,"topic":topic,"properties":{}}});
        let properties = HashMap::from([("runtime_config".into(), config.to_string())]);
        Ok(Self {
            pool,
            scope,
            workflow_id,
            properties,
            consumer,
        })
    }
    fn wire(&self, seq: i64) -> Result<MessageEnvelope<Value>> {
        let now = Utc::now();
        MessageEnvelope::from_durable(DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: Some(self.workflow_id),
            account_id: None,
            node_id: 5,
            target_node_id: Some(9),
            run_id: self.scope.env.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Request,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: json!({"key":"untrusted-client-key","range":"","rows":[["BTCUSDT",seq]]}),
        })
    }
    async fn received(&self, seq: i64) -> Result<()> {
        let message = tokio::time::timeout(Duration::from_secs(30), self.consumer.recv()).await??;
        let key = std::str::from_utf8(message.key().unwrap())?;
        assert_eq!(key, format!("{}/1/1/{}/5/{seq}", self.scope.env, self.scope.env));
        let body: Value = serde_json::from_slice(message.payload().unwrap())?;
        assert_eq!(body["key"], key);
        assert_eq!(body["rows"], json!([["BTCUSDT", seq]]));
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and actual isolated Kafka; no mock producer"]
async fn real_kafka_ack_restart_dedup_out_of_order_cursor_and_scope() -> Result<()> {
    let f = Fixture::new().await?;
    let first = f.wire(10)?;
    let bytes = serde_json::to_vec(&first)?;
    let runtime = ExportRuntime::from_properties(&f.properties).await?;
    runtime.consume(&bytes).await?;
    f.received(10).await?;
    drop(runtime);
    let restarted = ExportRuntime::from_properties(&f.properties).await?;
    restarted.consume(&bytes).await?;
    // Broker inflight messages may retain a pre-migration missing Workflow header.
    // Resolve the immutable run binding before comparing with the completed Inbox.
    let mut legacy = first.clone();
    legacy.durable.workflow_id = None;
    restarted.consume(&serde_json::to_vec(&legacy)?).await?;
    let older = f.wire(9)?;
    restarted.consume(&serde_json::to_vec(&older)?).await?;
    f.received(9).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(700), f.consumer.recv())
            .await
            .is_err(),
        "committed duplicate reached Kafka again"
    );
    let cursor: i64 = sqlx::query_scalar("SELECT seq FROM export_checkpoint WHERE env=$1")
        .bind(&f.scope.env)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(cursor, 10, "late input cannot move the durable cursor backwards");
    let outputs: Vec<Value> = sqlx::query_scalar("SELECT payload FROM messager_outbox WHERE env=$1 ORDER BY seq")
        .bind(&f.scope.env)
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(outputs.len(), 2);
    assert!(outputs
        .iter()
        .all(|p| p["kind"] == "result" && p.get("requestSpanId").is_none()));
    let mut collision = first.clone();
    collision.durable.payload["rows"] = json!([["BTCUSDT", 999]]);
    assert!(restarted.consume(&serde_json::to_vec(&collision)?).await.is_err());
    let mut wrong_route = first.clone();
    wrong_route.durable.target_node_id = Some(10);
    wrong_route.durable.seq = 11;
    restarted.consume(&serde_json::to_vec(&wrong_route)?).await?;
    restarted.consume(&serde_json::to_vec(&wrong_route)?).await?;
    let quarantined: i64 =
        sqlx::query_scalar("SELECT count(*) FROM messager_inbox WHERE env=$1 AND consumer=$2 AND state='quarantined'")
            .bind(&f.scope.env)
            .bind(format!("export:{}:1:1:{}:9:routing", f.scope.env, f.workflow_id))
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(quarantined, 1, "misrouted QoS inputs persist before acknowledgement");
    let mut foreign = first;
    foreign.durable.scope.space_id = 2;
    assert!(restarted.consume(&serde_json::to_vec(&foreign)?).await.is_err());
    assert!(tokio::time::timeout(Duration::from_millis(700), f.consumer.recv())
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and actual isolated Kafka; same node in two Workflows"]
async fn same_node_two_workflows_have_independent_inbox_and_outbox_owners() -> Result<()> {
    let f = Fixture::new().await?;
    let first = f.wire(1)?;
    let runtime = ExportRuntime::from_properties(&f.properties).await?;
    runtime.consume(&serde_json::to_vec(&first)?).await?;
    f.received(1).await?;
    let mut config: Value = serde_json::from_str(&f.properties["runtime_config"])?;
    config["workflow_id"] = json!(f.workflow_id + 1);
    let properties = HashMap::from([("runtime_config".into(), config.to_string())]);
    let other = ExportRuntime::from_properties(&properties).await?;
    assert!(other.consume(&serde_json::to_vec(&first)?).await.is_err());
    let mut second = first;
    second.durable.workflow_id = Some(f.workflow_id + 1);
    second.durable.run_id.push_str("-other");
    let bytes = serde_json::to_vec(&second)?;
    other.consume(&bytes).await?;
    let message = tokio::time::timeout(Duration::from_secs(30), f.consumer.recv()).await??;
    assert_eq!(
        std::str::from_utf8(message.key().unwrap())?,
        format!("{}/1/1/{}/5/1", f.scope.env, second.durable.run_id)
    );
    other.consume(&bytes).await?;
    assert!(tokio::time::timeout(Duration::from_millis(700), f.consumer.recv())
        .await
        .is_err());
    let owners: Vec<String> =
        sqlx::query_scalar("SELECT consumer FROM messager_inbox WHERE env=$1 AND state='done' ORDER BY consumer")
            .bind(&f.scope.env)
            .fetch_all(&f.pool)
            .await?;
    let mut expected = vec![
        format!("export:{}:1:1:{}:9", f.scope.env, f.workflow_id),
        format!("export:{}:1:1:{}:9", f.scope.env, f.workflow_id + 1),
    ];
    expected.sort();
    assert_eq!(owners, expected);
    let producers: Vec<String> =
        sqlx::query_scalar("SELECT producer FROM messager_outbox WHERE env=$1 ORDER BY producer")
            .bind(&f.scope.env)
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(owners, producers);
    Ok(())
}

#[test]
fn kafka_credentials_must_be_host_environment_references() {
    let plain: ExportSinkConfig =
        serde_json::from_value(json!({"provider":"kafka","brokers":"127.0.0.1:1","topic":"fixture",
        "properties":{"sasl.password":"test-only-plaintext"}}))
        .unwrap();
    assert!(ExportSinkFactory::create(&plain).is_err());
    let missing: ExportSinkConfig =
        serde_json::from_value(json!({"provider":"kafka","brokers":"127.0.0.1:1","topic":"fixture",
        "properties":{},"secret_env":{"sasl.password":"SIGBOT_TEST_MISSING_EXPORT_SECRET"}}))
        .unwrap();
    assert!(ExportSinkFactory::create(&missing).is_err());
}
