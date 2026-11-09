//! Real Kafka delivery and PostgreSQL checkpoints; only input rows are synthetic.
use anyhow::Result;
use chrono::Utc;
use common_context::ApplicationContext;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;
use rdkafka::Message;
use serde_json::json;
use serde_json::Value;
use sigbot_exporter::exporter::ExportService;
use sigbot_exporter::exporter::ExportSinkConfig;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;

struct Fixture {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: Uuid,
    run_id: Uuid,
    properties: HashMap<String, String>,
    consumer: StreamConsumer,
}
impl Fixture {
    async fn new() -> Result<Self> {
        ApplicationContext::global().init().await?;
        let brokers = std::env::var("MARKETPULSE_KAFKA_BROKERS")?;
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let scope = MessageScope {
            env: format!("export-{}", Utc::now().timestamp_micros()),
            tenant_id: Uuid::now_v7(),
        };
        let workflow_id = Uuid::now_v7();
        let run_id = Uuid::now_v7();
        sqlx::query("INSERT INTO tenant_info(id,name) VALUES($1,'export owner tenant')")
            .bind(scope.tenant_id)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'export owner fixture')")
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
        let connection: Uuid = sqlx::query_scalar("INSERT INTO exporter_info(env,tenant_id,name,provider,transport,configuration,secret_ref) VALUES($1,$2,'export contract','KAFKA','kafka',$3,'local-isolated-broker') RETURNING id")
            .bind(&scope.env).bind(scope.tenant_id)
            .bind(json!({"kind":"exporter","endpoint":"kafka://isolated"}))
            .fetch_one(&pool)
            .await?;
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
            "sink":{"provider":"kafka","brokers":brokers,"topic":topic,"properties":{}}});
        let properties = HashMap::from([("runtime_config".into(), config.to_string())]);
        Ok(Self {
            pool,
            scope,
            workflow_id,
            run_id,
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
            run_id: self.run_id,
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
        assert_eq!(
            key,
            format!("{}/{}/{}/5/{seq}", self.scope.env, self.scope.tenant_id, self.run_id)
        );
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
    let runtime = ExportService::from_properties(&f.properties).await?;
    runtime.consume(&bytes).await?;
    f.received(10).await?;
    drop(runtime);
    let restarted = ExportService::from_properties(&f.properties).await?;
    restarted.consume(&bytes).await?;
    let mut invalid = first.clone();
    invalid.durable.workflow_id = None;
    assert!(restarted.consume(&serde_json::to_vec(&invalid)?).await.is_err());
    let older = f.wire(9)?;
    restarted.consume(&serde_json::to_vec(&older)?).await?;
    f.received(9).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(700), f.consumer.recv())
            .await
            .is_err(),
        "committed duplicate reached Kafka again"
    );
    let cursor: i64 = sqlx::query_scalar("SELECT seq FROM exp_checkpoint WHERE env=$1")
        .bind(&f.scope.env)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(cursor, 10, "late input cannot move the durable cursor backwards");
    let outputs: Vec<Value> = sqlx::query_scalar("SELECT payload FROM msg_outbox WHERE env=$1 ORDER BY seq")
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
        sqlx::query_scalar("SELECT count(*) FROM msg_inbox WHERE env=$1 AND consumer=$2 AND state='quarantined'")
            .bind(&f.scope.env)
            .bind(format!(
                "export:{}:{}:{}:9:routing",
                f.scope.env, f.scope.tenant_id, f.workflow_id
            ))
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(quarantined, 1, "misrouted QoS inputs persist before acknowledgement");
    let mut foreign = first;
    foreign.durable.scope.tenant_id = Uuid::now_v7();
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
    let runtime = ExportService::from_properties(&f.properties).await?;
    runtime.consume(&serde_json::to_vec(&first)?).await?;
    f.received(1).await?;
    let mut config: Value = serde_json::from_str(&f.properties["runtime_config"])?;
    let other_workflow = Uuid::now_v7();
    let other_run = Uuid::now_v7();
    sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'second export owner')")
        .bind(other_workflow)
        .bind(&f.scope.env)
        .bind(f.scope.tenant_id)
        .execute(&f.pool)
        .await?;
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(other_run)
        .bind(other_workflow)
        .bind(&f.scope.env)
        .bind(f.scope.tenant_id)
        .execute(&f.pool)
        .await?;
    config["workflow_id"] = json!(other_workflow);
    let properties = HashMap::from([("runtime_config".into(), config.to_string())]);
    let other = ExportService::from_properties(&properties).await?;
    assert!(other.consume(&serde_json::to_vec(&first)?).await.is_err());
    let mut second = first;
    second.durable.workflow_id = Some(other_workflow);
    second.durable.run_id = other_run;
    let bytes = serde_json::to_vec(&second)?;
    other.consume(&bytes).await?;
    let message = tokio::time::timeout(Duration::from_secs(30), f.consumer.recv()).await??;
    assert_eq!(
        std::str::from_utf8(message.key().unwrap())?,
        format!("{}/{}/{}/5/1", f.scope.env, f.scope.tenant_id, second.durable.run_id)
    );
    other.consume(&bytes).await?;
    assert!(tokio::time::timeout(Duration::from_millis(700), f.consumer.recv())
        .await
        .is_err());
    let owners: Vec<String> =
        sqlx::query_scalar("SELECT consumer FROM msg_inbox WHERE env=$1 AND state='done' ORDER BY consumer")
            .bind(&f.scope.env)
            .fetch_all(&f.pool)
            .await?;
    let mut expected = vec![
        format!("export:{}:{}:{}:9", f.scope.env, f.scope.tenant_id, f.workflow_id),
        format!("export:{}:{}:{}:9", f.scope.env, f.scope.tenant_id, other_workflow),
    ];
    expected.sort();
    assert_eq!(owners, expected);
    let producers: Vec<String> = sqlx::query_scalar("SELECT producer FROM msg_outbox WHERE env=$1 ORDER BY producer")
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
    assert!(plain.connect().is_err());
    let missing: ExportSinkConfig =
        serde_json::from_value(json!({"provider":"kafka","brokers":"127.0.0.1:1","topic":"fixture",
        "properties":{},"secret_keys":{"sasl.password":"missing_export_secret"}}))
        .unwrap();
    assert!(missing.connect().is_err());
}
