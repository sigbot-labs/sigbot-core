use chrono::Utc;
use serde_json::json;
use sigbot_messager::client::{
    messager_factory::ISigbotMessagerClient,
    messager_local::{SigbotLocalMessagerClient, SigbotLocalMessagerClientConfig},
};
use sigbot_notification::server::notification_runtime::{
    NotificationRequest, NotificationRuntime, NotificationTemplate,
};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageEnvelope, MessageKind},
    topic::MessageScope,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[test]
fn templates_are_strict_and_versioned() {
    let template = NotificationTemplate {
        version: "v1".into(),
        body: "{{ symbol }} executed".into(),
        title: "Execution".into(),
    };
    let mut request = NotificationRequest {
        channel: "in_app".into(),
        recipient: "operator".into(),
        template: "fill".into(),
        variables: BTreeMap::new(),
    };
    assert!(NotificationRuntime::render(&template, &request).is_err());
    request.variables.insert("symbol".into(), json!("BTCUSDT"));
    assert_eq!(
        NotificationRuntime::render(&template, &request).unwrap(),
        ("Execution".into(), "BTCUSDT executed".into())
    );
}

#[tokio::test]
#[ignore = "requires MARKETPULSE_DATABASE_URL with the real project migrations applied"]
async fn postgres_in_app_duplicate_scope_single_owner_and_crash_recovery() {
    let url = std::env::var("MARKETPULSE_DATABASE_URL").expect("integration database required");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let id = Utc::now().timestamp_micros();
    let run = format!("notification-integration-{id}");
    sqlx::query("INSERT INTO s_workflow(id,name,status,env,tenant_id,space_id,flow_json) VALUES($1,'notification integration','RUNNING','simulated',711,812,'{}')").bind(id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state) VALUES($1,$2,'simulated',711,812,'running')").bind(&run).bind(id).execute(&pool).await.unwrap();
    let scope = MessageScope {
        env: "simulated".into(),
        tenant_id: 711,
        space_id: 812,
    };
    let properties=HashMap::from([("runtime_config".into(),json!({"scope":scope,"workflow_id":id,"node_id":8,"input_node_ids":[7],"database_url_env":"MARKETPULSE_DATABASE_URL", "templates":{"fill":{"version":"1","title":"Fill","body":"{{ symbol }}"}},"recipients":{"in_app":["operator"]},"providers":[]}).to_string())]);
    let messager = SigbotLocalMessagerClient::new(Arc::new(SigbotLocalMessagerClientConfig { queue_size: 32 })).await;
    messager.init().await;
    let runtime = NotificationRuntime::from_properties(&properties).await.unwrap();
    runtime.start(messager.clone()).await.unwrap();
    let competitor = NotificationRuntime::from_properties(&properties).await.unwrap();
    assert!(competitor.start(messager.clone()).await.is_err());
    let now = Utc::now();
    let source = DurableMessage {
        version: 2,
        scope: scope.clone(),
        workflow_id: Some(id),
        account_id: None,
        node_id: 7,
        target_node_id: Some(8),
        run_id: run.clone(),
        seq: 1,
        attempt_no: 0,
        kind: MessageKind::Request,
        event_time: now,
        available_time: now,
        produced_at: now,
        traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".into(),
        payload: NotificationRequest {
            channel: "in_app".into(),
            recipient: "operator".into(),
            template: "fill".into(),
            variables: BTreeMap::from([("symbol".into(), json!("BTCUSDT"))]),
        },
    };
    let wire = MessageEnvelope::from_durable(source).unwrap();
    let bytes = serde_json::to_vec(&wire).unwrap();
    runtime.consume(&bytes).await.unwrap();
    runtime.consume(&bytes).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM notification_delivery WHERE run_id=$1 AND state='delivered'")
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let mut foreign = wire;
    foreign.durable.scope.tenant_id = 712;
    assert!(runtime.consume(&serde_json::to_vec(&foreign).unwrap()).await.is_err());
    let persisted: String = sqlx::query_scalar("SELECT payload::text FROM notification_delivery WHERE run_id=$1")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!persisted.contains("requestSpanId"));
    sqlx::query("UPDATE notification_delivery SET state='sending',attempt_no=1 WHERE run_id=$1")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    runtime.close().await;
    messager.close().await;
    let recovered = NotificationRuntime::from_properties(&properties).await.unwrap();
    let second = SigbotLocalMessagerClient::new(Arc::new(SigbotLocalMessagerClientConfig { queue_size: 32 })).await;
    recovered.start(second.clone()).await.unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM notification_delivery WHERE run_id=$1")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "unknown");
    recovered.close().await;
    second.close().await;
    for table in [
        "notification_delivery",
        "messager_inbox",
        "messager_outbox",
        "workflow_sequence",
        "workflow_run",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE run_id=$1"))
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM s_workflow WHERE id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
}
