use chrono::Utc;
use common_context::IProvider;
use serde_json::json;
use sigbot_messager::local::local::LocalMessager;
use sigbot_messager::local::local::LocalMessagerConfig;
use sigbot_notifier::notifier::NotificationRequest;
use sigbot_notifier::notifier::NotificationTemplate;
use sigbot_notifier::notifier::NotifierService;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

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
    assert!(NotifierService::render(&template, &request).is_err());
    request.variables.insert("symbol".into(), json!("BTCUSDT"));
    assert_eq!(
        NotifierService::render(&template, &request).unwrap(),
        ("Execution".into(), "BTCUSDT executed".into())
    );
}

#[tokio::test]
#[ignore = "requires MARKETPULSE_DATABASE_URL with the real project migrations applied"]
async fn postgres_in_app_duplicate_scope_single_owner_and_crash_recovery() {
    let url = std::env::var("MARKETPULSE_DATABASE_URL").expect("integration database required");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let id = Uuid::now_v7();
    let run = Uuid::now_v7();
    let tenant_id = Uuid::now_v7();
    sqlx::query("INSERT INTO wf_workflow(id,name,status,env,tenant_id,flow_json) VALUES($1,'notification integration','RUNNING','test',$2,'{}')")
        .bind(id).bind(tenant_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,'test',$3,'running')")
        .bind(&run)
        .bind(id)
        .bind(tenant_id)
        .execute(&pool)
        .await
        .unwrap();
    let scope = MessageScope {
        env: "test".into(),
        tenant_id,
    };
    let properties=HashMap::from([("runtime_config".into(),json!({"scope":scope,"workflow_id":id,"node_id":8,"input_node_ids":[7], "templates":{"fill":{"version":"1","title":"Fill","body":"{{ symbol }}"}},"recipients":{"in_app":["operator"]},"providers":[]}).to_string())]);
    let messager = LocalMessager::new(Arc::new(LocalMessagerConfig { queue_size: 32 })).await;
    messager.init().await.unwrap();
    let runtime = NotifierService::from_properties(&properties).await.unwrap();
    runtime.start(messager.clone()).await.unwrap();
    let competitor = NotifierService::from_properties(&properties).await.unwrap();
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
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ntf_delivery WHERE run_id=$1 AND state='delivered'")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let mut foreign = wire;
    foreign.durable.scope.tenant_id = Uuid::now_v7();
    assert!(runtime.consume(&serde_json::to_vec(&foreign).unwrap()).await.is_err());
    let persisted: String = sqlx::query_scalar("SELECT payload::text FROM ntf_delivery WHERE run_id=$1")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!persisted.contains("requestSpanId"));
    sqlx::query("UPDATE ntf_delivery SET state='sending',attempt_no=1 WHERE run_id=$1")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    runtime.close().await;
    messager.close().await.unwrap();
    let recovered = NotifierService::from_properties(&properties).await.unwrap();
    let second = LocalMessager::new(Arc::new(LocalMessagerConfig { queue_size: 32 })).await;
    recovered.start(second.clone()).await.unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM ntf_delivery WHERE run_id=$1")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "unknown");
    recovered.close().await;
    second.close().await.unwrap();
    for table in ["ntf_delivery", "msg_inbox", "msg_outbox", "wf_sequence", "wf_run"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE run_id=$1"))
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM wf_workflow WHERE id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
}
