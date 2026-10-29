use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use sigbot_logservice::manager::archive::{LogArchiveConfig, LogArchiveFactory, LogLine};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageKind},
    topic::MessageScope,
};

struct Fixture;
impl Fixture {
    fn message(run: &str, seq: i64) -> DurableMessage<LogLine> {
        let now = Utc::now();
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "log-archive-test".into(),
                tenant_id: 900001,
                space_id: 1,
            },
            workflow_id: Some(1),
            account_id: None,
            node_id: 1,
            target_node_id: None,
            run_id: run.into(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            payload: LogLine {
                level: "INFO".into(),
                source: "rust".into(),
                content: format!("durable log {seq}"),
            },
        }
    }
}

#[tokio::test]
#[ignore = "requires actual PostgreSQL and MinIO configured via MARKETPULSE_* env"]
async fn durable_restart_late_cursor_dedup_and_scope() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var("MARKETPULSE_DATABASE_URL")?)
        .await?;
    let config = LogArchiveConfig {
        spool_path: directory.path().join("archive.sqlite"),
        max_buffer_bytes: 1_048_576,
        bucket: "sigbot-marketpulse-logs".into(),
        endpoint: "http://127.0.0.1:19000".into(),
        region: "us-east-1".into(),
        access_key_env: "MARKETPULSE_MINIO_ACCESS_KEY".into(),
        secret_key_env: "MARKETPULSE_MINIO_SECRET_KEY".into(),
        allow_http: true,
    };
    let run = format!("log-archive-{}", Utc::now().timestamp_micros());
    let first = Fixture::message(&run, 1);
    let archive = LogArchiveFactory::create(&config, pool.clone()).await?;
    assert!(archive.append(&first).await?);
    drop(archive); // Restart between durable ACK and any object upload.
    let archive = LogArchiveFactory::create(&config, pool.clone()).await?;
    let mut duplicate = first.clone();
    duplicate.attempt_no = 2;
    duplicate.produced_at = Utc::now();
    assert!(!archive.append(&duplicate).await?);
    assert_eq!(archive.archive_pending(4).await?, 1);
    assert_eq!(archive.archive_pending(4).await?, 0);
    let stream: DurableMessage<Value> = serde_json::from_value(serde_json::to_value(&first)?)?;
    let rows = archive.read(&stream, 0, 10).await?;
    assert_eq!(rows.len(), 1);
    let cursor = rows[0].0;
    assert_eq!(rows[0].1.payload.content, first.payload.content);
    assert!(archive.append(&Fixture::message(&run, 0)).await?);
    archive.archive_pending(4).await?;
    let late = archive.read(&stream, cursor, 10).await?;
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].1.seq, 0);
    let mut other = stream.clone();
    other.scope.tenant_id += 1;
    assert!(archive.read(&other, 0, 10).await?.is_empty());
    duplicate.payload.content = "identity collision".into();
    assert!(archive.append(&duplicate).await.is_err());
    sqlx::query("DELETE FROM log_chunk WHERE env='log-archive-test' AND run_id=$1")
        .bind(run)
        .execute(&pool)
        .await?;
    Ok(())
}
