use anyhow::Result;
use chrono::Utc;
use common_telemetry::tracing;
use common_telemetry::tracing_subscriber;
use common_telemetry::tracing_subscriber::filter::LevelFilter;
use common_telemetry::tracing_subscriber::filter::Targets;
use common_telemetry::tracing_subscriber::layer::SubscriberExt;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::refresh_config;
use sigbot_core::mgmt::apm::logging::LogRoutes;
use sigbot_core::modules::logservice::handler::logservice::LogHandler;
use sigbot_logservice::s3::capture::ArchiveCapture;
use sigbot_logservice::s3::capture::LogCaptureOptions;
use sigbot_logservice::s3::capture::LogRedactor;
use sigbot_logservice::s3::capture::LogStreamIdentity;
use sigbot_logservice::s3::s3::LogAppend;
use sigbot_logservice::s3::s3::LogArchiveConfig;
use sigbot_logservice::s3::s3::LogLine;
use sigbot_logservice::s3::s3::S3Logservice;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

struct Fixture;
impl Fixture {
    fn config(path: &Path, real: bool) -> LogArchiveConfig {
        if !real {
            std::env::set_var("SIGBOT__SECRETS__VALUES__LOG_TEST_ACCESS_KEY", "unused-local-test-key");
            std::env::set_var(
                "SIGBOT__SECRETS__VALUES__LOG_TEST_SECRET_KEY",
                "unused-local-test-secret",
            );
            refresh_config().expect("test configuration must reload");
        }
        LogArchiveConfig {
            spool_path: path.join("capture.sqlite"),
            max_buffer_bytes: 1_048_576,
            bucket: "sigbot-marketpulse-logs".into(),
            endpoint: if real {
                "http://127.0.0.1:19000"
            } else {
                "http://127.0.0.1:1"
            }
            .into(),
            region: "us-east-1".into(),
            allow_http: true,
            access_key_secret: if real {
                "minio_access_key"
            } else {
                "log_test_access_key"
            }
            .into(),
            secret_key_secret: if real {
                "minio_secret_key"
            } else {
                "log_test_secret_key"
            }
            .into(),
        }
    }
    fn offline_pool() -> Result<sqlx::PgPool> {
        Ok(sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unavailable")?)
    }
    async fn handler(config: &LogArchiveConfig, pool: sqlx::PgPool) -> Result<LogHandler> {
        LogHandler::from_pool(pool, &config.spool_path, config.max_buffer_bytes).await
    }
    fn identity(run: Uuid) -> LogStreamIdentity {
        LogStreamIdentity {
            scope: MessageScope {
                env: "rust-tracing-test".into(),
                tenant_id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000611").unwrap(),
            },
            workflow_id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000612").unwrap(),
            node_id: 5,
            run_id: run,
        }
    }
    fn message(run: Uuid, seq: i64, content: &str) -> DurableMessage<LogLine> {
        let now = Utc::now();
        let identity = Self::identity(run);
        DurableMessage {
            version: 2,
            scope: identity.scope,
            workflow_id: Some(identity.workflow_id),
            account_id: None,
            node_id: 5,
            target_node_id: None,
            run_id: run,
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            payload: LogLine {
                level: "INFO".into(),
                source: "contract".into(),
                content: content.into(),
            },
        }
    }
    fn emit() {
        let span = tracing::info_span!(target:"sigbot_strategy_runner::contract","input",
            traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let _entered = span.enter();
        tracing::info!(target:"sigbot_strategy_runner::contract",password="secret-sentinel",
            requestSpanId="ephemeral-sentinel",payload=%json!({"keep":"visible","request_span_id":"nested-pairing",
            "authorization":"Bearer nested-secret"}),
            debug_payload=?json!({"nested":{"password":"debug-password-secret","token":"debug-token-secret"}}),
            "actual Rust tracing event");
        tracing::warn!(target:"sigbot_archive_internal","this internal diagnostic must not recurse");
        tracing::debug!(target:"sigbot_strategy_runner::contract","debug is outside capture policy");
    }
}

#[test]
fn pairing_fields_and_credentials_are_removed_before_persistence() {
    let redacted = LogRedactor::text(
        r#"{"keep":1,"requestSpanId":"temporary","child":{"request_span_id":"other","api_key":"credential"}}"#,
    );
    assert!(!redacted.contains("requestSpanId") && !redacted.contains("request_span_id"));
    assert!(!redacted.contains("temporary") && !redacted.contains("credential"));
    assert!(redacted.contains("keep"));
    assert!(!LogRedactor::text("Object { requestSpanId: String(\"temporary\") }").contains("temporary"));
    let uri = LogRedactor::text("failed https://user:secret@example.invalid/feed?token=private Bearer aaa.bbb.ccc");
    for sensitive in ["user:secret", "private", "aaa.bbb.ccc"] {
        assert!(!uri.contains(sensitive));
    }
    let debug = format!(
        "{:?}",
        json!({"nested":{"password":"debug-password-secret","token":"debug-token-secret"}})
    );
    assert_eq!(LogRedactor::text(&debug), "[REDACTED SENSITIVE MESSAGE]");
    assert_eq!(
        LogRedactor::text("normal strategy decision"),
        "normal strategy decision"
    );
}

#[tokio::test]
async fn rolled_back_tentative_sequence_never_overwrites_another_writer() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = Fixture::config(directory.path(), false);
    let archive = S3Logservice::new(&config, Fixture::handler(&config, Fixture::offline_pool()?).await?).await?;
    let run = Uuid::now_v7();
    // This state represents an unacknowledged transaction that rolled back after
    // selecting seq=0; another writer committed that seq before it retried.
    let original = Fixture::message(run, 0, "first writer");
    let mut tentative = LogAppend::from_tentative(original.clone());
    let mut other = LogAppend::new(Fixture::message(run, 0, "other writer"));
    assert!(archive.append_generated(&mut other).await?);
    assert_eq!(other.message().seq, 0);
    assert!(archive.append_generated(&mut tentative).await?);
    assert_eq!(tentative.message().seq, 1);
    assert!(!archive.append_generated(&mut tentative).await?);
    assert!(
        archive.append(&original).await.is_err(),
        "explicit append keeps fixed-identity semantics"
    );
    drop(archive);
    let archive = S3Logservice::new(&config, Fixture::handler(&config, Fixture::offline_pool()?).await?).await?;
    let mut next = LogAppend::new(Fixture::message(run, 0, "after restart"));
    archive.append_generated(&mut next).await?;
    assert_eq!(next.message().seq, 2);
    let spool = sqlx::SqlitePool::connect(&format!("sqlite://{}", config.spool_path.display())).await?;
    let payloads: Vec<Vec<u8>> = sqlx::query_scalar("SELECT payload FROM log_buffer ORDER BY rowid")
        .fetch_all(&spool)
        .await?;
    let contents: Vec<String> = payloads
        .iter()
        .map(|bytes| {
            serde_json::from_slice::<DurableMessage<LogLine>>(bytes)
                .unwrap()
                .payload
                .content
        })
        .collect();
    assert_eq!(contents, vec!["other writer", "first writer", "after restart"]);
    Ok(())
}

#[tokio::test]
async fn full_capture_queue_fails_barrier_but_keeps_accepted_event_in_real_wal() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = Fixture::config(directory.path(), false);
    let archive =
        Arc::new(S3Logservice::new(&config, Fixture::handler(&config, Fixture::offline_pool()?).await?).await?);
    let (layer, capture) = ArchiveCapture::start(
        archive,
        Fixture::identity(Uuid::now_v7()),
        LogCaptureOptions {
            queue_capacity: 1,
            flush_timeout: Duration::from_secs(3),
        },
    )?;
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    // Single-thread test emits synchronously before the writer task can run.
    tracing::dispatcher::with_default(&dispatch, || {
        for _ in 0..3 {
            tracing::info!(target:"sigbot_strategy_runner::contract","bounded actual event");
        }
    });
    assert!(capture.flush_durable().await.is_err());
    let stats = capture.stats();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.rejected, 2);
    assert_eq!(stats.durable, 1);
    assert!(capture.shutdown().await.is_err());
    let spool = sqlx::SqlitePool::connect(&format!("sqlite://{}", config.spool_path.display())).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM log_buffer")
            .fetch_one(&spool)
            .await?,
        1
    );
    Ok(())
}

struct PausedDisplay {
    started: std::sync::mpsc::Sender<()>,
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}
impl std::fmt::Display for PausedDisplay {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = self.started.send(());
        let (lock, ready) = &*self.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        formatter.write_str("late formatted event")
    }
}

#[tokio::test]
async fn shutdown_barrier_cannot_be_overtaken_by_event_admission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let archive = Arc::new(
        S3Logservice::new(
            &Fixture::config(directory.path(), false),
            Fixture::handler(&Fixture::config(directory.path(), false), Fixture::offline_pool()?).await?,
        )
        .await?,
    );
    let (layer, capture) =
        ArchiveCapture::start(archive, Fixture::identity(Uuid::now_v7()), LogCaptureOptions::default())?;
    let dispatch = Arc::new(tracing::Dispatch::new(tracing_subscriber::registry().with(layer)));
    tracing::dispatcher::with_default(&dispatch, Fixture::emit);
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let (started, notification) = std::sync::mpsc::channel();
    let paused_message = PausedDisplay {
        started,
        gate: gate.clone(),
    };
    let thread_dispatch = dispatch.clone();
    let emitter = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&thread_dispatch, || {
            tracing::info!(target:"sigbot_strategy_runner::contract",message=%paused_message);
        })
    });
    notification.recv_timeout(Duration::from_secs(2))?;
    // The second event passed the fast closed check but is still formatting.
    let shutdown = capture.shutdown().await;
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    emitter.join().unwrap();
    shutdown?;
    let stats = capture.stats();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.durable, 1);
    assert_eq!(stats.rejected, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires actual MinIO and migrated PostgreSQL; emits real tracing events, first with both remote endpoints unavailable"]
async fn actual_tracing_offline_wal_restart_minio_index_and_cursor() -> Result<()> {
    // Exercise the exact production subscriber/reload registration, rather than
    // constructing the archive layer directly on a fresh Registry.
    std::env::set_var("SIGBOT__MGMT__ENABLED", "false");
    std::env::set_var("SIGBOT__LOGGING__LEVEL", "Off");
    sigbot_core::mgmt::apm::init().await;
    let directory = tempfile::tempdir()?;
    let offline = Fixture::config(directory.path(), false);
    let run = Uuid::now_v7();
    let archive =
        Arc::new(S3Logservice::new(&offline, Fixture::handler(&offline, Fixture::offline_pool()?).await?).await?);
    let (layer, capture) =
        ArchiveCapture::start(archive.clone(), Fixture::identity(run), LogCaptureOptions::default())?;
    let route = LogRoutes::install(
        Box::new(layer),
        Targets::new().with_target("sigbot_strategy_runner", LevelFilter::INFO),
    )?;
    Fixture::emit();
    tracing::info!(target:"sigbot_strategy_sdk::contract", "excluded by the Core route target filter");
    capture.flush_durable().await?;
    assert_eq!(
        capture.stats().durable,
        1,
        "internal and debug events must not enter capture"
    );
    capture.shutdown().await?;
    let spool = sqlx::SqlitePool::connect(&format!("sqlite://{}", offline.spool_path.display())).await?;
    let persisted: Vec<u8> = sqlx::query_scalar("SELECT payload FROM log_buffer LIMIT 1")
        .fetch_one(&spool)
        .await?;
    let persisted = std::str::from_utf8(&persisted)?;
    for forbidden in [
        "secret-sentinel",
        "ephemeral-sentinel",
        "nested-pairing",
        "nested-secret",
        "debug-password-secret",
        "debug-token-secret",
        "requestSpanId",
        "request_span_id",
    ] {
        assert!(
            !persisted.contains(forbidden),
            "sensitive or ephemeral field persisted in WAL"
        );
    }
    spool.close().await;
    drop(route);
    tracing::info!(target:"sigbot_strategy_runner::contract", "after route removal");
    assert_eq!(capture.stats().accepted, 1, "uninstalled route must not receive events");
    drop(capture);
    drop(archive);

    let pool = sqlx::PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
    let identity = Fixture::identity(run);
    sqlx::query(
        "INSERT INTO wf_workflow(id,env,tenant_id,name) VALUES($1,$2,$3,'log archive tracing')
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(identity.workflow_id)
    .bind(&identity.scope.env)
    .bind(identity.scope.tenant_id)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(run)
        .bind(identity.workflow_id)
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .execute(&pool)
        .await?;
    let online = Fixture::config(directory.path(), true);
    let archive = Arc::new(S3Logservice::new(&online, Fixture::handler(&online, pool.clone()).await?).await?);
    assert_eq!(archive.archive_pending(8).await?, 1);
    let stream: DurableMessage<Value> =
        serde_json::from_value(serde_json::to_value(Fixture::message(run, 0, "read"))?)?;
    let rows = archive.read(&stream, 0, 10).await?;
    assert_eq!(rows.len(), 1);
    let cursor = rows[0].0;
    let first = &rows[0].1;
    assert_eq!(first.seq, 0);
    assert!(first.payload.source.starts_with("rust:"));
    assert!(first.traceparent.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
    for forbidden in [
        "secret-sentinel",
        "ephemeral-sentinel",
        "nested-pairing",
        "nested-secret",
        "debug-password-secret",
        "debug-token-secret",
        "requestSpanId",
        "request_span_id",
    ] {
        assert!(
            !first.payload.content.contains(forbidden),
            "sensitive or ephemeral field persisted"
        );
    }
    assert!(first.payload.content.contains("visible"));
    assert!(!archive.append(first).await?);
    let (layer, capture) =
        ArchiveCapture::start(archive.clone(), Fixture::identity(run), LogCaptureOptions::default())?;
    let route = LogRoutes::install(
        Box::new(layer),
        Targets::new().with_target("sigbot_strategy_runner", LevelFilter::INFO),
    )?;
    tracing::warn!(target:"sigbot_strategy_runner::contract","after restart background event");
    capture.flush_durable().await?;
    capture.shutdown().await?;
    drop(route);
    assert_eq!(archive.archive_pending(8).await?, 1);
    let later = archive.read(&stream, cursor, 10).await?;
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].1.seq, 1);
    assert!(later[0].1.payload.content.contains("\"background\":true"));
    let mut other = stream;
    other.scope.tenant_id = Uuid::now_v7();
    assert!(archive.read(&other, 0, 10).await?.is_empty());
    assert_eq!(archive.archive_pending(8).await?, 0);
    sqlx::query("DELETE FROM log_chunk WHERE env='rust-tracing-test' AND run_id=$1")
        .bind(run)
        .execute(&pool)
        .await?;
    Ok(())
}
