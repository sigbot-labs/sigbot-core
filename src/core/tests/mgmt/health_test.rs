//! Actual TCP and PostgreSQL health checks; no database or HTTP interception.
use common_telemetry::readiness::RuntimeReadiness;
use sigbot_core::config::config::AppConfig;
use sigbot_core::config::config::AppConfigProperties;
use sigbot_core::config::config::StorageProvider;
use sigbot_core::context::state::SigbotState;
use sigbot_core::mgmt::health::HealthRouter;
use std::time::Duration;

struct Fixture;
impl Fixture {
    fn configuration() -> AppConfigProperties {
        let address = std::env::var("MARKETPULSE_DATABASE_URL").expect("isolated integration database required");
        let url = url::Url::parse(&address).unwrap();
        let mut config = AppConfigProperties::default();
        config.storage.provider = StorageProvider::Postgres;
        let postgres = &mut config.storage.postgres.inner;
        postgres.host = url.host_str().unwrap().into();
        postgres.port = url.port().unwrap_or(5432);
        postgres.database = url.path().trim_start_matches('/').into();
        postgres.schema = "public".into();
        postgres.username = percent_encoding::percent_decode_str(url.username())
            .decode_utf8()
            .unwrap()
            .into_owned();
        postgres.password = url.password().map(|value| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .unwrap()
                .into_owned()
        });
        postgres.min_connections = Some(1);
        postgres.max_connections = Some(2);
        config
    }
}

#[tokio::test]
#[ignore = "requires MARKETPULSE_DATABASE_URL and actual project migrations"]
async fn real_http_requires_ready_business_and_reachable_database_and_detects_task_exit() {
    let config = AppConfig::new(&Fixture::configuration());
    let state = SigbotState::new(&config).await;
    let pool = state.postgres_pool().await.unwrap();
    let router = HealthRouter::routes().with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/_/healthz", listener.local_addr().unwrap());
    let live_endpoint = format!("http://{}/_/livez", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    RuntimeReadiness::business("fixture", false);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 503);
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 200);
    RuntimeReadiness::business("fixture", true);
    let task = RuntimeReadiness::task("consumer");
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 503);
    task.set(true);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 200);
    task.set(false);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 503);
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 200);
    task.set(true);
    drop(task);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 503);
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 503);
    let replacement = RuntimeReadiness::task("consumer");
    replacement.set(true);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 200);
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 200);
    RuntimeReadiness::task("finite").complete();
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 200);
    pool.close().await;
    let reply = client.get(&endpoint).send().await.unwrap();
    assert_eq!(reply.status().as_u16(), 503);
    let body: serde_json::Value = reply.json().await.unwrap();
    assert_eq!(body["details"]["database"], "DOWN");
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 200);
    drop(replacement);
    assert_eq!(client.get(&endpoint).send().await.unwrap().status().as_u16(), 503);
    assert_eq!(client.get(&live_endpoint).send().await.unwrap().status().as_u16(), 503);
    server.abort();
}
