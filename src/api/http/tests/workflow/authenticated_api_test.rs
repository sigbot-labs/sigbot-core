//! Real TCP + PostgreSQL boundary test. The fixture is a trusted context issuer,
//! not a Hosted Login replacement and not evidence of browser session deployment.
use authguard_adapter_rust::sign_access_context;
use authguard_adapter_rust::AccessContext;
use authguard_adapter_rust::AccessContextInput;
use authguard_adapter_rust::HeaderAccessContextResolver;
use authguard_adapter_rust::ACCESS_CONTEXT_HEADER;
use axum::middleware;
use axum::Router;
use reqwest::Client;
use reqwest::Method;
use reqwest::Response;
use reqwest::StatusCode;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::AppConfig;
use sigbot_core::config::config::AppConfigProperties;
use sigbot_core::config::config::StorageProvider;
use sigbot_core::context::authguard::AuthguardProvider;
use sigbot_core::context::middleware::IdentityRouter;
use sigbot_core::context::state::SigbotState;
use sigbot_http::workflow::WorkflowHttpApi;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

struct Fixture {
    endpoint: String,
    client: Client,
    tenant: Uuid,
}
impl Fixture {
    const KEY: &'static [u8] = b"test-only-official-authguard-issuer-key";
    fn header(&self, method: &Method, path: &str, tenant: Uuid, deny: bool) -> String {
        let resource = format!(
            "urn:iam:default:sigbot:global:{tenant}:{}",
            path.trim_start_matches('/')
        );
        let business = format!("urn:iam:default:sigbot:global:{tenant}:environment/paper/workflow/*");
        let context = AccessContext::new(
            AccessContextInput {
                principal_id: "gateway-boundary-test".into(),
                action: if *method == Method::GET {
                    "sigbot:read"
                } else {
                    "sigbot:write"
                }
                .into(),
                resource_urn: resource.clone(),
                allow_resource_urns: vec![resource, business.clone()],
                deny_resource_urns: if deny { vec![business] } else { vec![] },
                policy_revision: 1,
            },
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Duration::from_secs(60),
        );
        sign_access_context(&context, Self::KEY).unwrap()
    }
    async fn request(&self, method: Method, path: &str, body: Option<Value>, tenant: Uuid, deny: bool) -> Response {
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.endpoint))
            .header(
                ACCESS_CONTEXT_HEADER,
                self.header(&method, path.split('?').next().unwrap(), tenant, deny),
            );
        if let Some(body) = body {
            request = request.json(&body);
        }
        request.send().await.unwrap()
    }
    fn configuration(database_url: &str) -> AppConfigProperties {
        let url = url::Url::parse(database_url).unwrap();
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
        postgres.password = url.password().map(|password| {
            percent_encoding::percent_decode_str(password)
                .decode_utf8()
                .unwrap()
                .into_owned()
        });
        postgres.min_connections = Some(1);
        postgres.max_connections = Some(4);
        config
    }
}

#[tokio::test]
#[ignore = "requires MARKETPULSE_DATABASE_URL and actual project migrations"]
async fn signed_gateway_context_is_scoped_through_real_http_workflow_transactions() {
    let database_url = std::env::var("MARKETPULSE_DATABASE_URL").expect("integration database required");
    let config = AppConfig::new(&Fixture::configuration(&database_url));
    let mut state = SigbotState::new(&config).await;
    state.authguard = Arc::new(AuthguardProvider::with_resolver(
        config.authguard.clone(),
        Arc::new(HeaderAccessContextResolver::new(Fixture::KEY).unwrap()),
    ));
    let pool = state.postgres_pool().await.unwrap();
    let router = Router::new()
        .merge(WorkflowHttpApi::routes())
        .merge(IdentityRouter::routes())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            IdentityRouter::authenticate,
        ))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let fixture = Fixture {
        endpoint,
        client: Client::builder().timeout(Duration::from_secs(10)).build().unwrap(),
        tenant: Uuid::now_v7(),
    };
    let path = "/api/v1/workflows";
    assert_eq!(
        fixture
            .client
            .get(format!("{}{path}", fixture.endpoint))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let forbidden = fixture
        .client
        .get(format!("{}{path}?env=paper", fixture.endpoint))
        .header(ACCESS_CONTEXT_HEADER, "agctx1.forged.invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::UNAUTHORIZED);
    let body = json!({"env":"paper","name":"signed API boundary","plain_configuration":{"mode":"paper"},"flow_info":{"nodes":[{
        "id":1,"name":"source","stage":"TWITTER@INPUT","position":{"x":0,"y":0},"config":{"module":"datafeed","argument_json":{
            "messager_config":{"properties":{}},"datafeed_config":{"provider":"TWITTER","properties":{}}
        }}}],"connections":[]}});
    let response = fixture
        .request(Method::POST, path, Some(body.clone()), fixture.tenant, false)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let id: Uuid = response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let row: (Uuid, String, String) = sqlx::query_as("SELECT tenant_id,env,status FROM wf_workflow WHERE id=$1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, (fixture.tenant, "paper".into(), "STOPPED".into()));
    let detail = format!("{path}/{id}");
    assert_eq!(
        fixture
            .request(Method::GET, &detail, None, Uuid::now_v7(), false)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        fixture
            .request(Method::GET, &detail, None, fixture.tenant, true)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut outside_scope = body;
    outside_scope["env"] = json!("prod");
    assert_eq!(
        fixture
            .request(Method::POST, path, Some(outside_scope), fixture.tenant, false)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let list = fixture
        .request(
            Method::GET,
            &format!("{path}?env=paper&num=1&limit=10"),
            None,
            fixture.tenant,
            false,
        )
        .await;
    assert_eq!(list.status(), StatusCode::OK);
    assert!(list.json::<Value>().await.unwrap()["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|workflow| workflow["id"] == json!(id)));
    let start = format!("{detail}/start");
    let first = fixture.request(Method::POST, &start, None, fixture.tenant, false).await;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let run = first.json::<Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let repeated = fixture.request(Method::POST, &start, None, fixture.tenant, false).await;
    assert_eq!(repeated.status(), StatusCode::ACCEPTED);
    assert_eq!(repeated.json::<Value>().await.unwrap()["run_id"], run);
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM wf_run WHERE workflow_id=$1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(runs, 1);
    assert_eq!(
        fixture
            .request(Method::POST, &format!("{detail}/stop"), None, fixture.tenant, false)
            .await
            .status(),
        StatusCode::ACCEPTED
    );
    let status: String = sqlx::query_scalar("SELECT status FROM wf_workflow WHERE id=$1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "STOPPING"); // Actual worker removal belongs to the controller acceptance test.
    sqlx::query("DELETE FROM wf_run WHERE workflow_id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM wf_workflow WHERE id=$1 AND tenant_id=$2")
        .bind(id)
        .bind(fixture.tenant)
        .execute(&pool)
        .await
        .unwrap();
    server.abort();
}
