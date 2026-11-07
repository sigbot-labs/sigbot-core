//! Real TCP boundary test with signed Authguard contexts and an isolated SQLite schema.
use authguard_adapter_rust::{
    sign_access_context, AccessContext, AccessContextInput, HeaderAccessContextResolver, ACCESS_CONTEXT_HEADER,
};
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};
use sigbot_core::config::config::{AppConfig, AppConfigProperties, StorageProvider};
use sigbot_core::context::authguard::AuthguardProvider;
use sigbot_core::context::state::SigbotState;
use sigbot_http::http::{AxumHttpApi, IHttpApi};
use sigbot_types::{domain::Domain, Uuid};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct Fixture {
    endpoint: String,
    client: Client,
    tenant: Uuid,
}
impl Fixture {
    const KEY: &'static [u8] = b"isolated-domain-test-authguard-signing-key";
    fn header(&self, method: &Method, path: &str, tenant: Uuid, deny: bool) -> String {
        let resource = format!(
            "urn:iam:default:sigbot:global:{tenant}:{}",
            path.trim_start_matches('/')
        );
        let business = format!("urn:iam:default:sigbot:global:{tenant}:environment/dev/**");
        let context = AccessContext::new(
            AccessContextInput {
                principal_id: "domain-http-test".into(),
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
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        tenant: Uuid,
        deny: bool,
    ) -> reqwest::Response {
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
}
#[tokio::test]
async fn signed_context_enforces_domain_crud_and_tenant_boundaries() -> anyhow::Result<()> {
    let directory = std::env::temp_dir().join(format!("sigbot-domain-http-{}", Uuid::now_v7()));
    std::fs::create_dir(&directory)?;
    let database = directory.join("sqlite.db");
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(true),
        )
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../migration/20261031000000/sqlite/init.ddl.sql"
    ))
    .execute(&pool)
    .await?;
    pool.close().await;
    let mut properties = AppConfigProperties::default();
    properties.storage.provider = StorageProvider::SQLite;
    properties.storage.sqlite.dir = Some(directory.to_string_lossy().into());
    let config = AppConfig::new(&properties);
    let mut state = SigbotState::new(&config).await;
    state.authguard = Arc::new(AuthguardProvider::with_resolver(
        config.authguard.clone(),
        Arc::new(HeaderAccessContextResolver::new(Fixture::KEY)?),
    ));
    let router = AxumHttpApi::new(state).router(None, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture {
        endpoint: format!("http://{}", listener.local_addr()?),
        client: Client::builder().timeout(Duration::from_secs(10)).build()?,
        tenant: Uuid::now_v7(),
    };
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    assert_eq!(
        fixture
            .client
            .get(format!("{}/api/v1/tenants?env=dev", fixture.endpoint))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let create = fixture
        .request(
            Method::POST,
            "/api/v1/tenants?env=dev",
            Some(json!({"name":"Tenant"})),
            fixture.tenant,
            false,
        )
        .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    for domain in Domain::ALL {
        let collection = format!("/api/v1/{}?env=dev", domain.collection());
        let response = fixture
            .request(Method::GET, &collection, None, fixture.tenant, false)
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{}", domain.as_str());
        assert_eq!(
            fixture
                .request(Method::GET, &collection, None, fixture.tenant, true)
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    let path = "/api/v1/datasources?env=dev";
    let body = json!({"name":"Twitter","provider":"twitter","transport":"http","configuration":{"endpoint":"https://example.test"},"secret_ref":"secret://twitter_credentials"});
    let response = fixture
        .request(Method::POST, path, Some(body), fixture.tenant, false)
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = response.json::<Value>().await?["id"].as_str().unwrap().to_owned();
    let detail = format!("/api/v1/datasources/{id}?env=dev");
    assert_eq!(
        fixture
            .request(Method::GET, &detail, None, Uuid::now_v7(), false)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        fixture
            .request(
                Method::PUT,
                &detail,
                Some(json!({"name":"Updated"})),
                fixture.tenant,
                false
            )
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request(
                Method::PUT,
                &detail,
                Some(json!({"unknown":"invalid"})),
                fixture.tenant,
                false
            )
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        fixture
            .request(
                Method::PUT,
                &detail,
                Some(json!({"tenant_id":Uuid::now_v7()})),
                fixture.tenant,
                false
            )
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        fixture
            .request(Method::DELETE, &detail, None, fixture.tenant, false)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request(Method::GET, &detail, None, fixture.tenant, false)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(directory)?;
    Ok(())
}
