use super::*;
use authguard_adapter_rust::{sign_access_context, AccessContext, AccessContextInput, HeaderAccessContextResolver};
use rust_mcp_sdk::mcp_server::ServerHandler;
use serde_json::{json, Value};
use sigbot_core::config::authguard::AuthguardProperties;
use std::time::{SystemTime, UNIX_EPOCH};

struct DiscoveryFixture {
    endpoint: String,
    metadata: Value,
    task: JoinHandle<()>,
}
impl DiscoveryFixture {
    const KEY: &'static [u8] = b"isolated-mcp-discovery-context-signing-key";
    async fn start() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let metadata = json!({"issuer":format!("{endpoint}/"),"authorization_endpoint":format!("{endpoint}/authorize"),"token_endpoint":format!("{endpoint}/token"),"jwks_uri":format!("{endpoint}/jwks"),"response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],"code_challenge_methods_supported":["S256"],"scopes_supported":["sigbot:read","sigbot:write"]});
        let response = metadata.clone();
        let router = axum::Router::new().route(
            "/.well-known/openid-configuration",
            axum::routing::get(move || {
                let response = response.clone();
                async move { axum::Json(response) }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Ok(Self {
            endpoint,
            metadata,
            task,
        })
    }
    fn oauth(&self) -> OAuthDiscoveryProperties {
        OAuthDiscoveryProperties {
            issuer: format!("{}/", self.endpoint),
            discovery_url: format!("{}/.well-known/openid-configuration", self.endpoint),
            scopes: vec!["sigbot:read".into(), "sigbot:write".into()],
        }
    }
    fn authguard() -> Arc<AuthguardProvider> {
        Arc::new(AuthguardProvider::with_resolver(
            AuthguardProperties::default(),
            Arc::new(HeaderAccessContextResolver::new(Self::KEY).unwrap()),
        ))
    }
    fn token() -> String {
        let tenant = sigbot_types::Uuid::now_v7();
        let resource = format!("urn:iam:default:sigbot:global:{tenant}:mcp");
        let context = AccessContext::new(
            AccessContextInput {
                principal_id: "discovery-test".into(),
                action: "sigbot:write".into(),
                resource_urn: resource.clone(),
                allow_resource_urns: vec![resource],
                deny_resource_urns: vec![],
                policy_revision: 1,
            },
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Duration::from_secs(60),
        );
        sign_access_context(&context, Self::KEY).unwrap()
    }
}
impl Drop for DiscoveryFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct DiscoveryHandler;
impl ServerHandler for DiscoveryHandler {}

#[tokio::test]
async fn official_sdk_serves_anonymous_discovery_and_authentication_challenge() -> Result<()> {
    let fixture = DiscoveryFixture::start().await?;
    let config = ApiMcpProperties {
        enabled: true,
        bind_address: "127.0.0.1:0".into(),
        public_url: "https://sigbot.example.test/mcp".into(),
        ..Default::default()
    };
    let auth = HttpMcpApi::oauth_provider(&config, &fixture.oauth(), DiscoveryFixture::authguard()).await?;
    let server = HttpMcpApi::server(&config, DiscoveryHandler.to_mcp_server_handler(), Arc::new(auth))?;
    let handle = server.server_handle();
    let task = tokio::spawn(server.start());
    let address = tokio::time::timeout(Duration::from_secs(5), handle.listening())
        .await?
        .context("MCP server did not bind")?;
    let base = format!("http://{address}");
    let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
    let resource_path = "/.well-known/oauth-protected-resource/mcp";
    let response = client.get(format!("{base}{resource_path}")).send().await?;
    assert_eq!(response.status(), 200);
    let resource: Value = response.json().await?;
    assert_eq!(resource["resource"], config.public_url);
    assert_eq!(resource["authorization_servers"], json!([fixture.oauth().issuer]));
    assert_eq!(resource["bearer_methods_supported"], json!(["header"]));
    assert_eq!(resource["scopes_supported"], json!(["sigbot:read", "sigbot:write"]));
    let metadata: Value = client
        .get(format!("{base}/.well-known/oauth-authorization-server"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(metadata["token_endpoint"], fixture.metadata["token_endpoint"]);
    assert_eq!(metadata["issuer"], fixture.metadata["issuer"]);
    assert_eq!(metadata["code_challenge_methods_supported"], json!(["S256"]));
    assert_eq!(
        client
            .post(format!("{base}{resource_path}"))
            .body("")
            .send()
            .await?
            .status(),
        405
    );
    assert_eq!(
        client.head(format!("{base}{resource_path}")).send().await?.status(),
        200
    );
    let unauthorized=client.post(format!("{base}/mcp")).header("Accept","application/json, text/event-stream").json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"discovery-test","version":"1"}}})).send().await?;
    assert_eq!(unauthorized.status(), 401);
    assert!(unauthorized.headers()["www-authenticate"]
        .to_str()?
        .contains("resource_metadata=\"https://sigbot.example.test/.well-known/oauth-protected-resource/mcp\""));
    let authenticated=client.post(format!("{base}/mcp")).bearer_auth(DiscoveryFixture::token()).header("Accept","application/json, text/event-stream").json(&json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"discovery-test","version":"1"}}})).send().await?;
    assert!(authenticated.status().is_success(), "{}", authenticated.status());
    assert_eq!(
        client
            .get(format!("{base}/.well-known/openid-configuration"))
            .send()
            .await?
            .status(),
        404
    );
    handle.shutdown();
    let _ = task.await;
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_wrong_issuer_and_unsafe_public_url() -> Result<()> {
    let fixture = DiscoveryFixture::start().await?;
    let config = ApiMcpProperties {
        public_url: "https://sigbot.example.test/mcp".into(),
        ..Default::default()
    };
    let mut oauth = fixture.oauth();
    oauth.issuer = "https://different-authority.example.test/".into();
    assert!(
        HttpMcpApi::oauth_provider(&config, &oauth, DiscoveryFixture::authguard())
            .await
            .is_err()
    );
    let config = ApiMcpProperties {
        public_url: "http://public.example.test/mcp".into(),
        ..config
    };
    assert!(
        HttpMcpApi::oauth_provider(&config, &fixture.oauth(), DiscoveryFixture::authguard())
            .await
            .is_err()
    );
    Ok(())
}
