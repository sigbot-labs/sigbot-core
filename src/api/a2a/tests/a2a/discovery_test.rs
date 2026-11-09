use super::*;
use a2a::{A2AError, StreamResponse};
use a2a_server::ExecutorContext;
use authguard_adapter_rust::{sign_access_context, AccessContext, AccessContextInput, HeaderAccessContextResolver};
use futures::stream::BoxStream;
use serde_json::Value;
use sigbot_core::config::authguard::AuthguardProperties;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct DiscoveryExecutor;
impl AgentExecutor for DiscoveryExecutor {
    fn execute(&self, _ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(futures::stream::empty())
    }
    fn cancel(&self, _ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(futures::stream::empty())
    }
}
struct DiscoveryFixture;
impl DiscoveryFixture {
    const KEY: &'static [u8] = b"isolated-a2a-discovery-context-signing-key";
    fn token(path: &str) -> String {
        let tenant = sigbot_types::Uuid::now_v7();
        let resource = format!(
            "urn:iam:default:sigbot:global:{tenant}:{}",
            path.trim_start_matches('/')
        );
        let context = AccessContext::new(
            AccessContextInput {
                principal_id: "a2a-discovery-test".into(),
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
    fn authguard() -> Arc<AuthguardProvider> {
        Arc::new(AuthguardProvider::with_resolver(
            AuthguardProperties::default(),
            Arc::new(HeaderAccessContextResolver::new(Self::KEY).unwrap()),
        ))
    }
}

#[tokio::test]
async fn official_public_and_authenticated_extended_cards_use_the_advertised_rpc_endpoint() -> Result<()> {
    let config = A2AProperties {
        public_url: "https://sigbot.example.test/a2a".into(),
        ..Default::default()
    };
    let oauth = OAuthDiscoveryProperties {
        issuer: "https://authority.example.test/".into(),
        discovery_url: "https://authority.example.test/.well-known/openid-configuration".into(),
        scopes: vec![],
    };
    let router =
        ResourceA2aApi::protocol_router(&config, Some(&oauth), DiscoveryFixture::authguard(), DiscoveryExecutor)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .default_headers(axum::http::HeaderMap::from_iter([(
            HeaderName::from_static("a2a-version"),
            HeaderValue::from_static(a2a::VERSION),
        )]))
        .build()?;
    let response = client.get(format!("{base}/.well-known/agent-card.json")).send().await?;
    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("cache-control"));
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    let card: Value = response.json().await?;
    assert_eq!(card["supportedInterfaces"][0]["url"], config.public_url);
    assert_eq!(card["supportedInterfaces"][0]["protocolVersion"], a2a::VERSION);
    assert_eq!(card["capabilities"]["extendedAgentCard"], true);
    assert_eq!(card["skills"].as_array().unwrap().len(), 10);
    assert_eq!(
        card["securitySchemes"]["authguard"]["openIdConnectSecurityScheme"]["openIdConnectUrl"],
        oauth.discovery_url
    );
    let request = json!({"jsonrpc":"2.0","id":1,"method":a2a::methods::GET_EXTENDED_AGENT_CARD,"params":{}});
    let unauthorized = client
        .post(format!("{base}/a2a"))
        .json(&request)
        .header(VERIFIED_ACCESS, "{\"principal_id\":\"forged\"}")
        .send()
        .await?;
    assert_eq!(unauthorized.status(), 401);
    assert_eq!(unauthorized.headers()["www-authenticate"], "Bearer");
    let response = client
        .post(format!("{base}/a2a"))
        .bearer_auth(DiscoveryFixture::token("/a2a"))
        .json(&request)
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let extended: Value = response.json().await?;
    assert!(extended.get("error").is_none(), "{extended}");
    assert_eq!(extended["result"]["name"], card["name"]);
    let response = client
        .post(format!("{base}/a2a/"))
        .header("x-authguard-context", DiscoveryFixture::token("/a2a"))
        .json(&request)
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let extended: Value = response.json().await?;
    assert!(extended.get("error").is_none(), "{extended}");
    let wrong_path = client
        .post(format!("{base}/a2a"))
        .bearer_auth(DiscoveryFixture::token("/different"))
        .json(&request)
        .send()
        .await?;
    assert_eq!(wrong_path.status(), 401);
    server.abort();
    let _ = server.await;
    Ok(())
}

#[test]
fn card_uses_standard_bearer_when_oidc_is_not_configured_and_rejects_non_https_public_hosts() -> Result<()> {
    let config = A2AProperties::default();
    let card = ResourceA2aApi::card(&config, None)?;
    let value = serde_json::to_value(card)?;
    assert_eq!(
        value["securitySchemes"]["authguard"]["httpAuthSecurityScheme"]["scheme"],
        "Bearer"
    );
    let config = A2AProperties {
        public_url: "http://public.example.test/a2a".into(),
        ..config
    };
    assert!(ResourceA2aApi::card(&config, None).is_err());
    Ok(())
}
