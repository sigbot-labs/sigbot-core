use authguard_adapter_rust::sign_access_context;
use authguard_adapter_rust::AccessContext;
use authguard_adapter_rust::AccessContextInput;
use authguard_adapter_rust::HeaderAccessContextResolver;
use authguard_adapter_rust::ACCESS_CONTEXT_HEADER;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use sigbot_core::config::authguard::AuthguardProperties;
use sigbot_core::context::authguard::AuthguardProvider;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

struct Fixture;
impl Fixture {
    const KEY: &'static [u8] = b"sigbot-authguard-test-key-32-bytes-only";
    fn provider() -> AuthguardProvider {
        AuthguardProvider::with_resolver(
            AuthguardProperties::default(),
            Arc::new(HeaderAccessContextResolver::new(Self::KEY).unwrap()),
        )
    }
    fn context(tenant: &str, subject: &str) -> AccessContext {
        let resource = format!("urn:iam:default:sigbot:global:{tenant}:api/v1/workflows");
        AccessContext::new(
            AccessContextInput {
                principal_id: subject.into(),
                action: "sigbot:read".into(),
                resource_urn: resource.clone(),
                allow_resource_urns: vec![resource],
                deny_resource_urns: vec![],
                policy_revision: 1,
            },
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Duration::from_secs(60),
        )
    }
    fn headers(context: &AccessContext) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCESS_CONTEXT_HEADER,
            HeaderValue::from_str(&sign_access_context(context, Self::KEY).unwrap()).unwrap(),
        );
        headers
    }
}

struct ScopeTokenFixture {
    encoded: String,
}
#[async_trait::async_trait]
impl authguard_adapter_rust::ScopeTokenClient for ScopeTokenFixture {
    async fn resolve_scope(&self, token: &str) -> Result<String, authguard_adapter_rust::AccessError> {
        if token == "isolated-scope-token" {
            Ok(self.encoded.clone())
        } else {
            Err(authguard_adapter_rust::AccessError::InvalidContext(
                "invalid fixture token".into(),
            ))
        }
    }
}

#[tokio::test]
async fn bearer_bridge_uses_the_official_signed_header_and_grpc_scope_resolvers() {
    let context = Fixture::context("10", "alice");
    let signed = sign_access_context(&context, Fixture::KEY).unwrap();
    let identity = Fixture::provider()
        .authenticate_bearer(&signed, &Method::GET, "/api/v1/workflows")
        .await
        .unwrap();
    assert_eq!(identity.principal_id(), "alice");
    let config = AuthguardProperties {
        provider: sigbot_core::config::authguard::AuthguardProviderKind::Grpc,
        ..Default::default()
    };
    let resolver = authguard_adapter_rust::GrpcAccessContextResolver::new(Arc::new(ScopeTokenFixture {
        encoded: authguard_adapter_rust::encode_access_context(&context).unwrap(),
    }));
    let provider = AuthguardProvider::with_resolver(config, Arc::new(resolver));
    assert_eq!(
        provider
            .authenticate_bearer("isolated-scope-token", &Method::GET, "/api/v1/workflows")
            .await
            .unwrap()
            .principal_id(),
        "alice"
    );
    assert!(provider
        .authenticate_bearer("wrong-token", &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
    assert!(provider
        .authenticate_bearer("isolated-scope-token", &Method::GET, "/api/v1/other")
        .await
        .is_err());
}

#[tokio::test]
async fn identities_are_isolated_across_concurrent_requests() {
    let provider = Arc::new(Fixture::provider());
    let mut tasks = Vec::new();
    for index in 1..=32 {
        let provider = provider.clone();
        tasks.push(tokio::spawn(async move {
            let tenant = index.to_string();
            let context = Fixture::context(&tenant, &format!("user-{index}"));
            let identity = provider
                .authenticate(&Fixture::headers(&context), &Method::GET, "/api/v1/workflows")
                .await
                .unwrap();
            tokio::task::yield_now().await;
            assert_eq!(identity.tenant_id(), tenant);
            assert_eq!(identity.principal_id(), format!("user-{index}"));
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn rejects_signature_expiry_and_context_replay() {
    let provider = Fixture::provider();
    let context = Fixture::context("10", "alice");
    let headers = Fixture::headers(&context);
    assert!(provider
        .authenticate(&headers, &Method::POST, "/api/v1/workflows")
        .await
        .is_err());
    assert!(provider
        .authenticate(&headers, &Method::GET, "/api/v1/identity")
        .await
        .is_err());
    let mut headers = headers;
    headers.insert("x-sigbot-tenant-id", HeaderValue::from_static("11"));
    assert!(provider
        .authenticate(&headers, &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
    let mut expired = context.clone();
    expired.issued_at_epoch_seconds = 1;
    expired.expires_at_epoch_seconds = 2;
    assert!(provider
        .authenticate(&Fixture::headers(&expired), &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
    headers.insert(ACCESS_CONTEXT_HEADER, HeaderValue::from_static("agctx1.forged.invalid"));
    assert!(provider
        .authenticate(&headers, &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
}

#[tokio::test]
async fn deny_overrides_allow_and_missing_context_fails_closed() {
    let provider = Fixture::provider();
    let mut context = Fixture::context("10", "alice");
    context.deny_resource_urns = context.allow_resource_urns.clone();
    assert!(provider
        .authenticate(&Fixture::headers(&context), &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
    assert!(provider
        .authenticate(&HeaderMap::new(), &Method::GET, "/api/v1/workflows")
        .await
        .is_err());
}
