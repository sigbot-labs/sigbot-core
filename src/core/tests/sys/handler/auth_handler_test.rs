use authguard_adapter_rust::{
    sign_access_context, AccessContext, AccessContextInput, HeaderAccessContextResolver, ACCESS_CONTEXT_HEADER,
};
use axum::http::{HeaderMap, HeaderValue, Method};
use sigbot_core::{config::authguard::AuthguardProperties, sys::handler::auth_handler::AuthguardProvider};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
