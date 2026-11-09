use serde_json::json;
use sigbot_types::controller::{SaveWorker, WorkerAuthConfig, WorkerConfiguration, WorkerProvider};

struct WorkerContractFixture;

impl WorkerContractFixture {
    fn request() -> serde_json::Value {
        json!({
            "id": null,
            "env": "prod",
            "workflow_id": "0192f7b2-5c00-7000-8000-000000000211",
            "name": "remote-kubernetes",
            "provider": "kubernetes",
            "conn_url": "https://worker.example.test:6443",
            "auth_config": {
                "auth_type": "kubernetes_private_key",
                "client_certificate_secret": "worker_client_certificate",
                "private_key_secret": "worker_private_key",
                "certificate_authority_secret": "worker_certificate_authority"
            },
            "configuration": {
                "runtime_type": "kubernetes",
                "namespace": "tenant-7",
                "runtime_secret": "sigbot-runtime",
                "runtime_config_map": "sigbot-runtime",
                "service_account": "sigbot-worker"
            }
        })
    }
}

#[test]
fn worker_configuration_is_typed_and_provider_scoped() {
    let request: SaveWorker = serde_json::from_value(WorkerContractFixture::request()).unwrap();
    assert_eq!(request.provider, WorkerProvider::Kubernetes);
    assert!(matches!(request.configuration, WorkerConfiguration::Kubernetes { .. }));
    assert!(request.auth_config.supports(request.provider));
    assert_eq!(request.auth_config.secret_keys().len(), 3);
}

#[test]
fn worker_authentication_rejects_unknown_or_plaintext_fields() {
    let mut request = WorkerContractFixture::request();
    request["auth_config"]["private_key_base64"] = json!("plaintext-material");
    assert!(serde_json::from_value::<SaveWorker>(request).is_err());

    assert!(!WorkerAuthConfig::DockerAnonymous.supports(WorkerProvider::Kubernetes));
}

#[test]
fn tenant_middleware_accepts_only_typed_image_and_resources() {
    let mut request = WorkerContractFixture::request();
    request["middleware"] = json!({
        "redis": {
            "image": "registry-1.docker.io/bitnami/redis-cluster:8.2.1",
            "resources": {"cpu_millis": 500, "memory_mb": 512, "storage_gb": 8}
        },
        "pgvector": null,
        "timescaledb": null,
        "emqx": null
    });
    assert!(serde_json::from_value::<SaveWorker>(request.clone()).is_ok());
    request["middleware"]["redis"]["environment"] = json!({"ALLOW_EMPTY_PASSWORD": "yes"});
    assert!(serde_json::from_value::<SaveWorker>(request).is_err());
}
