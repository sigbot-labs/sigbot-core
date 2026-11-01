use sigbot_core::config::config::{AppConfig, AppConfigProperties};
use sigbot_http::swagger::SwaggerHttpApi;

struct SwaggerFixture;

impl SwaggerFixture {
    fn document() -> serde_json::Value {
        let mut properties = AppConfigProperties::default();
        properties.server.context_path = Some("/sigbot".into());
        serde_json::to_value(SwaggerHttpApi::document(&AppConfig::new(&properties))).unwrap()
    }
}

#[test]
fn document_covers_current_scoped_api_and_security() {
    let document = SwaggerFixture::document();
    assert!(document["paths"]["/sigbot/api/v1/workers"].is_object());
    assert!(document["paths"]["/sigbot/api/v1/workers/defaults"].is_object());
    assert!(document["paths"]["/sigbot/api/v1/workflows/{id}/start"].is_object());
    assert!(document["components"]["schemas"]["WorkerAuthConfig"].is_object());
    assert_eq!(
        document["components"]["securitySchemes"]["authguard_bearer"]["scheme"],
        "bearer"
    );
}

#[test]
fn vendored_swagger_ui_archive_is_present() {
    let archive = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/swagger/swagger-ui-5.17.14.zip");
    assert!(archive.is_file());
    assert!(archive.metadata().unwrap().len() > 1_000_000);
}
