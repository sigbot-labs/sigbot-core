use serde_json::json;
use sigbot_core::config::config::{AppConfigProperties, ConfigLoader};
#[test]
fn canonical_keys_retain_legacy_file_compatibility_without_duplicate_defaults() {
    let legacy =
        json!({"service-name":"sigbot-test","server":{"context-path":"/"},"authguard":{"service-tenant-id":7}});
    let normalized = ConfigLoader::normalize(legacy).unwrap();
    let config = config::Config::builder()
        .add_source(config::File::from_str(
            &serde_json::to_string(&AppConfigProperties::default()).unwrap(),
            config::FileFormat::Json,
        ))
        .add_source(config::File::from_str(
            &normalized.to_string(),
            config::FileFormat::Json,
        ))
        .build()
        .unwrap();
    let result: AppConfigProperties = config.try_deserialize().unwrap();
    assert_eq!(result.service_name, "sigbot-test");
    assert_eq!(result.authguard.service_tenant_id, Some(7));
    assert!(ConfigLoader::normalize(json!({"service-name":"one","service_name":"two"})).is_err());
}
#[test]
fn unknown_keys_are_rejected_and_placeholder_defaults_are_explicit() {
    let mut value = serde_json::to_value(AppConfigProperties::default()).unwrap();
    value["unexpected"] = json!(true);
    assert!(serde_json::from_value::<AppConfigProperties>(value).is_err());
    assert_eq!(
        ConfigLoader::normalize(json!("${SIGBOT_TEST_UNSET_CONFIG_VALUE:fallback}")).unwrap(),
        json!("fallback")
    );
    assert!(ConfigLoader::normalize(json!("${SIGBOT_TEST_UNSET_CONFIG_VALUE}")).is_err());
}

#[test]
fn shipped_configuration_accepts_nested_environment_overrides() {
    let defaults = serde_json::to_string(&AppConfigProperties::default()).unwrap();
    let file = config::Config::builder()
        .add_source(config::File::from_str(
            include_str!("../../../../etc/sigbot.yaml"),
            config::FileFormat::Yaml,
        ))
        .build()
        .unwrap();
    let normalized = ConfigLoader::normalize(file.try_deserialize().unwrap()).unwrap();
    let environment = std::collections::HashMap::from([
        ("SIGBOT__MGMT__PORT".to_owned(), "19490".to_owned()),
        ("SIGBOT__MGMT__OTEL__ENABLED".to_owned(), "false".to_owned()),
        ("SIGBOT__APPDB__TYPE".to_owned(), "POSTGRESQL".to_owned()),
        ("SIGBOT_CFG_VERBOSE".to_owned(), "false".to_owned()),
    ]);
    let value = config::Config::builder()
        .add_source(config::File::from_str(&defaults, config::FileFormat::Json))
        .add_source(config::File::from_str(
            &normalized.to_string(),
            config::FileFormat::Json,
        ))
        .add_source(
            config::Environment::with_prefix("SIGBOT")
                .separator("__")
                .prefix_separator("__")
                .source(Some(environment)),
        )
        .build()
        .unwrap();
    let typed = ConfigLoader::deserialize(value).unwrap();
    assert_eq!(typed.mgmt.port, 19490);
    assert!(!typed.mgmt.otel.enabled);
    assert_eq!(typed.appdb.db_type, sigbot_core::config::config::AppDBType::POSTGRESQL);
}

#[test]
fn legacy_timescaledb_provider_uses_one_postgres_connection_configuration() {
    use sigbot_core::config::config::{AppConfig, AppDBType};
    let mut legacy = AppConfigProperties::default();
    legacy.appdb.db_type = AppDBType::TIMESCALEDB;
    legacy.appdb.timescaledb.inner.database = "existing_timescale_business".into();
    legacy.appdb.timescaledb.inner.schema = "public".into();
    let value = config::Config::builder()
        .add_source(config::File::from_str(
            &serde_json::to_string(&legacy).unwrap(),
            config::FileFormat::Json,
        ))
        .build()
        .unwrap();
    let canonical = ConfigLoader::deserialize(value).unwrap();
    assert_eq!(canonical.appdb.db_type, AppDBType::POSTGRESQL);
    assert_eq!(canonical.appdb.postgres.inner.database, "existing_timescale_business");
    let direct = AppConfig::new(&legacy);
    assert_eq!(direct.appdb.db_type, AppDBType::POSTGRESQL);
    assert_eq!(direct.appdb.postgres.inner.database, "existing_timescale_business");
    legacy.appdb.db_type = AppDBType::POSTGRESQL;
    assert_ne!(
        AppConfig::new(&legacy).appdb.postgres.inner.database,
        "existing_timescale_business"
    );
}
