use serde_json::json;
use sigbot_core::config::config::AppConfigProperties;
use sigbot_core::config::config::ConfigLoader;
#[test]
fn only_canonical_snake_case_keys_are_accepted() {
    let tenant = "0192f7b2-5c00-7000-8000-000000000501";
    let input =
        json!({"service_name":"sigbot-test","server":{"context_path":"/"},"authguard":{"service_tenant_id":tenant}});
    let normalized = ConfigLoader::normalize(input).unwrap();
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
    assert_eq!(
        result.authguard.service_tenant_id.map(|id| id.to_string()),
        Some(tenant.into())
    );
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
        ("SIGBOT__STORAGE__PROVIDER".to_owned(), "Postgres".to_owned()),
        ("SIGBOT_CFG_VERBOSE".to_owned(), "false".to_owned()),
    ]);
    let mut environment: serde_json::Value = config::Config::builder()
        .add_source(
            config::Environment::with_prefix("SIGBOT")
                .separator("__")
                .prefix_separator("__")
                .source(Some(environment)),
        )
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    environment = ConfigLoader::normalize(environment).unwrap();
    let value = config::Config::builder()
        .add_source(config::File::from_str(&defaults, config::FileFormat::Json))
        .add_source(config::File::from_str(
            &normalized.to_string(),
            config::FileFormat::Json,
        ))
        .add_source(config::File::from_str(
            &environment.to_string(),
            config::FileFormat::Json,
        ))
        .build()
        .unwrap();
    let typed = ConfigLoader::deserialize(value).unwrap();
    assert_eq!(typed.mgmt.port, 19490);
    assert!(!typed.mgmt.otel.enabled);
    assert_eq!(
        typed.storage.provider,
        sigbot_core::config::config::StorageProvider::Postgres
    );
}

#[test]
fn storage_accepts_only_the_canonical_section_and_provider_key() {
    use sigbot_core::config::config::StorageProvider;
    let canonical = ConfigLoader::normalize(
        json!({"storage": {"provider": "SQLite", "sqlite": {"dir": "/tmp/sigbot-config-test"}}}),
    )
    .unwrap();
    let merged = config::Config::builder()
        .add_source(config::File::from_str(
            &serde_json::to_string(&AppConfigProperties::default()).unwrap(),
            config::FileFormat::Json,
        ))
        .add_source(config::File::from_str(&canonical.to_string(), config::FileFormat::Json))
        .build()
        .unwrap();
    let properties = ConfigLoader::deserialize(merged).unwrap();
    assert_eq!(properties.storage.provider, StorageProvider::SQLite);
    for obsolete in [
        json!({"database": {"provider": "SQLite"}}),
        json!({"storage": {"type": "SQLite"}}),
        json!({"storage": {"db_type": "SQLite"}}),
    ] {
        let source = config::Config::builder()
            .add_source(config::File::from_str(&obsolete.to_string(), config::FileFormat::Json))
            .build()
            .unwrap();
        assert!(ConfigLoader::deserialize(source).is_err());
    }
}

#[test]
fn redis_nodes_environment_override_is_typed_without_parsing_other_strings() {
    let environment = config::Config::builder()
        .add_source(
            config::Environment::with_prefix("SIGBOT")
                .separator("__")
                .prefix_separator("__")
                .source(Some(std::collections::HashMap::from([
                    (
                        "SIGBOT__CACHE__REDIS__NODES".to_owned(),
                        "redis://redis-0:6379,redis://redis-1:6379".to_owned(),
                    ),
                    (
                        "SIGBOT__SECRETS__VALUES__NUMERIC_PASSWORD".to_owned(),
                        "123456".to_owned(),
                    ),
                ]))),
        )
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    let normalized = ConfigLoader::normalize_environment(environment).unwrap();
    assert_eq!(
        normalized.pointer("/cache/redis/nodes").unwrap(),
        &json!(["redis://redis-0:6379", "redis://redis-1:6379"])
    );
    assert_eq!(
        normalized.pointer("/secrets/values/numeric_password").unwrap(),
        "123456"
    );
}
