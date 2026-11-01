use super::*;
use serde_json::json;

#[test]
fn argument_preserves_provider_wire_values_and_host_secret_references() {
    let value = json!({"datafeed_config":{"name":"BINANCE","provider":"BINANCE",
        "properties":{"endpoint":"https://api.binance.com"}},
        "messager_config":{"properties":{"mqtt_server":"broker"},"secrets":{"mqtt_password_secret":"broker_password"}}});
    let argument = DatafeedArgument::from_json(&value.to_string()).unwrap();
    assert_eq!(
        argument.datafeed_config.provider,
        Some(datafeed::DatafeedProvider::Binance)
    );
    assert_eq!(argument.datafeed_config.name.as_deref(), Some("BINANCE"));
    let wire = serde_json::to_value(argument).unwrap();
    assert_eq!(wire["datafeed_config"]["provider"], "BINANCE");
    assert_eq!(
        wire["messager_config"]["secrets"]["mqtt_password_secret"],
        "broker_password"
    );
    assert!(DatafeedArgument::from_json(r#"{"datafeed_config":{}}"#).is_err());
}
