use super::*;
use serde_json::json;

#[test]
fn notification_argument_uses_the_shared_messager_and_provider_contract() {
    let value = json!({"properties":{"runtime_config":"{}"},
        "messager_config":{"properties":{"mqtt_server":"broker"}}});
    let argument = NotifierArgument::from_json(&value.to_string()).unwrap();
    assert_eq!(argument.properties.as_ref().unwrap()["runtime_config"], "{}");
    let wire = serde_json::to_value(argument).unwrap();
    assert_eq!(wire["properties"]["runtime_config"], "{}");
    assert_eq!(wire["messager_config"]["properties"]["mqtt_server"], "broker");
    assert!(NotifierArgument::from_json(r#"{}"#).is_err());
    assert!(NotifierArgument::from_json(r#"{"messager_config":{}}"#).is_ok());
}
