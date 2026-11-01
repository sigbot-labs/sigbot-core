use super::*;
use serde_json::json;

#[test]
fn strategy_argument_requires_a_run_mode_and_preserves_runtime_configuration() {
    let value = json!({"messager_config":{"properties":{"mqtt_server":"broker"}},
        "sys_environment":{},"run_mode":"V2","properties":{"runtime_config":"{}"}});
    let argument = StrategyArgument::from_json(&value.to_string()).unwrap();
    assert_eq!(argument.run_mode, "V2");
    assert!(argument.sys_environment.as_ref().unwrap().is_empty());
    assert_eq!(
        serde_json::to_value(argument).unwrap(),
        json!({"messager_config":{"properties":{"mqtt_server":"broker"},"secrets":null},
        "sys_environment":{},"run_mode":"V2","properties":{"runtime_config":"{}"}})
    );
    assert!(StrategyArgument::from_json(r#"{"messager_config":{}}"#).is_err());
}
