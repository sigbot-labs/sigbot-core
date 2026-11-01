use serde_json::json;
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;

#[test]
fn deployment_environments_do_not_select_a_trading_mode() {
    for env in ["dev", "sit", "uat", "prod"] {
        let scope = MessageScope {
            env: env.into(),
            tenant_id: Uuid::now_v7(),
        };
        scope.validate().unwrap();
        for (mode, name) in [
            (ExecutionMode::Live, "live"),
            (ExecutionMode::Paper, "paper"),
            (ExecutionMode::Backtest, "backtest"),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), json!(name));
            assert!(scope.prefix().unwrap().starts_with(&format!("internal/v2/{env}/")));
        }
    }
    assert!(serde_json::from_value::<ExecutionMode>(json!("sandbox")).is_err());
    assert!(serde_json::from_value::<ExecutionMode>(json!("prod")).is_err());
}
