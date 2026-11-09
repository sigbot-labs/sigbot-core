use serde_json::json;
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::workflow::deployment::WorkflowDeployment;
use sigbot_types::workflow::workflow::WorkflowFlowInfo;

struct Fixture;
impl Fixture {
    fn flow(modes: &[&str]) -> WorkflowFlowInfo {
        serde_json::from_value(
            json!({"connections": [], "nodes": modes.iter().enumerate().map(|(i, mode)| {
            json!({"id": i + 1, "name": "order", "stage": "DEFAULT@TRANSACTION", "position": {"x": 0, "y": 0},
                "config": {"module": "order", "argument_json": {"properties": {"runtime_config":
                    json!({"exchange": {"mode": mode}}).to_string()}}}})
        }).collect::<Vec<_>>()}),
        )
        .unwrap()
    }
}

#[test]
fn workflow_freezes_one_mode_and_rejects_conflicting_execution_adapters() {
    assert_eq!(
        WorkflowDeployment::execution_mode(&Fixture::flow(&["backtest"]), Some(&json!("backtest"))).unwrap(),
        ExecutionMode::Backtest
    );
    assert!(WorkflowDeployment::execution_mode(&Fixture::flow(&["live"]), Some(&json!("paper"))).is_err());
    assert!(WorkflowDeployment::execution_mode(&Fixture::flow(&["paper", "backtest"]), Some(&json!("paper"))).is_err());
    assert_eq!(
        WorkflowDeployment::execution_mode(&Fixture::flow(&[]), Some(&json!("live"))).unwrap(),
        ExecutionMode::Live
    );
    assert!(WorkflowDeployment::execution_mode(&Fixture::flow(&["sandbox"]), Some(&json!("sandbox"))).is_err());
    assert!(WorkflowDeployment::execution_mode(&Fixture::flow(&["paper"]), None).is_err());
}
