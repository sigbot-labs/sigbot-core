use serde_json::json;
use sigbot_evaluator::executor::runtime::EvaluationAnswer;

#[test]
fn evaluation_rejects_non_probabilities_and_missing_evidence() {
    let value = json!({"probabilities":{"trend":0.5,"range":0.2,"stabilizing":0.1,"suppression":0.1,"unknown":0.1},
        "parameters":{},"model_version":"fixture-v1","facts":[{"source":"observed"}],"inferences":["hypothesis"],"valid_for_seconds":60});
    let answer: EvaluationAnswer = serde_json::from_value(value.clone()).unwrap();
    answer.validate().unwrap();
    let mut invalid = value.clone();
    invalid["probabilities"]["trend"] = json!(1.2);
    assert!(serde_json::from_value::<EvaluationAnswer>(invalid)
        .unwrap()
        .validate()
        .is_err());
    let mut ungrounded = value;
    ungrounded["facts"] = json!([]);
    assert!(serde_json::from_value::<EvaluationAnswer>(ungrounded)
        .unwrap()
        .validate()
        .is_err());
}
