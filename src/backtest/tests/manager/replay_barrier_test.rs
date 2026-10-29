use sigbot_backtest::manager::replay_barrier::{ReplayBarrierConfig, ReplayPayloadKind, ReplaySubscription};
use sigbot_types::modules::messager::topic::MessageScope;

#[test]
fn legacy_owner_normalization_is_scoped_and_leaves_frozen_configuration_unchanged() {
    let scope = MessageScope {
        env: "replay".into(),
        tenant_id: 1,
        space_id: 2,
    };
    let names = [
        "order:replay:1:2:6",
        "order.execution:replay:1:2:6",
        "order:replay:1:2:6:reservation",
        "order:replay:1:2:6:venue",
        "export:replay:1:2:6",
        "order:other:1:2:6",
        "wallet:replay:1:2:6",
        "order:replay:1:2:123:6",
    ];
    let config = ReplayBarrierConfig {
        scope,
        run_id: "run".into(),
        subscriptions: names
            .iter()
            .map(|name| ReplaySubscription {
                producer_node_id: 5,
                target_node_id: 6,
                topic: "unused-in-normalization".into(),
                consumer: (*name).into(),
                payload_kind: ReplayPayloadKind::Any,
            })
            .collect(),
        terminal_topics: vec![],
        max_messages: 10,
        timeout_seconds: 1,
    };
    let frozen = serde_json::to_value(&config).unwrap();
    let mut runtime = config.clone();
    runtime.migrate_legacy_owners(123).unwrap();
    assert_eq!(runtime.subscriptions[0].consumer, "order:replay:1:2:123:6");
    assert_eq!(runtime.subscriptions[1].consumer, "order.execution:replay:1:2:123:6");
    assert_eq!(runtime.subscriptions[2].consumer, "order:replay:1:2:123:6:reservation");
    assert_eq!(runtime.subscriptions[3].consumer, "order:replay:1:2:123:6:venue");
    assert_eq!(runtime.subscriptions[4].consumer, "export:replay:1:2:123:6");
    for index in 5..8 {
        assert_eq!(runtime.subscriptions[index].consumer, names[index]);
    }
    let once = serde_json::to_value(&runtime).unwrap();
    runtime.migrate_legacy_owners(123).unwrap();
    assert_eq!(serde_json::to_value(&runtime).unwrap(), once);
    assert_eq!(serde_json::to_value(&config).unwrap(), frozen);
}
