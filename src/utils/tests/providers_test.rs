use serde_json::json;
use sigbot_utils::providers::ProviderInstanceId;

#[test]
fn configured_instances_are_tenant_scoped_and_ignore_telemetry() {
    let first = json!({"tenant_id":1,"account_id":2,"mode":"paper","traceparent":"first"});
    let reordered = json!({"traceparent":"second","mode":"paper","account_id":2,"tenant_id":1});
    let second_tenant = json!({"tenant_id":2,"account_id":2,"mode":"paper"});
    let live = json!({"tenant_id":1,"account_id":2,"mode":"live"});
    let key = ProviderInstanceId::configured("provider", &first).unwrap();
    assert_eq!(key, ProviderInstanceId::configured("provider", &reordered).unwrap());
    assert_ne!(key, ProviderInstanceId::configured("provider", &second_tenant).unwrap());
    assert_ne!(key, ProviderInstanceId::configured("provider", &live).unwrap());
    assert!(!key.contains("first"));
}
