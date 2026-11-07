use sigbot_mcp::{HttpMcpApi, IMcpApi};

fn assert_provider<T: IMcpApi>() {}

#[test]
fn server_implements_mcp_provider() {
    assert_provider::<HttpMcpApi>();
}

#[test]
fn registry_exposes_ten_domains_with_complete_crud() {
    let tools = sigbot_mcp::domain::resource::ResourceMcpRegistry::tools();
    assert_eq!(tools.len(), 50);
    for domain in sigbot_types::domain::Domain::ALL {
        for operation in ["list", "get", "create", "update", "delete"] {
            let tool = tools
                .iter()
                .find(|tool| tool.name == format!("{}_{operation}", domain.as_str()))
                .unwrap();
            assert_eq!(
                tool.annotations.as_ref().unwrap().read_only_hint,
                Some(matches!(operation, "list" | "get"))
            );
        }
    }
}
