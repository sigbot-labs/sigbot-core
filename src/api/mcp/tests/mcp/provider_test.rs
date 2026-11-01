use sigbot_mcp::{HttpMcpApi, IMcpApi};

fn assert_provider<T: IMcpApi>() {}

#[test]
fn server_implements_mcp_provider() {
    assert_provider::<HttpMcpApi>();
}
