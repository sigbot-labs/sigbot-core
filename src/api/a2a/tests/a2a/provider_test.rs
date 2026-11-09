use sigbot_a2a::a2a::IA2aApi;
use sigbot_a2a::ResourceA2aApi;

fn assert_provider<T: IA2aApi>() {}

#[test]
fn workflow_api_implements_a2a_provider() {
    assert_provider::<ResourceA2aApi>();
}
