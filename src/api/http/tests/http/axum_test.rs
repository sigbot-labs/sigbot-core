use sigbot_http::http::axum::AxumHttpApi;
use sigbot_http::http::IHttpApi;

fn assert_http_provider<T: IHttpApi>() {}

#[test]
fn axum_api_implements_http_provider() {
    assert_http_provider::<AxumHttpApi>();
}
