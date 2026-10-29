use chrono::{TimeZone, Utc};
use serde_json::json;
use sigbot_datafeed::client::news::social_feed::NewsNormalizer;

#[test]
fn source_availability_cannot_precede_the_event() {
    let available = Utc.with_ymd_and_hms(2026, 10, 28, 9, 0, 0).unwrap();
    let post = json!({"id":"123", "created_at":"2026-10-28T09:00:01Z", "text":"future"});
    assert!(NewsNormalizer::from_post("twitter", post, available).is_err());
}

#[test]
fn truth_html_is_normalized_without_losing_the_source_fact() {
    let available = Utc.with_ymd_and_hms(2026, 10, 28, 9, 0, 0).unwrap();
    let post = json!({"id":"123", "created_at":"2026-10-28T08:59:59Z", "content":"<p>Rate &amp; liquidity</p>"});
    let event = NewsNormalizer::from_post("truthsocial", post.clone(), available).unwrap();
    assert_eq!(event.raw, post);
    assert_eq!(event.content.as_deref(), Some("Rate & liquidity"));
    assert_eq!(event.available_time, available);
}
