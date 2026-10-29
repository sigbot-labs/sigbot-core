use sigbot_datafeed::client::news::social_transport::{SocialTransport, SocialTransportMode};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires the real external boundary services; run through the Marketpulse runner"]
async fn real_rest_mcp_a2a_transport_equivalence() -> anyhow::Result<()> {
    let token = std::env::var("MARKETPULSE_MOCK_TOKEN")?;
    let twitter = std::env::var("MARKETPULSE_TWITTER_URL")?;
    let truth = std::env::var("MARKETPULSE_TRUTHSOCIAL_URL")?;
    let http = reqwest::Client::new();
    let fixture: serde_json::Value = http
        .get(format!("{truth}/api/v1/accounts/100000000000001/statuses"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let facts: Vec<_> = (0..43)
        .map(|index| {
            let mut fact = fixture[0].clone();
            fact["id"] = format!("{}{:02}", chrono::Utc::now().timestamp_millis(), index).into();
            fact["content"] = "<p>Synthetic pagination contract observation.</p>".into();
            fact
        })
        .collect();
    http.post(format!("{truth}/fixtures"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"data": facts}))
        .send()
        .await?
        .error_for_status()?;
    for (source, endpoint, account) in [
        ("twitter", twitter, "2244994945"),
        ("truthsocial", truth, "100000000000001"),
    ] {
        let mut expected = None;
        for mode in [
            SocialTransportMode::Rest,
            SocialTransportMode::Mcp,
            SocialTransportMode::A2a,
        ] {
            let transport = SocialTransport::new(
                endpoint.clone(),
                mode,
                account.to_owned(),
                source.to_owned(),
                token.clone(),
                Duration::from_secs(15),
            )?;
            let mut cursor = "0".to_owned();
            let mut ids = Vec::new();
            let mut pages = 0;
            loop {
                pages += 1;
                assert!(pages <= 100, "unbounded pagination");
                let value = transport
                    .fetch(
                        "0",
                        &cursor,
                        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                        "protocol-check",
                    )
                    .await?;
                let posts = value
                    .as_array()
                    .or_else(|| value.get("data").and_then(serde_json::Value::as_array))
                    .unwrap();
                ids.extend(posts.iter().map(|post| post["id"].as_str().unwrap().to_owned()));
                let next = value
                    .get("next_token")
                    .or_else(|| value.pointer("/meta/next_token"))
                    .and_then(serde_json::Value::as_str);
                match next {
                    Some(next) => {
                        assert_ne!(next, cursor);
                        cursor = next.to_owned();
                    }
                    None => break,
                }
            }
            assert!(!ids.is_empty());
            if source == "truthsocial" {
                assert!(ids.len() >= 46, "must cross the native 40-status page boundary");
            }
            if let Some(expected) = &expected {
                assert_eq!(expected, &ids);
            } else {
                expected = Some(ids);
            }
        }
    }
    Ok(())
}
