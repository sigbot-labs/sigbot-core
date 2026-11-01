use chrono::Duration;
use chrono::Utc;
use sigbot_datafeed::datafeed::PolicyNewsSource;
use sigbot_types::datafeed::datafeed::DatafeedProvider;

struct PolicyFixture;
impl PolicyFixture {
    fn rss(date: &str, guid: &str, link: &str) -> String {
        format!(
            r#"<rss version="2.0"><channel><title>Explicit synthetic parser fixture</title>
        <link>https://example.test</link><description>Contract only</description><item>
        <title>Rate announcement</title><description>Liquidity &amp; rates</description>
        {guid}{link}{date}</item></channel></rss>"#
        )
    }
    async fn official(provider: DatafeedProvider) -> anyhow::Result<()> {
        let endpoint = PolicyNewsSource::default_endpoint(&provider)?;
        let source = PolicyNewsSource::new(
            &provider,
            None,
            std::time::Duration::from_secs(300),
            "00-11111111111111111111111111111111-2222222222222222-01".to_owned(),
        )?;
        let events = source.fetch().await?;
        assert!(!events.is_empty(), "official feed has no actual dated entries");
        assert!(events
            .iter()
            .all(|event| event.source == provider.as_str().to_ascii_lowercase()
                && event.event_time <= event.available_time
                && event.revision == 0));
        assert!(endpoint.starts_with("https://"));
        Ok(())
    }
}

#[test]
fn policy_parser_rejects_future_missing_dates_and_unstable_identity() {
    let now = Utc::now();
    let future = format!("<pubDate>{}</pubDate>", (now + Duration::days(1)).to_rfc2822());
    let past = format!("<pubDate>{}</pubDate>", (now - Duration::days(1)).to_rfc2822());
    for xml in [
        PolicyFixture::rss(&future, "<guid>one</guid>", ""),
        PolicyFixture::rss("", "<guid>one</guid>", ""),
        PolicyFixture::rss(&past, "", ""),
    ] {
        assert!(PolicyNewsSource::parse("fed", "https://example.test/rss", xml.as_bytes(), now).is_err());
    }
}

#[test]
fn policy_parser_preserves_source_identity_and_distinguishes_collection_time() {
    let now = Utc::now();
    let published = now - Duration::days(1);
    let xml = PolicyFixture::rss(
        &format!("<pubDate>{}</pubDate>", published.to_rfc2822()),
        "",
        "<link>https://example.test/policy/one</link>",
    );
    let one = PolicyNewsSource::parse("fed", "https://example.test/rss", xml.as_bytes(), now)
        .unwrap()
        .remove(0);
    let two = PolicyNewsSource::parse(
        "fed",
        "https://example.test/rss",
        xml.as_bytes(),
        now + Duration::seconds(1),
    )
    .unwrap()
    .remove(0);
    assert_eq!(one.source_event_id, "https://example.test/policy/one");
    assert_eq!(one.source_event_id, two.source_event_id);
    assert_eq!(one.seq, two.seq);
    assert_eq!(one.raw, two.raw);
    assert_ne!(one.available_time, two.available_time);
    assert!(one.content.unwrap().contains("Liquidity & rates"));
}

#[test]
fn policy_atom_uses_explicit_updated_time_and_has_bounded_input() {
    let xml = br#"<feed xmlns="http://www.w3.org/2005/Atom"><title>Fixture</title><id>feed</id>
        <updated>2026-10-27T01:00:00Z</updated><entry><id>policy:1</id><title>Decision</title>
        <updated>2026-10-27T01:00:00Z</updated><content type="text">Published decision</content></entry></feed>"#;
    let event = PolicyNewsSource::parse("treasury", "https://example.test/rss", xml, Utc::now())
        .unwrap()
        .remove(0);
    assert_eq!(event.event_time.to_rfc3339(), "2026-10-27T01:00:00+00:00");
    assert!(PolicyNewsSource::parse(
        "fed",
        "https://example.test/rss",
        &vec![b'x'; 2 * 1024 * 1024 + 1],
        Utc::now()
    )
    .is_err());
}

#[tokio::test]
#[ignore = "requires actual public Federal Reserve HTTPS; failures are not skipped"]
async fn official_federal_reserve_feed_is_read_only_and_dated() -> anyhow::Result<()> {
    PolicyFixture::official(DatafeedProvider::Fed).await
}

#[tokio::test]
#[ignore = "requires actual public TreasuryDirect HTTPS; failures are not skipped"]
async fn official_treasury_feed_is_read_only_and_dated() -> anyhow::Result<()> {
    PolicyFixture::official(DatafeedProvider::Treasury).await
}
