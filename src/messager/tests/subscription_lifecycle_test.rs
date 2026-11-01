use common_context::IProvider;
use sigbot_messager::local::local::LocalMessager;
use sigbot_messager::local::local::LocalMessagerConfig;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::SubscriptionSet;
use std::sync::Arc;

#[tokio::test]
async fn named_consumers_share_a_topic_and_unsubscribe_independently() -> anyhow::Result<()> {
    let client = LocalMessager::new(Arc::new(LocalMessagerConfig { queue_size: 8 })).await;
    client.init().await?;
    let first = SubscriptionSet::default();
    let second = SubscriptionSet::default();
    first.bind(client.clone())?;
    second.bind(client.clone())?;
    let (first_sender, mut first_receiver) = tokio::sync::mpsc::channel(4);
    let (second_sender, mut second_receiver) = tokio::sync::mpsc::channel(4);
    first
        .subscribe(
            "topic".into(),
            "first".into(),
            Arc::new(move |bytes| {
                let sender = first_sender.clone();
                Box::pin(async move {
                    sender.send(bytes).await?;
                    Ok("first".into())
                })
            }),
        )
        .await?;
    second
        .subscribe(
            "topic".into(),
            "second".into(),
            Arc::new(move |bytes| {
                let sender = second_sender.clone();
                Box::pin(async move {
                    sender.send(bytes).await?;
                    Ok("second".into())
                })
            }),
        )
        .await?;
    client.publish("topic", "one").await?;
    assert_eq!(first_receiver.recv().await.as_deref(), Some(b"one".as_slice()));
    assert_eq!(second_receiver.recv().await.as_deref(), Some(b"one".as_slice()));
    first.close().await?;
    client.publish("topic", "two").await?;
    assert_eq!(second_receiver.recv().await.as_deref(), Some(b"two".as_slice()));
    assert!(!matches!(
        tokio::time::timeout(std::time::Duration::from_millis(20), first_receiver.recv()).await,
        Ok(Some(_))
    ));
    second.close().await?;
    client.close().await?;
    Ok(())
}
