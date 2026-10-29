// SPDX-License-Identifier: GPL-3.0-only
use super::social_feed::{FeedHandler, SocialFeed};
use crate::client::datafeed_factory::ISigbotDatafeedClient;
use anyhow::Result;
use async_trait::async_trait;
use sigbot_types::modules::datafeed::{datafeed::DatafeedProvider, SigbotDatefeedArgument};
use std::sync::Arc;

pub struct SigbotTwitterDatafeedClient {
    feed: SocialFeed,
}

impl SigbotTwitterDatafeedClient {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            feed: SocialFeed::new("twitter"),
        })
    }
}

#[async_trait]
impl ISigbotDatafeedClient for SigbotTwitterDatafeedClient {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::TWITTER
    }
    async fn init(&self, argument: Arc<SigbotDatefeedArgument>) -> Result<()> {
        self.feed.init(argument).await
    }
    async fn close(&self) {
        self.feed.close().await;
    }
    async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        self.feed.subscribe(handler).await
    }
}
