// SPDX-License-Identifier: GPL-3.0-only

use crate::datafeed::IDatafeed;
use crate::social::social::FeedHandler;
use crate::social::social::SocialFeed;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use sigbot_types::datafeed::DatafeedArgument;
use std::sync::Arc;

pub struct TwitterDatafeed {
    feed: SocialFeed,
}

impl TwitterDatafeed {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            feed: SocialFeed::new("twitter"),
        })
    }
}

#[async_trait]
impl IDatafeed for TwitterDatafeed {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::Twitter
    }
    async fn configure(&self, argument: Arc<DatafeedArgument>) -> Result<()> {
        self.feed.configure(argument).await
    }

    async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        self.feed.subscribe(handler).await
    }
}

#[async_trait::async_trait]
impl IProvider for TwitterDatafeed {
    async fn close(&self) -> anyhow::Result<()> {
        self.feed.close().await;

        Ok(())
    }
}
