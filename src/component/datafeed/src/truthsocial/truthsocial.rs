// SPDX-License-Identifier: GPL-3.0-only

use crate::datafeed::IDatafeed;
use crate::social::social::FeedHandler;
use crate::social::social::SocialFeed;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use sigbot_core::config::config::DatafeedProperties;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use std::sync::Arc;

pub struct TruthSocialDatafeed {
    feed: SocialFeed,
}

impl TruthSocialDatafeed {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            feed: SocialFeed::new("truthsocial"),
        })
    }
}

#[async_trait]
impl IDatafeed for TruthSocialDatafeed {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::TruthSocial
    }
    async fn configure(&self, argument: Arc<DatafeedProperties>) -> Result<()> {
        self.feed.configure(argument).await
    }

    async fn subscribe(&self, handler: FeedHandler) -> Result<()> {
        self.feed.subscribe(handler).await
    }
}

#[async_trait::async_trait]
impl IProvider for TruthSocialDatafeed {
    async fn close(&self) -> anyhow::Result<()> {
        self.feed.close().await;

        Ok(())
    }
}
