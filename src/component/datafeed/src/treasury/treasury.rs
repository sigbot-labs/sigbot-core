// SPDX-License-Identifier: GPL-3.0-only

use crate::datafeed::IDatafeed;
use crate::datafeed::PolicyDatafeed;
use crate::social::social::FeedHandler;
use common_context::IProvider;
use sigbot_core::config::config::DatafeedProperties;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use std::sync::Arc;

pub struct TreasuryDatafeed {
    policy: Arc<PolicyDatafeed>,
}
impl TreasuryDatafeed {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            policy: PolicyDatafeed::new(DatafeedProvider::Treasury),
        })
    }
}
#[async_trait::async_trait]
impl IDatafeed for TreasuryDatafeed {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::Treasury
    }
    async fn configure(&self, argument: Arc<DatafeedProperties>) -> anyhow::Result<()> {
        self.policy.configure(argument).await
    }
    async fn subscribe(&self, handler: FeedHandler) -> anyhow::Result<()> {
        self.policy.subscribe(handler).await
    }
}
#[async_trait::async_trait]
impl IProvider for TreasuryDatafeed {
    async fn close(&self) -> anyhow::Result<()> {
        IProvider::close(self.policy.as_ref()).await
    }
}
