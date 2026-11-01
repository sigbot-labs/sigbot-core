// SPDX-License-Identifier: GPL-3.0-only

use crate::datafeed::IDatafeed;
use crate::datafeed::PolicyDatafeed;
use crate::social::social::FeedHandler;
use common_context::IProvider;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use sigbot_types::datafeed::DatafeedArgument;
use std::sync::Arc;

pub struct FederalReserveDatafeed {
    policy: Arc<PolicyDatafeed>,
}
impl FederalReserveDatafeed {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            policy: PolicyDatafeed::new(DatafeedProvider::Fed),
        })
    }
}
#[async_trait::async_trait]
impl IDatafeed for FederalReserveDatafeed {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::Fed
    }
    async fn configure(&self, argument: Arc<DatafeedArgument>) -> anyhow::Result<()> {
        self.policy.configure(argument).await
    }
    async fn subscribe(&self, handler: FeedHandler) -> anyhow::Result<()> {
        self.policy.subscribe(handler).await
    }
}
#[async_trait::async_trait]
impl IProvider for FederalReserveDatafeed {
    async fn close(&self) -> anyhow::Result<()> {
        IProvider::close(self.policy.as_ref()).await
    }
}
