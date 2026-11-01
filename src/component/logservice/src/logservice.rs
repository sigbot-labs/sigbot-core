// SPDX-License-Identifier: GPL-3.0-only

use async_trait::async_trait;
use common_context::IProvider;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;

use crate::s3::s3::LogLine;

#[async_trait]
pub trait ILogservice: IProvider {
    /// The acknowledgement boundary is the durable local WAL commit.
    async fn append(&self, message: &DurableMessage<LogLine>) -> anyhow::Result<bool>;

    /// Copies durable WAL records to immutable object storage and indexes them.
    async fn archive_pending(&self, limit: u32) -> anyhow::Result<usize>;

    async fn read(
        &self,
        stream: &DurableMessage<Value>,
        after_cursor: i64,
        limit: u32,
    ) -> anyhow::Result<Vec<(i64, DurableMessage<LogLine>)>>;
}
