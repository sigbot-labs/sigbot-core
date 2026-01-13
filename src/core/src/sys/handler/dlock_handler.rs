// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::context::state::SigbotState;
use anyhow::Error;
use async_trait::async_trait;
use common_audit_log::audit_log;
use lazy_static::lazy_static;
use sigbot_types::sys::dlock::DLockInfo;
use sigbot_types::EntityBase;
use std::sync::Arc;
use std::time::Duration;

lazy_static! {
    pub static ref PID: String =
        std::env::var("POD_NAME").unwrap_or(std::env::var("HOSTNAME").unwrap_or(uuid::Uuid::new_v4().to_string()));
}

#[async_trait]
pub trait IDLockHandler: Send + Sync {
    async fn acquire(&self, name: String, timeout: Duration) -> Result<bool, Error>;
    async fn release(&self, name: String) -> Result<bool, Error>;
}

pub struct DLockHandler {
    state: Arc<SigbotState>,
}

impl DLockHandler {
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl IDLockHandler for DLockHandler {
    #[audit_log("[DLock][ACQUIRE] name: {name}, timeout: {timeout.as_millis()}")]
    async fn acquire(&self, name: String, timeout: Duration) -> Result<bool, Error> {
        // Genearte the holder by current pod id and process id and tokio coroutine id.
        let holder = format!("{}:{}:{}", PID.as_str(), std::process::id(), tokio::task::id());
        let repo = self.state.lock_repo.lock().await;
        let result = repo
            .get(&self.state.config)
            // Actually it call to acquire func.
            .upsert(DLockInfo {
                base: EntityBase::new_empty(),
                name: Some(name),
                holder: Some(holder.to_string()),
                timeout: Some(timeout),
            })
            .await?;
        if result > 0 {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[audit_log("[DLock][RELEASE] name: {name}")]
    async fn release(&self, name: String) -> Result<bool, Error> {
        let holder = "";
        let repo = self.state.lock_repo.lock().await;
        let result = repo
            .get(&self.state.config)
            .update(DLockInfo {
                // Actually it call to release func.
                base: EntityBase::new_empty(),
                name: Some(name),
                holder: Some(holder.to_string()),
                timeout: None,
            })
            .await?;
        if result > 0 {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
