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
use sigbot_types::sys::log::{AppendLogRequest, LogInfo, SearchLogRequest, StatsLogRequest, TailLogRequest};
use sigbot_types::{PageRequest, PageResponse};

#[async_trait]
pub trait ILogHandler: Send {
    async fn append(&self, param: AppendLogRequest) -> Result<i64, Error>;
    async fn tail(&self, param: TailLogRequest) -> Result<Vec<LogInfo>, Error>;
    async fn search(&self, param: SearchLogRequest, page: PageRequest) -> Result<(PageResponse, Vec<LogInfo>), Error>;
    async fn stats(&self, param: StatsLogRequest) -> Result<sigbot_types::sys::log::LogStats, Error>;
}

pub struct LogHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> LogHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> ILogHandler for LogHandler<'a> {
    #[audit_log("[LOG][APPEND] service_name: {param.service_name:?}")]
    async fn append(&self, param: AppendLogRequest) -> Result<i64, Error> {
        let repo = self.state.log_repo.lock().await;
        repo.get(&self.state.config.appdb).append(param.to_log()).await
    }

    async fn tail(&self, param: TailLogRequest) -> Result<Vec<LogInfo>, Error> {
        let repo = self.state.log_repo.lock().await;
        repo.get(&self.state.config.appdb)
            .tail(
                param.service_name,
                param.log_type,
                param.keyword,
                param.limit,
                param.since,
            )
            .await
    }

    async fn search(&self, param: SearchLogRequest, page: PageRequest) -> Result<(PageResponse, Vec<LogInfo>), Error> {
        let repo = self.state.log_repo.lock().await;
        repo.get(&self.state.config.appdb)
            .search(
                param.service_name,
                param.log_type,
                param.level,
                param.source,
                param.tags,
                param.keyword,
                param.start_time,
                param.end_time,
                page,
            )
            .await
    }

    async fn stats(&self, param: StatsLogRequest) -> Result<sigbot_types::sys::log::LogStats, Error> {
        let repo = self.state.log_repo.lock().await;
        repo.get(&self.state.config.appdb)
            .stats(param.service_name, param.log_type, param.start_time, param.end_time)
            .await
    }
}
