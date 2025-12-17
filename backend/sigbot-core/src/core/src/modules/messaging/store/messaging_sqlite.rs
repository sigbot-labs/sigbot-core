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

use crate::config::config::SqliteAppDBProperties;
use crate::dynamic_sqlite_insert;
use crate::dynamic_sqlite_query;
use crate::dynamic_sqlite_update;
use crate::store::sqlite::SQLiteRepository;
use crate::store::AsyncRepository;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::messaging::messaging::MessagingInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct MessagingInfoSQLiteRepository {
    inner: SQLiteRepository<MessagingInfo>,
}

impl MessagingInfoSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(MessagingInfoSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<MessagingInfo> for MessagingInfoSQLiteRepository {
    async fn select(
        &self,
        messaging: MessagingInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<MessagingInfo>), Error> {
        let result = dynamic_sqlite_query!(
            messaging,
            "t_messaging",
            self.inner.get_pool(),
            "updated_at",
            page,
            MessagingInfo
        )?;

        info!("query messagings: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<MessagingInfo, Error> {
        let messaging = sqlx::query_as::<_, MessagingInfo>("SELECT * FROM t_messaging WHERE id = $1 and del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        info!("query messaging: {:?}", messaging);
        Ok(messaging)
    }

    async fn insert(&self, mut messaging: MessagingInfo) -> Result<i64, Error> {
        let inserted_id = dynamic_sqlite_insert!(messaging, "t_messaging", self.inner.get_pool())?;
        info!("Inserted messaging.id: {:?}", inserted_id);
        Ok(inserted_id)
    }

    async fn update(&self, mut messaging: MessagingInfo) -> Result<i64, Error> {
        let updated_id = dynamic_sqlite_update!(messaging, "t_messaging", self.inner.get_pool())?;
        info!("Updated messaging.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_messaging")
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_messaging WHERE id = $1 and del_flag = 0")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
