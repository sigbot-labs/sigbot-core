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

use crate::config::config::PostgresAppDBProperties;
use crate::dynamic_postgres_insert;
use crate::dynamic_postgres_query;
use crate::dynamic_postgres_update;
use crate::store::postgres::PostgresRepository;
use crate::store::AsyncRepository;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::datafeed::datafeed::DatafeedInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct DatafeedInfoPostgresRepository {
    inner: PostgresRepository<DatafeedInfo>,
}

impl DatafeedInfoPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(DatafeedInfoPostgresRepository {
            inner: PostgresRepository::new(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<DatafeedInfo> for DatafeedInfoPostgresRepository {
    async fn select(
        &self,
        datafeed: DatafeedInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<DatafeedInfo>), Error> {
        let result = dynamic_postgres_query!(
            datafeed,
            "t_datafeed",
            self.inner.get_pool(),
            "updated_time",
            page,
            DatafeedInfo
        )?;
        info!("query datafeeds: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<DatafeedInfo, Error> {
        let datafeed = sqlx::query_as::<_, DatafeedInfo>("SELECT * FROM t_datafeed WHERE id = $1 and del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        info!("query datafeed: {:?}", datafeed);
        Ok(datafeed)
    }

    async fn insert(&self, mut datafeed: DatafeedInfo) -> Result<i64, Error> {
        let inserted_id = dynamic_postgres_insert!(datafeed, "datafeeds", self.inner.get_pool())?;
        info!("Inserted datafeed.id: {:?}", inserted_id);
        Ok(inserted_id)
    }

    async fn update(&self, mut datafeed: DatafeedInfo) -> Result<i64, Error> {
        let updated_id = dynamic_postgres_update!(datafeed, "t_datafeed", self.inner.get_pool())?;
        info!("Updated datafeed.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_datafeed")
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_datafeed WHERE id = $1 and del_flag = 0")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
