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
use sigbot_types::modules::strategy::strategy::StrategyInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct StrategyInfoPostgresRepository {
    inner: PostgresRepository<StrategyInfo>,
}

impl StrategyInfoPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(StrategyInfoPostgresRepository {
            inner: PostgresRepository::new(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<StrategyInfo> for StrategyInfoPostgresRepository {
    async fn select(
        &self,
        strategy: StrategyInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<StrategyInfo>), Error> {
        let result = dynamic_postgres_query!(
            strategy,
            "t_strategy",
            self.inner.get_pool(),
            "updated_time",
            page,
            StrategyInfo
        )?;
        info!("query strategys: {:?}", result);
        Ok((result.0, result.1))
    }

    async fn select_by_id(&self, id: i64) -> Result<StrategyInfo, Error> {
        let strategy = sqlx::query_as::<_, StrategyInfo>("SELECT * FROM t_strategy WHERE id = $1 and del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        info!("query strategy: {:?}", strategy);
        Ok(strategy)
    }

    async fn insert(&self, mut strategy: StrategyInfo) -> Result<i64, Error> {
        let inserted_id = dynamic_postgres_insert!(strategy, "strategys", self.inner.get_pool())?;
        info!("Inserted strategy.id: {:?}", inserted_id);
        Ok(inserted_id)
    }

    async fn update(&self, mut strategy: StrategyInfo) -> Result<i64, Error> {
        let updated_id = dynamic_postgres_update!(strategy, "t_strategy", self.inner.get_pool())?;
        info!("Updated strategy.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_strategy")
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM t_strategy WHERE id = $1 and del_flag = 0")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
