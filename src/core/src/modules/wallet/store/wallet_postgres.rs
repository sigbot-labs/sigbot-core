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
use crate::dynamic_postgres_query;
use crate::dynamic_postgres_update;
use crate::dynamic_postgres_upsert;
use crate::store::postgres::PostgresRepository;
use crate::store::IAsyncRepository;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::wallet::wallet::WalletInfo;
use sigbot_types::{PageRequest, PageResponse};

pub struct WalletInfoPostgresRepository {
    inner: PostgresRepository<WalletInfo>,
}

impl WalletInfoPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(WalletInfoPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl IAsyncRepository<WalletInfo> for WalletInfoPostgresRepository {
    async fn select(&self, wallet: WalletInfo, page: PageRequest) -> Result<(PageResponse, Vec<WalletInfo>), Error> {
        let result = dynamic_postgres_query!(
            wallet,
            "s_wallets",
            self.inner.get_pool(),
            "updated_at",
            page,
            WalletInfo
        )
        .map_err(|e: sqlx::Error| Error::from(e))?;
        info!("query wallets: {:?}", result);
        Ok(result)
    }

    async fn select_by_id(&self, id: i64) -> Result<WalletInfo, Error> {
        let wallet = sqlx::query_as::<_, WalletInfo>("SELECT * FROM s_wallets WHERE id = $1 AND del_flag = FALSE")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await
            .context("Failed to select wallet by id")?;
        info!("query wallet: {:?}", wallet);
        Ok(wallet)
    }

    async fn upsert(&self, mut wallet: WalletInfo) -> Result<i64, Error> {
        let upserted_id = dynamic_postgres_upsert!(wallet, "s_wallets", self.inner.get_pool())?;
        info!("Inserted wallet.id: {:?}", upserted_id);
        Ok(upserted_id)
    }

    async fn update(&self, mut wallet: WalletInfo) -> Result<i64, Error> {
        let updated_id = dynamic_postgres_update!(wallet, "s_wallets", self.inner.get_pool())?;
        info!("Updated wallet.id: {:?}", updated_id);
        Ok(updated_id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_wallets SET del_flag = TRUE")
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("UPDATE s_wallets SET del_flag = TRUE WHERE id = $1 AND del_flag = FALSE")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;
        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
