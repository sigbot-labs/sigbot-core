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
use sigbot_types::sys::user::UserInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct UserSQLiteRepository {
    inner: SQLiteRepository<UserInfo>,
}

impl UserSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(UserSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
        })
    }
}

#[async_trait]
impl AsyncRepository<UserInfo> for UserSQLiteRepository {
    async fn select(&self, user: UserInfo, page: PageRequest) -> Result<(PageResponse, Vec<UserInfo>), Error> {
        let result = dynamic_sqlite_query!(user, "sys_user", self.inner.get_pool(), "updated_at", page, UserInfo)?;

        info!("query users: {:?}", result);
        Ok((result.0, result.1))

        // sqlx
        //   ::query_as::<_, User>("SELECT * FROM sys_user LIMIT $1 OFFSET $2")
        //   .bind(page.get_offset())
        //   .bind(page.get_limit())
        //   .fetch_all(self.inner.get_pool()).await
        //   .map_err(|e| {
        //      info!("Error to select all: {:?}", e);
        //      Error::msg(e.to_string())
        //   })
    }

    async fn select_by_id(&self, id: i64) -> Result<UserInfo, Error> {
        let user = sqlx::query_as::<_, UserInfo>("SELECT * FROM sys_user WHERE id = $1 and del_flag = 0")
            .bind(id)
            .fetch_one(self.inner.get_pool())
            .await?;

        info!("query user: {:?}", user);
        Ok(user)
    }

    async fn insert(&self, mut user: UserInfo) -> Result<i64, Error> {
        let inserted_id = dynamic_sqlite_insert!(user, "sys_user", self.inner.get_pool())?;
        info!("Inserted user.id: {:?}", inserted_id);
        Ok(inserted_id)

        //  let result = sqlx
        //   ::query(
        //     r#"
        //     INSERT INTO sys_user (id, name, email, password, created_by, created_at, updated_by, updated_at, del_flag)
        //      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        //     "#
        //   )
        //   .bind(user.base.id)
        //   .bind(user.name)
        //   .bind(user.email)
        //   .bind(user.phone)
        //   .bind(user.password) // TODO persistent encrypt password
        //   .bind(user.base.created_by)
        //   .bind(user.base.created_at)
        //   .bind(user.base.updated_by)
        //   .bind(user.base.updated_at)
        //   .bind(user.base.del_flag)
        //   .execute(self.inner.get_pool()).await
        //   ?;
        // info!("Inserted result: {:?}, user.id: {:?}", result, id);

        // Ok(id)
    }

    async fn update(&self, mut user: UserInfo) -> Result<i64, Error> {
        let updated_id = dynamic_sqlite_update!(user, "sys_user", self.inner.get_pool())?;
        info!("Updated user.id: {:?}", updated_id);
        Ok(updated_id)

        // let id = param.base.id.ok_or_else(|| Error::msg("User id is required for update"))?;
        // let update_result = sqlx
        //   ::query("UPDATE sys_user SET name = $1, email = $2 WHERE id = $3")
        //   .bind(param.name)
        //   .bind(param.email)
        //   .bind(id)
        //   .execute(self.inner.get_pool()).await
        //   ?;
        // info!("updated result: {:?}", update_result);
        // Ok(update_result.rows_affected() as i64)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM sys_user")
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        let delete_result = sqlx::query("DELETE FROM sys_user WHERE id = $1 and del_flag = 0")
            .bind(id)
            .execute(self.inner.get_pool())
            .await?;

        info!("Deleted result: {:?}", delete_result);
        Ok(delete_result.rows_affected())
    }
}
