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
use crate::store::postgres::PostgresRepository;
use crate::store::AsyncRepository;
use anyhow::Context;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use sigbot_types::sys::dlock::DLock;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

pub struct DLockPostgresRepository {
    inner: PostgresRepository<DLock>,
}

impl DLockPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(DLockPostgresRepository {
            inner: PostgresRepository::new(config).await?,
        })
    }

    // TODO: We can further enhance the implementation as follows:
    // 1. Add a `version` field to enable repeated lock acquisition;
    // 2. Add a `renew` function to allow the lock holder to renew the lock before its expiration, based on business needs.
    pub async fn acquire(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;
        let timeout_ms = dlock.timeout.context("timeout is required")?.as_millis() as i64;

        let mut tx = self.inner.get_pool().begin().await?;

        // 1. Assuming the distributed lock (ID or name) has no holder, then the update will definitely succeed (affected=1),
        //    meaning the current attempt to acquired the distributed lock.
        // 2. If another holder crashes after acquiring the lock, a timeout period must be elapsed before allowing another
        //    holder to acquire the lock.
        let acquire_sql = r#"
            UPDATE sys_dlock SET status = 1, updated_time = CURRENT_TIMESTAMP(13), holder = ?
            WHERE del_flag = 0 AND (
                ((id = ? OR name = ?) AND status = 0)
                OR (updated_time < ? - INTERVAL '? milliseconds')
            )
        "#;
        let update_result = sqlx::query(&acquire_sql)
            .bind(&holder)
            .bind(dlock.base.id.context("id is required")?)
            .bind(&name)
            .bind(timeout_ms)
            .execute(&mut *tx)
            .await?;
        if update_result.rows_affected() > 0 {
            tx.commit().await?;
            Ok(1) // Acquired the distributed lock.
        } else {
            // 1. Assuming the first acquire lock, then it should be insert a new locked record.
            let insert_sql = r#"
                INSERT INTO sys_dlock (name, status, timeout, holder, created_time, updated_time)
                VALUES (?, 1, ?, ?, CURRENT_TIMESTAMP(13), CURRENT_TIMESTAMP(13))
                ON CONFLICT (id, name) DO NOTHING
            "#;
            let insert_result = sqlx::query(&insert_sql)
                .bind(&name)
                .bind(timeout_ms)
                .bind(&holder)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?; // Commit transaction, return error if commit fails
            if insert_result.rows_affected() > 0 {
                return Ok(1); // Acquired the distributed lock after successful commit.
            } else {
                Ok(0) // Failed to acquire the distributed lock.
            }
        }
    }

    pub async fn release(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;

        let mut tx = self.inner.get_pool().begin().await?;

        // Release to unlock for current holder only.
        let unlock_sql = r#"
            UPDATE sys_dlock SET status = 0, updated_time = CURRENT_TIMESTAMP(13)
            WHERE del_flag = 0 AND holder = ? AND (
                ((id = ? OR name = ?) AND status = 1)
            )
        "#;
        let update_result = sqlx::query(&unlock_sql)
            .bind(&holder)
            .bind(dlock.base.id.context("id is required")?)
            .bind(&name)
            .execute(&mut *tx)
            .await?;
        if update_result.rows_affected() > 0 {
            tx.commit().await?;
            Ok(1) // Released the distributed lock.
        } else {
            Ok(0) // Failed to release the distributed lock.
        }
    }
}

#[async_trait]
impl AsyncRepository<DLock> for DLockPostgresRepository {
    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn select(&self, dlock: DLock, page: PageRequest) -> Result<(PageResponse, Vec<DLock>), Error> {
        unimplemented!("Unsupported operation: select for DLock");
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn select_by_id(&self, id: i64) -> Result<DLock, Error> {
        unimplemented!("Unsupported operation: select_by_id for DLock");
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn insert(&self, dlock: DLock) -> Result<i64, Error> {
        Ok(self.acquire(dlock).await?) // Specifically logically, insert is equivalent to acquire.
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn update(&self, dlock: DLock) -> Result<i64, Error> {
        Ok(self.release(dlock).await?) // Specifically logically, update is equivalent to release.
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn delete_all(&self) -> Result<u64, Error> {
        unimplemented!("Unsupported operation: delete_all for DLock")
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        unimplemented!("Unsupported operation: delete_by_id for DLock")
    }
}
