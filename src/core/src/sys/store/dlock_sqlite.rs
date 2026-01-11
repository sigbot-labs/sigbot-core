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
use crate::store::sqlite::SQLiteRepository;
use crate::store::AsyncRepository;
use anyhow::Context;
use anyhow::{Error, Ok};
use async_trait::async_trait;
use sigbot_types::sys::dlock::DLock;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use sigbot_utils::dash_maps::ConcurrentMap;
use std::time::SystemTime;

pub struct DLockSQLiteRepository {
    inner: SQLiteRepository<DLock>,
    initializer: ConcurrentMap<String, u64>,
}

impl DLockSQLiteRepository {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        Ok(DLockSQLiteRepository {
            inner: SQLiteRepository::get_or_init(config).await?,
            initializer: ConcurrentMap::new(),
        })
    }

    // TODO: We can further enhance the implementation as follows:
    // 1. Add a `version` field to enable repeated lock acquisition;
    // 2. Add a `renew` function to allow the lock holder to renew the lock before its expiration, based on business needs.
    pub async fn acquire(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;
        let timeout_ms = dlock.timeout.context("timeout is required")?.as_millis() as i64;

        let name0 = name.clone();
        let holder0 = holder.clone();
        self.initializer
            .compute_if_absent(name.to_owned(), || async move {
                // 1. Assuming the first acquire lock, then it should be insert a new locked record.
                // SQLite supports ON CONFLICT, but INSERT OR IGNORE is more commonly used and compatible
                let insert_sql = r#"
                    INSERT OR IGNORE INTO sys_dlock (name, status, timeout, holder, created_at, updated_at)
                    VALUES (?, 1, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                "#;
                let mut tx = self
                    .inner
                    .get_pool()
                    .begin()
                    .await
                    .expect("Failed to begin transaction.");
                sqlx::query(&insert_sql)
                    .bind(&name0)
                    .bind(timeout_ms)
                    .bind(&holder0)
                    .execute(&mut *tx)
                    .await
                    // Panic if failed to acquire the distributed lock due to error for business safety (force consistency).
                    .expect("Failed to acquire the distributed lock due to error.");
                tx.commit().await.expect("Failed to commit transaction.");

                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as u64
            })
            .await;

        let mut tx = self.inner.get_pool().begin().await?;

        // Calculate the timeout timestamp in SQLite format
        // SQLite datetime function: datetime('now', '-123 milliseconds') but SQLite doesn't support milliseconds directly
        // So we convert milliseconds to seconds (with fractional seconds)
        let timeout_seconds = timeout_ms as f64 / 1000.0;
        // Format timeout_seconds as a string for SQLite datetime function
        let timeout_seconds_str = format!("{:.3}", timeout_seconds);

        // 1. Assuming the distributed lock (ID or name) has no holder, then the update will definitely succeed (affected=1),
        //    meaning the current attempt to acquired the distributed lock.
        // 2. If another holder crashes after acquiring the lock, a timeout period must be elapsed before allowing another
        //    holder to acquire the lock.
        // Note: SQLite datetime function syntax: datetime(updated_at, '+' || '123.456' || ' seconds')
        // We need to embed the timeout value directly in the SQL string since parameter binding doesn't work for datetime modifiers
        let acquire_sql = format!(
            r#"
            UPDATE sys_dlock SET status = 1, updated_at = CURRENT_TIMESTAMP, holder = ?
            WHERE del_flag = 0 AND (
                ((id = ? OR name = ?) AND status = 0)
                OR (datetime(updated_at, '+' || '{}' || ' seconds') < datetime('now'))
            )
            "#,
            timeout_seconds_str
        );
        let update_result = sqlx::query(&acquire_sql)
            .bind(&holder)
            .bind(dlock.base.id.context("id is required")?)
            .bind(&name)
            .execute(&mut *tx)
            .await?;
        if update_result.rows_affected() > 0 {
            tx.commit().await?;
            Ok(1) // Acquired the distributed lock.
        } else {
            Ok(0) // Failed to acquire the distributed lock.
        }
    }

    pub async fn release(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;

        let mut tx = self.inner.get_pool().begin().await?;

        // Release to unlock for current holder only.
        let unlock_sql = r#"
            UPDATE sys_dlock SET status = 0, updated_at = CURRENT_TIMESTAMP
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
impl AsyncRepository<DLock> for DLockSQLiteRepository {
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
