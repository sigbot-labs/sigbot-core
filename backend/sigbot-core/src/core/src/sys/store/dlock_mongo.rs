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

use crate::config::config::MongoAppDBProperties;
use crate::store::mongo::MongoRepository;
use crate::store::AsyncRepository;
use anyhow::Context;
use anyhow::Error;
use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::sys::dlock::DLock;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use std::sync::Arc;

pub struct DLockMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<DLock>>,
    collection: Collection<DLock>,
}

impl DLockMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("sys_dlock");
        Ok(DLockMongoRepository { inner, collection })
    }

    // TODO: We can further enhance the implementation as follows:
    // 1. Add a `version` field to enable repeated lock acquisition;
    // 2. Add a `renew` function to allow the lock holder to renew the lock before its expiration, based on business needs.
    pub async fn acquire(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;
        let timeout_ms = dlock.timeout.context("timeout is required")?.as_millis() as i64;
        let id = dlock.base.id.context("id is required")?;

        // 1. Try to update existing lock: either it's unlocked (status = 0) or it has timed out
        // 2. If another holder crashes after acquiring the lock, a timeout period must be elapsed before allowing another
        //    holder to acquire the lock.
        // Use MongoDB server time ($expr with $$NOW) to avoid time synchronization issues
        let filter = doc! {
            "del_flag": { "$ne": 1 },
            "$or": [
                {
                    "$and": [
                        { "$or": [{ "id": id }, { "name": &name }] },
                        { "status": 0 }
                    ]
                },
                {
                    "$expr": {
                        "$lt": [
                            "$updated_time",
                            {
                                "$subtract": [
                                    "$$NOW",
                                    timeout_ms
                                ]
                            }
                        ]
                    }
                }
            ]
        };

        let update = doc! {
            "$set": {
                "status": 1,
                "holder": &holder
            },
            "$currentDate": {
                "updated_time": true
            }
        };

        let update_result = self.collection.update_one(filter, update).await?;

        if update_result.modified_count > 0 {
            Ok(1) // Acquired the distributed lock.
        } else {
            // 2. If no existing lock found, try to insert a new lock record
            // Create DLock instance without timestamps, then use MongoDB's $currentDate to set server time
            use std::time::Duration;

            let new_lock = DLock {
                base: sigbot_types::EntityBase {
                    id: Some(id),
                    status: Some(1),
                    created_by: None,
                    created_time: None, // Will be set by MongoDB $currentDate
                    updated_by: None,
                    updated_time: None, // Will be set by MongoDB $currentDate
                    del_flag: Some(0),
                },
                name: Some(name.clone()),
                holder: Some(holder.clone()),
                timeout: Some(Duration::from_millis(timeout_ms as u64)),
            };

            // First try to insert, MongoDB will handle duplicate key errors
            match self.collection.insert_one(&new_lock).await {
                Ok(_) => {
                    // After successful insert, update with MongoDB server time using $currentDate
                    let time_filter = doc! {
                        "$or": [
                            { "id": id },
                            { "name": &name }
                        ]
                    };
                    let time_update = doc! {
                        "$currentDate": {
                            "created_time": true,
                            "updated_time": true
                        }
                    };
                    // This update should always succeed since we just inserted
                    let _ = self.collection.update_one(time_filter, time_update).await;
                    Ok(1) // Acquired the distributed lock after successful insert.
                }
                Err(e) => {
                    // If duplicate key error, it means another process already acquired the lock
                    let error_str = e.to_string();
                    if error_str.contains("duplicate") || error_str.contains("E11000") {
                        Ok(0) // Failed to acquire the distributed lock (race condition).
                    } else {
                        Err(Error::from(e)) // Other errors should be propagated
                    }
                }
            }
        }
    }

    pub async fn release(&self, dlock: DLock) -> Result<i64, Error> {
        let name = dlock.name.context("name is required")?;
        let holder = dlock.holder.context("holder is required")?;
        let id = dlock.base.id.context("id is required")?;

        // Release to unlock for current holder only.
        let filter = doc! {
            "del_flag": { "$ne": 1 },
            "holder": &holder,
            "$or": [
                { "id": id },
                { "name": &name }
            ],
            "status": 1
        };

        let update = doc! {
            "$set": {
                "status": 0
            },
            "$currentDate": {
                "updated_time": true
            }
        };

        let update_result = self.collection.update_one(filter, update).await?;

        if update_result.modified_count > 0 {
            Ok(1) // Released the distributed lock.
        } else {
            Ok(0) // Failed to release the distributed lock.
        }
    }
}

#[async_trait]
impl AsyncRepository<DLock> for DLockMongoRepository {
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
