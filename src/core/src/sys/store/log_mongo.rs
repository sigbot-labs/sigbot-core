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
use crate::store::IAsyncRepository;
use crate::sys::store::ILogRepository;
use anyhow::Error;
use async_trait::async_trait;
use chrono::Utc;
use common_telemetry::info;
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::sys::log::LogInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::collections::HashMap;
use std::sync::Arc;

pub struct LogMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<LogInfo>>,
    collection: Collection<LogInfo>,
}

impl LogMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("sys_log");
        Ok(LogMongoRepository { inner, collection })
    }
}

#[async_trait]
impl IAsyncRepository<LogInfo> for LogMongoRepository {
    async fn select(&self, _: LogInfo, _: PageRequest) -> Result<(PageResponse, Vec<LogInfo>), Error> {
        Err(Error::msg("Log select are not supported"))
    }

    async fn select_by_id(&self, _: i64) -> Result<LogInfo, Error> {
        Err(Error::msg("Log select_by_id are not supported"))
    }

    async fn upsert(&self, _: LogInfo) -> Result<i64, Error> {
        Err(Error::msg("Log insert are not supported"))
    }

    async fn update(&self, _log: LogInfo) -> Result<i64, Error> {
        Err(Error::msg("Log updates are not supported"))
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        Err(Error::msg("Log delete_all are not supported"))
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        Err(Error::msg("Log delete_by_id are not supported"))
    }
}

#[async_trait]
impl ILogRepository for LogMongoRepository {
    async fn append(&self, mut log: LogInfo) -> Result<i64, Error> {
        // Logs are append-only, use insert instead of upsert
        use crate::util::auths::SecurityContext;
        let insert_by = SecurityContext::get_instance().get_current_uname_for_store().await;
        let id = log.base.pre_insert(insert_by).await;

        let result = self.collection.insert_one(&log).await?;

        // MongoDB returns ObjectId, but we use i64 for id
        // The id is already set by pre_insert, so we return it
        if let Some(_inserted_id) = result.inserted_id.as_object_id() {
            info!("Inserted log.id: {}", id);
            Ok(id)
        } else {
            // Fallback: return the id from pre_insert
            info!("Inserted log.id: {} (MongoDB ObjectId not available)", id);
            Ok(id)
        }
    }

    async fn tail(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        keyword: Option<String>,
        limit: Option<u32>,
        since: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<LogInfo>, Error> {
        let limit = limit.unwrap_or(100);
        let mut filter = doc! { "del_flag": 0 };

        if let Some(ref sn) = service_name {
            filter.insert("service_name", sn);
        }
        if let Some(ref lt) = log_type {
            filter.insert("log_type", lt);
        }
        if let Some(ref kw) = keyword {
            filter.insert("content", doc! { "$regex": kw, "$options": "i" });
        }
        if let Some(ref ts) = since {
            filter.insert(
                "created_at",
                doc! { "$gt": mongodb::bson::DateTime::from_millis(ts.timestamp_millis()) },
            );
        }

        let limit_i64 = limit as i64;
        let sort = doc! { "created_at": -1 };

        let cursor = self.collection.find(filter).limit(limit_i64).sort(sort).await?;
        let logs: Vec<LogInfo> = cursor.try_collect().await?;
        Ok(logs)
    }

    async fn search(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        level: Option<String>,
        source: Option<String>,
        tags: Option<String>,
        keyword: Option<String>,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<LogInfo>), Error> {
        let mut filter = doc! { "del_flag": 0 };

        if let Some(ref sn) = service_name {
            filter.insert("service_name", sn);
        }
        if let Some(ref lt) = log_type {
            filter.insert("log_type", lt);
        }
        if let Some(ref lvl) = level {
            filter.insert("level", lvl);
        }
        if let Some(ref src) = source {
            filter.insert("source", src);
        }
        if let Some(ref tgs) = tags {
            filter.insert("tags", doc! { "$regex": tgs, "$options": "i" });
        }
        if let Some(ref kw) = keyword {
            filter.insert("content", doc! { "$regex": kw, "$options": "i" });
        }
        if let Some(ref st) = start_time {
            filter.insert(
                "created_at",
                doc! { "$gte": mongodb::bson::DateTime::from_millis(st.timestamp_millis()) },
            );
        }
        if let Some(ref et) = end_time {
            filter.insert(
                "created_at",
                doc! { "$lte": mongodb::bson::DateTime::from_millis(et.timestamp_millis()) },
            );
        }

        let total_count = self.collection.count_documents(filter.clone()).await? as i64;

        let options = mongodb::options::FindOptions::builder()
            .skip(page.get_offset() as u64)
            .limit(page.get_limit() as i64)
            .sort(doc! { "created_at": -1 })
            .build();

        let cursor = self
            .collection
            .find(filter)
            .skip(options.skip.unwrap_or(0))
            .limit(options.limit.unwrap_or(100))
            .sort(options.sort.unwrap_or_default())
            .await?;
        let logs: Vec<LogInfo> = cursor.try_collect().await?;

        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        Ok((page_response, logs))
    }

    async fn stats(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
    ) -> Result<sigbot_types::sys::log::LogStats, Error> {
        let mut filter = doc! { "del_flag": 0 };

        if let Some(ref sn) = service_name {
            filter.insert("service_name", sn);
        }
        if let Some(ref lt) = log_type {
            filter.insert("log_type", lt);
        }
        if let Some(ref st) = start_time {
            filter.insert(
                "created_at",
                doc! { "$gte": mongodb::bson::DateTime::from_millis(st.timestamp_millis()) },
            );
        }
        if let Some(ref et) = end_time {
            filter.insert(
                "created_at",
                doc! { "$lte": mongodb::bson::DateTime::from_millis(et.timestamp_millis()) },
            );
        }

        let total_count = self.collection.count_documents(filter.clone()).await? as i64;

        // Stats by level
        let mut by_level = HashMap::new();
        let level_pipeline = vec![
            doc! { "$match": filter.clone() },
            doc! { "$match": doc! { "level": doc! { "$ne": mongodb::bson::Bson::Null } } },
            doc! { "$group": doc! { "_id": "$level", "count": doc! { "$sum": 1 } } },
        ];
        let mut level_cursor = self.collection.aggregate(level_pipeline).await?;
        while let Some(doc_result) = level_cursor.try_next().await? {
            let level_opt = doc_result.get_str("_id").ok();
            let count_opt = doc_result.get_i64("count").ok();
            if let (Some(level), Some(count)) = (level_opt, count_opt) {
                by_level.insert(level.to_string(), count);
            }
        }

        // Stats by log_type
        let mut by_log_type = HashMap::new();
        let log_type_pipeline = vec![
            doc! { "$match": filter },
            doc! { "$match": doc! { "log_type": doc! { "$ne": mongodb::bson::Bson::Null } } },
            doc! { "$group": doc! { "_id": "$log_type", "count": doc! { "$sum": 1 } } },
        ];
        let mut log_type_cursor = self.collection.aggregate(log_type_pipeline).await?;
        while let Some(doc_result) = log_type_cursor.try_next().await? {
            let log_type_opt = doc_result.get_str("_id").ok();
            let count_opt = doc_result.get_i64("count").ok();
            if let (Some(log_type), Some(count)) = (log_type_opt, count_opt) {
                by_log_type.insert(log_type.to_string(), count);
            }
        }

        Ok(sigbot_types::sys::log::LogStats {
            total_count,
            by_level,
            by_log_type,
        })
    }
}
