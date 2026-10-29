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
use crate::dynamic_mongo_query;
use crate::store::mongo::MongoRepository;
use crate::store::IAsyncRepository;
use anyhow::Error;
use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::Collection;
use sigbot_types::modules::workflow::workflow::WorkflowInfo;
use sigbot_types::{PageRequest, PageResponse};
use std::sync::Arc;

pub struct WorkflowInfoMongoRepository {
    #[allow(unused)]
    inner: Arc<MongoRepository<WorkflowInfo>>,
    collection: Collection<WorkflowInfo>,
}

impl WorkflowInfoMongoRepository {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let inner = Arc::new(MongoRepository::new(config).await?);
        let collection = inner.get_database().collection("s_workflow");
        collection
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! {"id": 1})
                    .options(mongodb::options::IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await?;
        Ok(WorkflowInfoMongoRepository { inner, collection })
    }
}

#[async_trait]
impl IAsyncRepository<WorkflowInfo> for WorkflowInfoMongoRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        anyhow::ensure!(workflow.tenant_id.is_some(), "explicit workflow tenant scope required");
        match dynamic_mongo_query!(workflow, self.collection, "updated_at", page, WorkflowInfo) {
            Ok(result) => Ok((result.0, result.1)),
            Err(error) => Err(error),
        }
    }

    async fn select_by_id(&self, _id: i64) -> Result<WorkflowInfo, Error> {
        Err(Error::msg("use select with explicit workflow tenant scope"))
    }

    async fn upsert(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        anyhow::ensure!(
            workflow.tenant_id.is_some()
                && workflow.space_id.is_some()
                && workflow.env.as_deref().is_some_and(|env| env != "legacy"),
            "bound workflow scope required"
        );
        if workflow.base.id.is_none() {
            let actor = workflow.base.created_by.clone();
            workflow.base.pre_insert(actor).await;
        }
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        let filter = doc! {"id": id, "tenant_id": workflow.tenant_id, "space_id": workflow.space_id, "env": &workflow.env, "del_flag": 0};
        self.collection.replace_one(filter, workflow).upsert(true).await?;
        Ok(id)
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<i64, Error> {
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.space_id.is_some() && workflow.env.is_some(),
            "bound workflow scope required"
        );
        let deleted = workflow.base.del_flag;
        let actor = workflow.base.updated_by.clone();
        workflow.base.pre_update(actor).await;
        workflow.base.del_flag = deleted;
        let mut filter = doc! {"id": id, "tenant_id": workflow.tenant_id, "space_id": workflow.space_id, "env": &workflow.env, "del_flag": 0};
        if workflow
            .status
            .as_ref()
            .is_some_and(|status| matches!(status.as_str(), "STOPPED" | "SUCCESS" | "FAILED"))
        {
            filter.insert("status", workflow.status.as_ref().expect("checked status").as_str());
        }
        let changed = self.collection.replace_one(filter, workflow).await?;
        anyhow::ensure!(changed.matched_count == 1, "workflow missing or deleted");
        Ok(id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        Err(Error::msg("unscoped workflow deletion is disabled"))
    }

    async fn delete_by_id(&self, _id: i64) -> Result<u64, Error> {
        Err(Error::msg("use a scoped workflow update to mark deletion"))
    }
}
