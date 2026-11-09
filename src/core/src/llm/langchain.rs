// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

use super::llm::ILlm;
use crate::config::config::get_config;
use anyhow::Ok;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::debug;
use langchain_rust::embedding::openai::OpenAiEmbedder;
use langchain_rust::language_models::options::CallOptions;
use langchain_rust::llm::OpenAI;
use langchain_rust::llm::OpenAIConfig;
use langchain_rust::schemas::Document;
use langchain_rust::schemas::FunctionCallBehavior;
use langchain_rust::vectorstore::pgvector::StoreBuilder;
use langchain_rust::vectorstore::VecStoreOptions;
use langchain_rust::vectorstore::VectorStore;
use sigbot_types::llm::knowledge::KnowledgeCategory;
use sigbot_types::llm::knowledge::KnowledgeStatus;
use sigbot_types::llm::knowledge::KnowledgeUploadInfo;
use sigbot_types::llm::LLMProvider;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

/// see:https://github.com/wl4g-ai/langchain-rust/blob/main/examples/conversational_retriever_chain_with_vector_store.rs
pub struct LangchainLlm {
    pgvec_store: Arc<Box<dyn VectorStore>>,
    openai_llm: OpenAI<OpenAIConfig>,
}

impl LangchainLlm {
    pub async fn new() -> Result<Arc<Self>> {
        let config = get_config();
        let llm_config = &config.llm;
        let mut embedding_openai_config = OpenAIConfig::new().with_api_base(&config.llm.embedding.api_uri);
        if let Some(api_key) = &config.llm.embedding.api_key {
            // Default used by 'OPENAI_KEY' and 'OPENAI_BASE_URL'.
            // Not require API key to run model by Ollama default.
            embedding_openai_config = embedding_openai_config.with_api_key(api_key);
        }
        if let Some(org_id) = &config.llm.embedding.org_id {
            embedding_openai_config = embedding_openai_config.with_org_id(org_id);
        }
        if let Some(project_id) = &config.llm.embedding.project_id {
            embedding_openai_config = embedding_openai_config.with_org_id(project_id);
        }

        let vecdb_config = &config.llm.vecdb;
        // Add connect_timeout parameter to connection URL for connection timeout
        let pgconn_url = format!(
            "postgresql://{}:{}@{}:{}/{}?schema={}&connect_timeout=10",
            vecdb_config.pg_vector.username,
            vecdb_config.pg_vector.password.clone().unwrap_or_default(),
            vecdb_config.pg_vector.host,
            vecdb_config.pg_vector.port,
            vecdb_config.pg_vector.database,
            vecdb_config.pg_vector.schema,
        );

        debug!("Connecting to the configured PG vector database");

        // Create the knowledge vector store for PG vector.
        let pgvec_store = StoreBuilder::new()
            .embedder(OpenAiEmbedder::new(embedding_openai_config))
            .vector_dimensions(1536)
            .pre_delete_collection(false)
            // Note: connection_url and pool are mutually exclusive - if pool is provided, connection_url is ignored
            .pool(
                PgPoolOptions::new()
                    .acquire_timeout(Duration::from_secs(15))
                    .idle_timeout(Duration::from_secs(600))
                    .max_lifetime(Duration::from_secs(1800))
                    .connect(&pgconn_url)
                    .await?,
            )
            .build()
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;

        debug!("Connected to the PG vector database successfully.");

        // Create the call LLM config for openai compability.
        let mut call_openai_config = OpenAIConfig::new().with_api_base(&llm_config.generate.api_uri);
        if let Some(api_key) = &llm_config.generate.api_key {
            call_openai_config = call_openai_config.with_api_key(api_key);
        }
        if let Some(org_id) = &llm_config.generate.org_id {
            call_openai_config = call_openai_config.with_org_id(org_id);
        }
        if let Some(project_id) = &llm_config.generate.project_id {
            call_openai_config = call_openai_config.with_org_id(project_id);
        }

        // Create the call LLM client for openai compability.
        let call_opts = CallOptions::new()
            .with_max_tokens(config.llm.generate.max_tokens)
            .with_temperature(config.llm.generate.temperature)
            .with_candidate_count(config.llm.generate.candidate_count)
            // TODO: whether the support configuration of this items?
            .with_functions(Vec::new())
            .with_stop_words(Vec::new())
            .with_top_k(config.llm.generate.top_k)
            .with_top_p(config.llm.generate.top_p)
            // .with_seed(0)
            .with_function_call_behavior(FunctionCallBehavior::Auto);
        let openai_llm = OpenAI::new(call_openai_config)
            .with_model(config.llm.generate.model.clone())
            .with_options(call_opts);

        Ok(Arc::new(Self {
            pgvec_store: Arc::new(Box::new(pgvec_store)),
            openai_llm,
        }))
    }
}

#[async_trait]
impl IProvider for LangchainLlm {
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ILlm for LangchainLlm {
    fn provider(&self) -> LLMProvider {
        LLMProvider::LANGCHAIN
    }

    async fn embedding(&self, mut info: KnowledgeUploadInfo, file: File) -> Result<KnowledgeUploadInfo, anyhow::Error> {
        info.status = KnowledgeStatus::RECEIVED;

        // TODO: Update to upload table.
        // ...

        info.status = KnowledgeStatus::PERSISTING;
        // TODO: Upload to Object Storage for backup raw file
        // ...

        info.status = KnowledgeStatus::PREPARING;
        // TODO: Update to upload table.
        // ...

        // Parse file into documents
        let reader = BufReader::new(file);
        let mut documents = Vec::new();

        for (line_num, line_result) in reader.lines().enumerate() {
            if let std::result::Result::Ok(content) = line_result {
                if content.trim().is_empty() {
                    continue;
                }

                // Create metadata for sample document.
                let mut metadata = HashMap::new();
                metadata.insert("filename".to_string(), info.name.clone().into());
                metadata.insert("linenum".to_string(), line_num.to_string().into());

                // Addidtion the user-provided labels.
                for (key, value) in &info.labels {
                    metadata.insert(key.clone(), value.clone().into());
                }

                documents.push(Document::new(&content).with_metadata(metadata));
            }
        }

        let store_options = match info.category {
            KnowledgeCategory::NORMAL => {
                VecStoreOptions::new()
                    .with_name_space("NORMAL") // Normal requests (positive category samples)
                    .with_score_threshold(0.5_f32) // TODO: score threshold
            }
            KnowledgeCategory::MALICIOUS => {
                VecStoreOptions::new()
                    .with_name_space("MALICIOUS") // Maybe attack malicious requests (negative category sample)
                    .with_score_threshold(0.5_f32) // TODO: score threshold
            }
        };

        info.status = KnowledgeStatus::EMBEDDING;
        // TODO: Update to upload table.
        // ...

        match self.pgvec_store.add_documents(&documents, &store_options).await {
            std::result::Result::Ok(_) => {
                tracing::info!("Embedding success.");
                info.status = KnowledgeStatus::EMBEDDED;
                // TODO: update to upload table.
                // ...
            }
            Err(e) => {
                tracing::error!("Embedding failed: {}", e);
                info.status = KnowledgeStatus::FAILED;
                // TODO: update to upload table.
                // ...
            }
        }

        Ok(info)
    }

    async fn generate(&self, prompt: String) -> Result<String, anyhow::Error> {
        use langchain_rust::language_models::llm::LLM;
        anyhow::ensure!(!prompt.trim().is_empty(), "LLM prompt is empty");
        Ok(self.openai_llm.invoke(&prompt).await?)
    }
}
