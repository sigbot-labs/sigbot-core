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

use super::config::PostgresPropertiesBase;
use serde::Deserialize;
use serde::Serialize;
use std::ops::Deref;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct LlmProperties {
    #[serde(default = "VectorDBProperties::default")]
    pub vecdb: VectorDBProperties,
    // see:https://platform.openai.com/docs/guides/completions
    // see:https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-completion
    // see:https://help.aliyun.com/zh/model-studio/getting-started/what-is-model-studio#16693d2e3fmir
    #[serde(rename = "embedding")]
    pub embedding: EmbeddingLLMProperties,
    #[serde(rename = "generate")]
    pub generate: GenerateLLMProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct VectorDBProperties {
    #[serde(rename = "type")]
    pub db_type: VectorDBType,
    #[serde(default = "PgVectorDBProperties::default")]
    pub pg_vector: PgVectorDBProperties,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum VectorDBType {
    PgVector,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PgVectorDBProperties {
    #[serde(flatten)]
    pub inner: PostgresPropertiesBase,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingLLMProperties {
    pub api_uri: String,
    pub api_key: Option<String>,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
    #[serde(rename = "model")]
    pub model: String,
    pub pre_delete_collection: bool,
    pub vector_dimensions: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct GenerateLLMProperties {
    pub api_uri: String,
    pub api_key: Option<String>,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
    #[serde(rename = "model")]
    pub model: String,
    pub max_tokens: u32,
    #[serde(rename = "temperature")]
    pub temperature: f32,
    pub candidate_count: usize,
    pub top_k: usize,
    pub top_p: f32,
    pub system_prompt: String,
}

// Vector DB Properties impls.

impl Default for VectorDBProperties {
    fn default() -> Self {
        VectorDBProperties {
            db_type: VectorDBType::PgVector,
            pg_vector: PgVectorDBProperties::default(),
        }
    }
}

impl Deref for PgVectorDBProperties {
    type Target = PostgresPropertiesBase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for EmbeddingLLMProperties {
    fn default() -> Self {
        EmbeddingLLMProperties {
            api_uri: String::from("https://dashscope.aliyuncs.com/compatible-mode/v1"),
            api_key: None,
            org_id: None,
            project_id: None,
            model: String::from("bge-m3:latest"),
            pre_delete_collection: false,
            vector_dimensions: 1536,
        }
    }
}

impl Default for GenerateLLMProperties {
    fn default() -> Self {
        GenerateLLMProperties {
            api_uri: String::from("https://dashscope.aliyuncs.com/compatible-mode/v1"),
            api_key: None,
            org_id: None,
            project_id: None,
            model: String::from("qwen-plus"),
            max_tokens: 65535,
            candidate_count: 1,
            temperature: 0.1,
            top_k: 1,
            top_p: 1.0,
            system_prompt: String::from(
                "You are a security expert.\n\
                 You are given a list of rules and a request.\n\
                 You must determine if the request is safe or not.\n\
                 If the request is safe, you must return \"safe\".\n\
                 If the request is not safe, you must return \"unsafe\" and provide a reason.\n\
                 You must also provide a list of rules that were used to determine the result.\n\
                 You must also provide a list of rules that were not used to determine the result.",
            ),
        }
    }
}
