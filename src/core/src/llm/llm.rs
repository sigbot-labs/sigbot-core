// SPDX-License-Identifier: GPL-3.0-only

use crate::llm::langchain::LangchainLlm;
use anyhow::Result;
use async_trait::async_trait;
use common_context::{ApplicationContext, IProvider};
use sigbot_types::llm::knowledge::KnowledgeUploadInfo;
use sigbot_types::llm::LLMProvider;
use std::fs::File;
use std::sync::Arc;

#[async_trait]
pub trait ILlm: IProvider {
    fn provider(&self) -> LLMProvider;
    async fn embedding(&self, info: KnowledgeUploadInfo, file: File) -> Result<KnowledgeUploadInfo>;
    async fn generate(&self, prompt: String) -> Result<String>;
}

/// Registers and resolves LLM providers through the process ApplicationContext.
pub struct LlmApplication;

impl LlmApplication {
    pub async fn register_default() -> Result<Arc<dyn ILlm>> {
        let instance_id = LLMProvider::LANGCHAIN.as_str();
        let context = ApplicationContext::global();
        context.register_if_absent::<dyn ILlm, _>(instance_id, || async {
            Ok(LangchainLlm::new().await? as Arc<dyn ILlm>)
        })?;
        context.get::<dyn ILlm>(instance_id).await
    }

    pub async fn get(provider: &str) -> Result<Arc<dyn ILlm>> {
        ApplicationContext::global().get::<dyn ILlm>(provider).await
    }

    pub async fn get_default() -> Result<Arc<dyn ILlm>> {
        Self::get(LLMProvider::LANGCHAIN.as_str()).await
    }
}
