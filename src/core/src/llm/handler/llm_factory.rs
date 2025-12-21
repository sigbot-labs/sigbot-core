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

use crate::llm::handler::llm_langchain::LangchainOperation;
use anyhow::Error;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::llm::{knowledge::KnowledgeUploadInfo, LLMProvider};
use std::{
    collections::HashMap,
    fs::File,
    sync::{self, Arc, RwLock},
};

#[async_trait::async_trait]
pub trait ILLMOperation {
    fn provider(&self) -> LLMProvider;
    async fn init(&self);
    async fn close(&self);
    async fn embedding(&self, mut info: KnowledgeUploadInfo, file: File) -> Result<KnowledgeUploadInfo, anyhow::Error>;
    async fn generate(&self, prompt: String) -> Result<String, anyhow::Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotLLMFactory> = RwLock::new(SigbotLLMFactory::new());
}

pub struct SigbotLLMFactory {
    pub implementations: HashMap<String, sync::Arc<dyn ILLMOperation + Send + Sync>>,
}

impl SigbotLLMFactory {
    fn new() -> Self {
        SigbotLLMFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotLLMFactory> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        info!("Initializing LangChain LLM operation ...");
        match Self::get()
            .write() // If acquire fails, then it block until acquired.
            .unwrap() // If acquire fails, then it should panic.
            .register0(LLMProvider::LANGCHAIN.as_str(), LangchainOperation::new().await)
        {
            Ok(registered) => {
                info!("Initialized LangChain LLM operation successfully.");
                let _ = registered.init().await;
            }
            Err(e) => panic!("Failed to initialize LangChain LLM operation. - {}", e),
        }
    }

    fn register0<T: ILLMOperation + Send + Sync + 'static>(
        &mut self,
        provider: &str,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        // Check if the name already exists
        if self.implementations.contains_key(provider) {
            debug!("Already register the LLM operation with provider: '{}'.", provider);
            return Ok(handler);
        }
        self.implementations.insert(provider.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub fn get_implementation(provider: &str) -> Result<Arc<dyn ILLMOperation + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotLLMFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(provider) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not get LLM operation with provider: '{}'.", provider);
            return Err(Error::msg(errmsg));
        }
    }

    pub fn get_default() -> Arc<dyn ILLMOperation + Send + Sync> {
        Self::get_implementation(LLMProvider::LANGCHAIN.as_str()).expect("Failed to get default LLM handler")
    }

    pub async fn close() {
        let this = SigbotLLMFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}
