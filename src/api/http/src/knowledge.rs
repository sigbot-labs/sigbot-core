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

use axum::extract::Multipart;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Json;
use axum::Router;
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_core::llm::llm::LlmApplication;
use sigbot_types::llm::knowledge::KnowledgeUploadInfo;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

pub struct KnowledgeHttpApi;

impl KnowledgeHttpApi {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/knowledge/upload", post(Self::upload))
    }

    #[allow(unused_variables)]
    async fn upload(State(state): State<SigbotState>, mut multipart: Multipart) -> impl IntoResponse {
        // Create temp directory for uploaded files
        let config = get_config();
        let temp_dir_path = PathBuf::from(&config.services.api.upload_temp_dir);

        if let Err(e) = fs::create_dir_all(&temp_dir_path) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create temporary directory: {}", e),
            )
                .into_response();
        }

        // Extract file and metadata from multipart form
        let mut file_path = None;
        let mut knowledge_data = None;

        // Loop through each field in the multipart form.
        while let Ok(Some(field)) = multipart.next_field().await {
            match field.name() {
                Some("file") => {
                    // Process file upload
                    let file_name = field.file_name().unwrap_or("unknown").to_string();
                    let unique_filename = format!("{}-{}", Uuid::now_v7(), file_name);
                    let file_path_str = temp_dir_path.join(&unique_filename).to_string_lossy().to_string();

                    match field.bytes().await {
                        Ok(data) => {
                            if let Err(e) = File::create(&file_path_str).and_then(|mut f| f.write_all(&data)) {
                                return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to save file: {}", e))
                                    .into_response();
                            }
                            file_path = Some(file_path_str);
                        }
                        Err(e) => {
                            return (StatusCode::BAD_REQUEST, format!("Failed to read file: {}", e)).into_response()
                        }
                    }
                }
                Some("metadata") => {
                    // Process metadata
                    match field.text().await {
                        Ok(text) => {
                            knowledge_data = match serde_json::from_str::<KnowledgeUploadInfo>(&text) {
                                Ok(data) => Some(data),
                                Err(e) => {
                                    return (StatusCode::BAD_REQUEST, format!("Invalid metadata: {}", e))
                                        .into_response()
                                }
                            };
                        }
                        Err(e) => {
                            return (StatusCode::BAD_REQUEST, format!("Failed to read metadata: {}", e)).into_response()
                        }
                    }
                }
                _ => continue,
            }
        }

        // Validate required data presence
        let file_path = match file_path {
            Some(path) => path,
            None => return (StatusCode::BAD_REQUEST, "No file uploaded".to_string()).into_response(),
        };
        let knowledge_info = match knowledge_data {
            Some(data) => data,
            None => return (StatusCode::BAD_REQUEST, "No metadata provided".to_string()).into_response(),
        };

        // Create cleanup guard for temp file
        let _cleanup_guard = CleanupGuard::new(&file_path);

        // Process file content and create documents
        let file = match File::open(&file_path) {
            Ok(file) => file,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to open temp file: {}", e),
                )
                    .into_response()
            }
        };

        // Store documents to Vector DB.
        let llm = match LlmApplication::get_default().await {
            Ok(llm) => llm,
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("LLM provider unavailable: {error}"),
                )
                    .into_response()
            }
        };
        match &llm.embedding(knowledge_info, file).await {
            Ok(info) => {
                let response = serde_json::json!({
                    "id": &info.id,
                    "name": &info.name,
                    "status": info.status,
                    "lines": info.lines,
                });
                (StatusCode::OK, Json(response)).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to store documents: {}", e),
            )
                .into_response(),
        }
    }
}

// Helper struct to ensure temp file cleanup
struct CleanupGuard<'a> {
    path: &'a str,
}

impl<'a> CleanupGuard<'a> {
    fn new(path: &'a str) -> Self {
        Self { path }
    }
}

impl<'a> Drop for CleanupGuard<'a> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path);
    }
}
