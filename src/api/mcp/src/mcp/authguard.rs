// SPDX-License-Identifier: GPL-3.0-only

use async_trait::async_trait;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, HeaderValue, Method};
use rust_mcp_sdk::auth::{AuthInfo, AuthProvider, AuthenticationError, OauthEndpoint};
use rust_mcp_sdk::mcp_http::http;
use rust_mcp_sdk::mcp_http::{GenericBody, GenericBodyExt, McpAppState};
use rust_mcp_sdk::mcp_server::error::TransportServerError;
use sha2::{Digest, Sha256};
use sigbot_core::context::authguard::AuthguardProvider;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct AuthguardMcpAuth {
    authguard: Arc<AuthguardProvider>,
}

impl AuthguardMcpAuth {
    pub fn new(authguard: Arc<AuthguardProvider>) -> Self {
        Self { authguard }
    }
}

#[async_trait]
impl AuthProvider for AuthguardMcpAuth {
    async fn verify_token(&self, access_token: String) -> Result<AuthInfo, AuthenticationError> {
        let mut headers = HeaderMap::new();
        let context = HeaderValue::from_str(&access_token).map_err(|_| AuthenticationError::InvalidToken {
            description: "invalid Authguard context",
        })?;
        headers.insert("x-authguard-context", context.clone());
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {access_token}")).map_err(|_| {
                AuthenticationError::InvalidToken {
                    description: "invalid Authguard bearer token",
                }
            })?,
        );
        let identity = self
            .authguard
            .authenticate(&headers, &Method::POST, "/mcp")
            .await
            .map_err(|_| AuthenticationError::InvalidToken {
                description: "invalid Authguard context",
            })?;
        let token_unique_id = format!("{:x}", Sha256::digest(access_token.as_bytes()));
        Ok(AuthInfo {
            token_unique_id,
            client_id: Some(identity.principal_id().to_owned()),
            user_id: Some(identity.principal_id().to_owned()),
            scopes: Some(vec![identity.access.action.clone()]),
            // The signed context was validated immediately above. A deliberately
            // short SDK lifetime prevents a transport session from extending it.
            expires_at: Some(SystemTime::now() + Duration::from_secs(1)),
            audience: None,
            extra: Some(serde_json::Map::from_iter([(
                "tenant_id".to_owned(),
                serde_json::Value::String(identity.tenant_id().to_owned()),
            )])),
        })
    }

    fn auth_endpoints(&self) -> Option<&HashMap<String, OauthEndpoint>> {
        None
    }

    async fn handle_request(
        &self,
        _request: http::Request<&str>,
        _state: Arc<McpAppState>,
    ) -> Result<http::Response<GenericBody>, TransportServerError> {
        Ok(GenericBody::create_404_response())
    }

    fn protected_resource_metadata_url(&self) -> Option<&str> {
        None
    }
}
