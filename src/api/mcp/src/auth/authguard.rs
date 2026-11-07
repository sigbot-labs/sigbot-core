// SPDX-License-Identifier: GPL-3.0-only

use async_trait::async_trait;
use axum::http::Method;
use rust_mcp_sdk::auth::{AuthInfo, AuthenticationError, OauthTokenVerifier};
use sha2::{Digest, Sha256};
use sigbot_core::context::authguard::AuthguardProvider;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Supplies the official RemoteAuthProvider with verified Authguard identity and grants.
pub struct AuthguardMcpAuth {
    authguard: Arc<AuthguardProvider>,
    resource_path: String,
}

impl AuthguardMcpAuth {
    pub fn new(authguard: Arc<AuthguardProvider>, resource_path: String) -> Self {
        Self {
            authguard,
            resource_path,
        }
    }
}

#[async_trait]
impl OauthTokenVerifier for AuthguardMcpAuth {
    async fn verify_token(&self, access_token: String) -> Result<AuthInfo, AuthenticationError> {
        let identity = self
            .authguard
            .authenticate_bearer(&access_token, &Method::POST, &self.resource_path)
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
            extra: Some(serde_json::Map::from_iter([
                ("request_access".to_owned(), identity.to_verified_value()),
                (
                    "tenant_id".to_owned(),
                    serde_json::Value::String(identity.tenant_id().to_owned()),
                ),
            ])),
        })
    }
}
