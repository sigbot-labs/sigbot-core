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

use crate::context::state::SigbotState;
use anyhow::Error;
use async_trait::async_trait;
use common_audit_log::audit_log;
use sigbot_types::sys::tenant::{
    DeleteTenantRequest, QueryTenantRequest, SaveTenantRequest, SaveTenantResponse, Tenant, TenantEncryptionKeys,
};
use sigbot_types::{PageRequest, PageResponse};

#[async_trait]
pub trait ITenantHandler: Send {
    async fn find(&self, param: QueryTenantRequest, page: PageRequest) -> Result<(PageResponse, Vec<Tenant>), Error>;

    async fn save(&self, param: SaveTenantRequest) -> Result<SaveTenantResponse, Error>;

    async fn delete(&self, param: DeleteTenantRequest) -> Result<u64, Error>;
}

pub struct TenantHandler<'a> {
    state: &'a SigbotState,
}

impl<'a> TenantHandler<'a> {
    pub fn new(state: &'a SigbotState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<'a> ITenantHandler for TenantHandler<'a> {
    #[audit_log("[TENANT][FIND] name: {param.name.clone().unwrap_or_default()}")]
    async fn find(&self, param: QueryTenantRequest, page: PageRequest) -> Result<(PageResponse, Vec<Tenant>), Error> {
        let repo = self.state.tenant_repo.lock().await;
        repo.get(&self.state.config).select(param.to_tenant(), page).await
    }

    #[audit_log("[TENANT][ADD] name: {param.name.clone().unwrap_or_default()}")]
    async fn save(&self, param: SaveTenantRequest) -> Result<SaveTenantResponse, Error> {
        let repo = self.state.tenant_repo.lock().await;

        let mut tenant = param.to_tenant();
        let mut private_key_to_return: Option<String> = None;

        // Get system master key from environment or config
        // This key is used to encrypt tenant private keys when cloud-managed is enabled
        let system_master_key = std::env::var("SIGBOT_SYSTEM_MASTER_KEY").unwrap_or_else(|_| {
            // Fallback: use JWT secret as system master key if available
            self.state.config.auth.jwt_secret.clone().unwrap_or_else(|| {
                // Last resort: use a default (should be changed in production)
                "default-system-master-key-change-in-production".to_string()
            })
        });

        // Default to enable cloud-managed key to prevent user from losing private key
        let enable_cloud_managed = param.cloud_managed_key.unwrap_or(true);

        // Handle encryption keys for new tenants (when id is None)
        if param.id.is_none() {
            if let Some(user_provided_public_key) = param.encryption_public_key.as_ref() {
                // User provided their own key pair
                if let Some(user_provided_private_key) = param.encryption_private_key.as_ref() {
                    // Verify that private key matches public key
                    match TenantEncryptionKeys::from_private_key(user_provided_private_key) {
                        Ok(keys) => {
                            if keys.public_key != *user_provided_public_key {
                                return Err(Error::msg("Private key does not match public key"));
                            }
                            tenant.encryption_public_key = Some(keys.public_key);
                            tenant.encryption_private_key_hash =
                                Some(TenantEncryptionKeys::hash_private_key(user_provided_private_key));

                            // Always enable cloud-managed key by default (unless explicitly disabled)
                            if enable_cloud_managed {
                                match TenantEncryptionKeys::encrypt_private_key_with_system_key(
                                    user_provided_private_key,
                                    &system_master_key,
                                ) {
                                    Ok(encrypted) => {
                                        tenant.encrypted_private_key = Some(encrypted);
                                        tenant.cloud_managed_key = Some(true);
                                    }
                                    Err(e) => {
                                        return Err(Error::msg(format!(
                                            "Failed to encrypt private key for cloud management: {}",
                                            e
                                        )));
                                    }
                                }
                            } else {
                                tenant.cloud_managed_key = Some(false);
                                tenant.encrypted_private_key = None;
                            }
                            // Don't return private key if user provided it (they should have it)
                        }
                        Err(e) => {
                            return Err(Error::msg(format!("Invalid private key: {}", e)));
                        }
                    }
                } else {
                    // User provided public key but no private key - this is invalid
                    return Err(Error::msg("Private key must be provided when public key is provided"));
                }
            } else {
                // System generates key pair
                match TenantEncryptionKeys::generate() {
                    Ok(keys) => {
                        tenant.encryption_public_key = Some(keys.public_key.clone());
                        tenant.encryption_private_key_hash =
                            Some(TenantEncryptionKeys::hash_private_key(&keys.private_key));

                        // Always enable cloud-managed key by default (unless explicitly disabled)
                        if enable_cloud_managed {
                            match TenantEncryptionKeys::encrypt_private_key_with_system_key(
                                &keys.private_key,
                                &system_master_key,
                            ) {
                                Ok(encrypted) => {
                                    tenant.encrypted_private_key = Some(encrypted);
                                    tenant.cloud_managed_key = Some(true);
                                }
                                Err(e) => {
                                    return Err(Error::msg(format!(
                                        "Failed to encrypt private key for cloud management: {}",
                                        e
                                    )));
                                }
                            }
                        } else {
                            tenant.cloud_managed_key = Some(false);
                            tenant.encrypted_private_key = None;
                        }

                        // Return private key only once (for system-generated keys)
                        private_key_to_return = Some(keys.private_key);
                    }
                    Err(e) => {
                        return Err(Error::msg(format!("Failed to generate encryption keys: {}", e)));
                    }
                }
            }
        } else {
            // For existing tenants, handle cloud-managed key toggle
            let existing_tenant = repo.get(&self.state.config).select_by_id(param.id.unwrap()).await?;

            // Default to enable cloud-managed key if not explicitly set (for updates)
            // If cloud_managed_key status is being changed
            if let Some(enable_cloud_managed) = param.cloud_managed_key {
                if enable_cloud_managed && existing_tenant.encrypted_private_key.is_none() {
                    // User wants to enable cloud management but private key is not stored
                    // Need to get private key from user or use existing one if available
                    if let Some(user_provided_private_key) = param.encryption_private_key.as_ref() {
                        // Verify private key matches
                        let hash = TenantEncryptionKeys::hash_private_key(user_provided_private_key);
                        let existing_hash = existing_tenant
                            .encryption_private_key_hash
                            .as_ref()
                            .map(|s| s.as_str())
                            .unwrap_or("");
                        if hash != existing_hash {
                            return Err(Error::msg("Provided private key does not match tenant's key"));
                        }

                        // Encrypt and store
                        match TenantEncryptionKeys::encrypt_private_key_with_system_key(
                            user_provided_private_key,
                            &system_master_key,
                        ) {
                            Ok(encrypted) => {
                                tenant.encrypted_private_key = Some(encrypted);
                                tenant.cloud_managed_key = Some(true);
                            }
                            Err(e) => {
                                return Err(Error::msg(format!(
                                    "Failed to encrypt private key for cloud management: {}",
                                    e
                                )));
                            }
                        }
                    } else {
                        return Err(Error::msg("Private key must be provided to enable cloud-managed key"));
                    }
                } else if !enable_cloud_managed {
                    // User wants to disable cloud management - clear encrypted private key
                    tenant.cloud_managed_key = Some(false);
                    tenant.encrypted_private_key = None;
                } else {
                    // Cloud management already enabled, keep existing encrypted key
                    tenant.cloud_managed_key = Some(true);
                    tenant.encrypted_private_key = existing_tenant.encrypted_private_key.clone();
                }
            } else {
                // Preserve existing cloud_managed_key status
                tenant.cloud_managed_key = existing_tenant.cloud_managed_key;
                tenant.encrypted_private_key = existing_tenant.encrypted_private_key.clone();
            }

            // Preserve existing encryption keys (don't regenerate)
            tenant.encryption_public_key = existing_tenant.encryption_public_key.clone();
            tenant.encryption_private_key_hash = existing_tenant.encryption_private_key_hash.clone();
        }

        let tenant_id = if param.id.is_some() {
            repo.get(&self.state.config).update(tenant).await?
        } else {
            repo.get(&self.state.config).insert(tenant).await?
        };

        // Get public key for response
        let public_key = repo
            .get(&self.state.config)
            .select_by_id(tenant_id)
            .await?
            .encryption_public_key
            .unwrap_or_default();

        Ok(SaveTenantResponse::new(tenant_id, public_key, private_key_to_return))
    }

    #[audit_log("[TENANT][DELETE] id: {param.id}")]
    async fn delete(&self, param: DeleteTenantRequest) -> Result<u64, Error> {
        let repo = self.state.tenant_repo.lock().await;
        repo.get(&self.state.config).delete_by_id(param.id).await
    }
}
