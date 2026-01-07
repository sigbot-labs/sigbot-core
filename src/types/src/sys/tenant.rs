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

use crate::{EntityBase, PageResponse};
use base64::{engine::general_purpose, Engine as _};
use common_makestruct::MakeStructWith;
use openssl::rsa::{Padding, Rsa};
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use serde_json;
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::collections::HashMap;
use std::error::Error;
use validator::Validate;

/// Component type enumeration
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ComponentType {
    Postgresql,
    Emqx,
    Redis,
    Timescaledb,
    Datafeed,
    Strategy,
    Backtest,
    Notification,
}

/// Component connection configuration with encrypted password
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct ComponentConnectionConfig {
    /// Component instance host/address
    pub host: Option<String>,
    /// Component instance port
    pub port: Option<u16>,
    /// Component instance database name (for databases)
    pub database: Option<String>,
    /// Component instance username
    pub username: Option<String>,
    /// Encrypted password (base64 encoded, encrypted with tenant's public key)
    pub encrypted_password: Option<String>,
    /// Additional connection parameters
    pub params: Option<HashMap<String, String>>,
}

/// Component deployment instance information
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct ComponentInstance {
    /// Component type
    #[serde(rename = "type")]
    pub component_type: ComponentType,
    /// Component instance name/identifier
    pub name: String,
    /// Deployment mode (standalone/cluster)
    pub mode: Option<String>,
    /// Number of replicas (for cluster mode)
    pub replicas: Option<u32>,
    /// Connection configuration
    pub connection: Option<ComponentConnectionConfig>,
    /// Performance parameters
    pub performance: Option<HashMap<String, serde_json::Value>>,
    /// Deployment metadata (Kubernetes namespace, Docker network, etc.)
    pub deployment_metadata: Option<HashMap<String, String>>,
    /// Deployment status
    pub status: Option<String>, // e.g., "running", "stopped", "error"
    /// Last deployment timestamp
    pub deployed_at: Option<String>,
    /// Last update timestamp
    pub updated_at: Option<String>,
}

/// Components configuration container
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct ComponentsConfig {
    /// List of deployed components
    pub components: Vec<ComponentInstance>,
}

impl ComponentsConfig {
    pub fn new() -> Self {
        ComponentsConfig { components: Vec::new() }
    }

    /// Find component by type and name
    pub fn find_component(&self, component_type: &ComponentType, name: &str) -> Option<&ComponentInstance> {
        self.components
            .iter()
            .find(|c| c.component_type == *component_type && c.name == name)
    }

    /// Find component by type and name (mutable)
    pub fn find_component_mut(&mut self, component_type: &ComponentType, name: &str) -> Option<&mut ComponentInstance> {
        self.components
            .iter_mut()
            .find(|c| c.component_type == *component_type && c.name == name)
    }

    /// Add or update component
    pub fn upsert_component(&mut self, component: ComponentInstance) {
        if let Some(existing) = self.find_component_mut(&component.component_type, &component.name) {
            *existing = component;
        } else {
            self.components.push(component);
        }
    }

    /// Remove component
    pub fn remove_component(&mut self, component_type: &ComponentType, name: &str) -> bool {
        let index = self
            .components
            .iter()
            .position(|c| c.component_type == *component_type && c.name == name);
        if let Some(idx) = index {
            self.components.remove(idx);
            true
        } else {
            false
        }
    }
}

impl Default for ComponentsConfig {
    fn default() -> Self {
        ComponentsConfig::new()
    }
}

// Tenant encryption key pair for encrypting component passwords and exchange keys
pub struct TenantEncryptionKeys {
    pub public_key: String,  // Base64 encoded RSA public key
    pub private_key: String, // Base64 encoded RSA private key (should be shown only once)
}

impl TenantEncryptionKeys {
    /// Generate a new RSA key pair for tenant encryption
    pub fn generate() -> Result<Self, Box<dyn Error>> {
        use sigbot_utils::rsa_ciphers::RSACipher;
        let cipher = RSACipher::new(2048)?;
        Ok(TenantEncryptionKeys {
            public_key: cipher.get_base64_public_key()?,
            private_key: cipher.get_base64_private_key()?,
        })
    }

    /// Create from existing private key (for user-provided keys)
    pub fn from_private_key(private_key: &str) -> Result<Self, Box<dyn Error>> {
        use sigbot_utils::rsa_ciphers::RSACipher;
        let cipher = RSACipher::from_base64(private_key)?;
        Ok(TenantEncryptionKeys {
            public_key: cipher.get_base64_public_key()?,
            private_key: private_key.to_string(),
        })
    }

    /// Calculate SHA256 hash of private key for verification
    pub fn hash_private_key(private_key: &str) -> String {
        let hash = sha256(private_key.as_bytes());
        general_purpose::STANDARD.encode(hash)
    }

    /// Encrypt password using tenant's public key
    /// Note: public_key should be base64 encoded PEM format
    pub fn encrypt_password(public_key: &str, password: &str) -> Result<String, Box<dyn Error>> {
        // Decode base64 to get PEM
        let pem = general_purpose::STANDARD.decode(public_key)?;
        // Parse RSA public key from PEM
        let rsa = Rsa::public_key_from_pem(&pem)?;
        // Encrypt password
        let mut buf: Vec<u8> = vec![0; rsa.size() as usize];
        let len = rsa.public_encrypt(password.as_bytes(), &mut buf, Padding::PKCS1)?;
        buf.truncate(len);
        // Return base64 encoded encrypted data
        Ok(general_purpose::STANDARD.encode(&buf))
    }

    /// Decrypt password using tenant's private key
    pub fn decrypt_password(private_key: &str, encrypted_password: &str) -> Result<String, Box<dyn Error>> {
        use sigbot_utils::rsa_ciphers::RSACipher;
        let cipher = RSACipher::from_base64(private_key)?;
        let decrypted = cipher.decrypt_from_base64(encrypted_password)?;
        String::from_utf8(decrypted).map_err(|e| e.into())
    }

    /// Encrypt tenant private key with system master key (for cloud-managed keys)
    /// Uses AES-256-GCM for symmetric encryption
    pub fn encrypt_private_key_with_system_key(
        private_key: &str,
        system_master_key: &str,
    ) -> Result<String, Box<dyn Error>> {
        use openssl::symm::{encrypt, Cipher};

        // Derive AES key from system master key using SHA256
        let key = sha256(system_master_key.as_bytes());

        // Generate random IV (12 bytes for GCM)
        use openssl::rand::rand_bytes;
        let mut iv = vec![0u8; 12];
        rand_bytes(&mut iv)?;

        // Encrypt private key
        let cipher = Cipher::aes_256_gcm();
        let ciphertext = encrypt(cipher, &key, Some(&iv), private_key.as_bytes())?;

        // Combine IV + ciphertext and encode as base64
        let mut combined = iv;
        combined.extend_from_slice(&ciphertext);
        Ok(general_purpose::STANDARD.encode(&combined))
    }

    /// Decrypt tenant private key with system master key (for cloud-managed keys)
    pub fn decrypt_private_key_with_system_key(
        encrypted_private_key: &str,
        system_master_key: &str,
    ) -> Result<String, Box<dyn Error>> {
        use openssl::symm::{decrypt, Cipher};

        // Decode base64
        let combined = general_purpose::STANDARD.decode(encrypted_private_key)?;

        // Extract IV (first 12 bytes) and ciphertext
        if combined.len() < 12 {
            return Err("Invalid encrypted data format".into());
        }
        let iv = &combined[0..12];
        let ciphertext = &combined[12..];

        // Derive AES key from system master key using SHA256
        let key = sha256(system_master_key.as_bytes());

        // Decrypt private key
        let cipher = Cipher::aes_256_gcm();
        let decrypted = decrypt(cipher, &key, Some(iv), ciphertext)?;
        String::from_utf8(decrypted).map_err(|e| e.into())
    }

    /// Get tenant private key from cloud-managed storage
    /// This is used when decrypting encrypted data (e.g., component passwords, exchange keys)
    /// Returns the decrypted private key if cloud-managed is enabled, otherwise returns None
    pub fn get_tenant_private_key_from_cloud(
        cloud_managed_key: Option<bool>,
        encrypted_private_key: Option<&str>,
        system_master_key: &str,
    ) -> Result<Option<String>, Box<dyn Error>> {
        if cloud_managed_key.unwrap_or(false) {
            if let Some(encrypted_private_key) = encrypted_private_key {
                let decrypted = Self::decrypt_private_key_with_system_key(encrypted_private_key, system_master_key)?;
                Ok(Some(decrypted))
            } else {
                Err("Cloud-managed key is enabled but encrypted private key is not stored".into())
            }
        } else {
            // Private key is not managed by platform - user must provide it
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct Tenant {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub shared: Option<bool>,  // True if the tenant is shared, false if the tenant is exclusive.
    pub admin_id: Option<i64>, // The ID of the administrator user.
    pub properties: Option<serde_json::Value>,
    pub description: Option<String>,
    // Tenant-specific encryption key pair for encrypting component passwords
    // Public key is stored in database, private key should be shown only once during tenant creation
    pub encryption_public_key: Option<String>, // Base64 encoded RSA public key
    pub encryption_private_key_hash: Option<String>, // SHA256 hash of private key for verification
    // Encrypted private key (encrypted with system master key, only stored if cloud_managed_key is true)
    pub encrypted_private_key: Option<String>, // Base64 encoded, encrypted with system master key
    // Cloud-managed key: If true, platform stores encrypted private key for recovery
    // This prevents data loss if user loses their private key
    pub cloud_managed_key: Option<bool>, // True if platform manages the private key
    // Component deployment configurations (encrypted passwords stored here)
    pub components: Option<serde_json::Value>, // JSON object containing component configurations
}

impl Default for Tenant {
    fn default() -> Self {
        Tenant {
            base: EntityBase::new_empty(),
            name: None,
            shared: None,
            admin_id: None,
            properties: None,
            description: None,
            encryption_public_key: None,
            encryption_private_key_hash: None,
            cloud_managed_key: None,
            encrypted_private_key: None,
            components: None,
        }
    }
}

/// SqliteRow impl for Tenant.

impl<'r> FromRow<'r, SqliteRow> for Tenant {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Tenant {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            shared: row.try_get("shared")?,
            admin_id: row.try_get("admin_id")?,
            properties: row.try_get::<Option<String>, _>("properties")?.and_then(|json_str| {
                if json_str.is_empty() {
                    None
                } else {
                    serde_json::from_str::<serde_json::Value>(&json_str).ok()
                }
            }),
            description: row.try_get("description")?,
            encryption_public_key: row.try_get("encryption_public_key")?,
            encryption_private_key_hash: row.try_get("encryption_private_key_hash")?,
            cloud_managed_key: row.try_get("cloud_managed_key")?,
            encrypted_private_key: row.try_get("encrypted_private_key")?,
            components: row.try_get::<Option<String>, _>("components")?.and_then(|json_str| {
                if json_str.is_empty() {
                    None
                } else {
                    serde_json::from_str::<serde_json::Value>(&json_str).ok()
                }
            }),
        })
    }
}

/// Postgres Row impl for Tenant.

impl<'r> FromRow<'r, PgRow> for Tenant {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Tenant {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            shared: row.try_get("shared")?,
            admin_id: row.try_get("admin_id")?,
            properties: {
                // PostgreSQL JSONB can be retrieved as serde_json::Value
                let json_val: Option<serde_json::Value> = row.try_get("properties").ok().flatten();
                json_val.and_then(|v| if v.is_null() { None } else { Some(v) })
            },
            description: row.try_get("description")?,
            encryption_public_key: row.try_get("encryption_public_key")?,
            encryption_private_key_hash: row.try_get("encryption_private_key_hash")?,
            cloud_managed_key: row.try_get("cloud_managed_key")?,
            encrypted_private_key: row.try_get("encrypted_private_key")?,
            components: {
                // PostgreSQL JSONB can be retrieved as serde_json::Value
                let json_val: Option<serde_json::Value> = row.try_get("components").ok().flatten();
                json_val.and_then(|v| if v.is_null() { None } else { Some(v) })
            },
        })
    }
}

// --- Models. ---

#[derive(
    Deserialize,
    Clone,
    Debug,
    PartialEq,
    Validate,
    utoipa::ToSchema,
    utoipa::IntoParams, // PageableQueryRequest // Try using macro auto generated pageable query request.
)]
#[into_params(parameter_in = Query)]
pub struct QueryTenantRequest {
    #[validate(length(min = 1, max = 64))]
    pub name: Option<String>,
    pub shared: Option<bool>,
    pub admin_id: Option<i64>,
    pub properties: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 1024))]
    pub description: Option<String>,
}

impl QueryTenantRequest {
    pub fn to_tenant(&self) -> Tenant {
        Tenant {
            base: EntityBase::new_empty(),
            name: Some(self.name.clone().unwrap_or_default()),
            shared: self.shared.clone(),
            admin_id: self.admin_id.clone(),
            properties: self
                .properties
                .as_ref()
                .map(|props| serde_json::to_value(props).unwrap_or(serde_json::Value::Null)),
            description: self.description.clone(),
            encryption_public_key: None,
            encryption_private_key_hash: None,
            cloud_managed_key: None,
            encrypted_private_key: None,
            components: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryTenantResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<Tenant>>,
}

impl QueryTenantResponse {
    pub fn new(page: PageResponse, data: Vec<Tenant>) -> Self {
        QueryTenantResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
// #[smart_copy(target = "SaveTenantRequestWith")]
pub struct SaveTenantRequest {
    pub id: Option<i64>,
    #[validate(length(min = 1, max = 64))]
    pub name: Option<String>,
    pub shared: Option<bool>,
    pub admin_id: Option<i64>,
    pub properties: Option<HashMap<String, String>>,
    pub description: Option<String>,
    /// User-provided encryption public key (base64 encoded RSA public key)
    /// If not provided, system will generate a key pair
    pub encryption_public_key: Option<String>,
    /// User-provided encryption private key (base64 encoded RSA private key)
    /// If provided, must match the public key. System will hash it for verification.
    /// WARNING: This should only be provided during tenant creation and shown once.
    pub encryption_private_key: Option<String>,
    /// Enable cloud-managed key: Platform will store encrypted private key for recovery
    /// This prevents data loss if user loses their private key
    pub cloud_managed_key: Option<bool>,
}

impl SaveTenantRequest {
    pub fn to_tenant(&self) -> Tenant {
        Tenant {
            base: EntityBase::new_with_id(self.id),
            name: self.name.clone(), // self.name.as_ref().map(|n| n.to_string())
            shared: None,
            admin_id: None,
            properties: None,
            description: None,
            encryption_public_key: self.encryption_public_key.clone(),
            encryption_private_key_hash: None, // Will be set after hashing private key
            cloud_managed_key: self.cloud_managed_key.clone(),
            encrypted_private_key: None, // Will be set if cloud_managed_key is true
            components: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveTenantResponse {
    pub id: i64,
    /// Encryption public key (always returned)
    pub encryption_public_key: String,
    /// Encryption private key (only returned once during creation if system generated)
    /// WARNING: This is the ONLY time the private key will be shown. Save it securely!
    pub encryption_private_key: Option<String>,
}

impl SaveTenantResponse {
    pub fn new(id: i64, encryption_public_key: String, encryption_private_key: Option<String>) -> Self {
        SaveTenantResponse {
            id,
            encryption_public_key,
            encryption_private_key,
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteTenantRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteTenantResponse {
    pub count: u64,
}

impl DeleteTenantResponse {
    pub fn new(count: u64) -> Self {
        DeleteTenantResponse { count }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_encryption() {
        // Generate key pair
        let keys = TenantEncryptionKeys::generate().unwrap();
        assert!(!keys.public_key.is_empty());
        assert!(!keys.private_key.is_empty());

        // Hash private key
        let hash = TenantEncryptionKeys::hash_private_key(&keys.private_key);
        assert!(!hash.is_empty());

        // Test encryption/decryption
        let password = "test_password_123";
        let encrypted = TenantEncryptionKeys::encrypt_password(&keys.public_key, password).unwrap();
        assert!(!encrypted.is_empty());

        let decrypted = TenantEncryptionKeys::decrypt_password(&keys.private_key, &encrypted).unwrap();
        assert_eq!(password, decrypted);
    }
}
