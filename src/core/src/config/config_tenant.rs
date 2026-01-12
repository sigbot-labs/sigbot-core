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

use crate::config::config::{
    init, init_with_custom, register_custom_configurer, AppConfig, AppConfigProperties, CustomConfigConfigurer,
    CUSTOM_CONFIGURER,
};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use common_telemetry::warn;
use once_cell::sync::Lazy;
use sigbot_types::{
    modules::decode_arg_config,
    sys::tenant::{ComponentType, ComponentsConfig},
};
use std::sync::Arc;

// Global tenant-specific configuration instance for tenant-level services.
pub(crate) static TENANT_CONFIG: Lazy<ArcSwap<AppConfig>> = Lazy::new(|| {
    ArcSwap::from(init()) // Back to platform-level config if not initialized with tenant config
});

pub(crate) struct DefaultTenantConfigConfigurer;

// This default tenant configuration configurer extracts tenant-specific such as middleware configuration
// (e.g. Postgres, Redis, etc.) from the tenant configuration and applies it to the base application configuration.
impl CustomConfigConfigurer for DefaultTenantConfigConfigurer {
    fn configure(&self, base_config: &AppConfigProperties, custom_config_json: &str) -> Result<AppConfigProperties> {
        let mut configured = base_config.clone();

        // Parse tenant configuration JSON to extract component information
        // The tenant_config_json may contain components configuration or other tenant-specific settings
        let tenant_config: serde_json::Value =
            serde_json::from_str(custom_config_json).context("Failed to parse tenant configuration JSON")?;

        // Try to extract components configuration from tenant config
        // Components config may be nested in different places depending on how deployer passes it
        if let Some(components_json) = tenant_config.get("components") {
            if let Ok(components_config) = serde_json::from_value::<ComponentsConfig>(components_json.clone()) {
                // Update database configuration from PostgreSQL component
                if let Some(postgres_component) = components_config
                    .components
                    .iter()
                    .find(|c| c.component_type == ComponentType::Postgresql)
                {
                    if let Some(conn) = &postgres_component.connection {
                        configured.appdb.postgres.inner.host = conn
                            .host
                            .clone()
                            .unwrap_or_else(|| configured.appdb.postgres.inner.host.clone());
                        if let Some(port) = conn.port {
                            configured.appdb.postgres.inner.port = port;
                        }
                        if let Some(database) = &conn.database {
                            configured.appdb.postgres.inner.database = database.clone();
                        }
                        if let Some(username) = &conn.username {
                            configured.appdb.postgres.inner.username = username.clone();
                        }
                        // Note: encrypted_password needs to be decrypted using tenant's private key
                        // This is typically handled by the service that has access to tenant keys
                        // For now, we'll leave it as None and let the service handle decryption
                        warn!("PostgreSQL password decryption should be handled by the service with tenant key access");
                    }
                }

                // Update Redis configuration from Redis component
                if let Some(redis_component) = components_config
                    .components
                    .iter()
                    .find(|c| c.component_type == ComponentType::Redis)
                {
                    if let Some(conn) = &redis_component.connection {
                        let host = conn.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
                        let port = conn.port.unwrap_or(6379);
                        let redis_url = format!("redis://{}:{}", host, port);
                        configured.cache.redis.nodes = vec![redis_url];
                        if let Some(username) = &conn.username {
                            configured.cache.redis.username = Some(username.clone());
                        }
                        // Note: encrypted_password needs to be decrypted
                        warn!("Redis password decryption should be handled by the service with tenant key access");
                    }
                }

                // Update TimescaleDB configuration if used for vector DB
                if let Some(timescaledb_component) = components_config
                    .components
                    .iter()
                    .find(|c| c.component_type == ComponentType::Timescaledb)
                {
                    if let Some(conn) = &timescaledb_component.connection {
                        configured.llm.vecdb.pg_vector.inner.host = conn
                            .host
                            .clone()
                            .unwrap_or_else(|| configured.llm.vecdb.pg_vector.inner.host.clone());
                        if let Some(port) = conn.port {
                            configured.llm.vecdb.pg_vector.inner.port = port;
                        }
                        if let Some(database) = &conn.database {
                            configured.llm.vecdb.pg_vector.inner.database = database.clone();
                        }
                        if let Some(username) = &conn.username {
                            configured.llm.vecdb.pg_vector.inner.username = username.clone();
                        }
                        warn!(
                            "TimescaleDB password decryption should be handled by the service with tenant key access"
                        );
                    }
                }
            }
        }

        // Also check for direct database/redis configuration in tenant config
        // This allows deployer to pass configuration in different formats
        if let Some(db_config) = tenant_config.get("database") {
            if let Some(host) = db_config.get("host").and_then(|v| v.as_str()) {
                configured.appdb.postgres.inner.host = host.to_string();
            }
            if let Some(port) = db_config.get("port").and_then(|v| v.as_u64()) {
                configured.appdb.postgres.inner.port = port as u16;
            }
            if let Some(database) = db_config.get("database").and_then(|v| v.as_str()) {
                configured.appdb.postgres.inner.database = database.to_string();
            }
            if let Some(username) = db_config.get("username").and_then(|v| v.as_str()) {
                configured.appdb.postgres.inner.username = username.to_string();
            }
        }

        if let Some(redis_config) = tenant_config.get("redis") {
            if let Some(host) = redis_config.get("host").and_then(|v| v.as_str()) {
                let port = redis_config.get("port").and_then(|v| v.as_u64()).unwrap_or(6379) as u16;
                let redis_url = format!("redis://{}:{}", host, port);
                configured.cache.redis.nodes = vec![redis_url];
            }
            if let Some(username) = redis_config.get("username").and_then(|v| v.as_str()) {
                configured.cache.redis.username = Some(username.to_string());
            }
        }

        Ok(configured)
    }
}

pub fn init_tenant_config(configuration: &str) {
    // Ensure DefaultTenantConfigConfigurer is registered (idempotent)
    if CUSTOM_CONFIGURER.load().is_none() {
        register_custom_configurer(Arc::new(DefaultTenantConfigConfigurer));
    }

    // Decode tenant-level custom configuration
    let custom_config_json = decode_arg_config(configuration).expect("Failed to decode custom configuration.");

    // Initialize and cache tenant-level custom configuration
    TENANT_CONFIG.store(init_with_custom(&custom_config_json));
}

pub fn get_tenant_config() -> Arc<AppConfig> {
    TENANT_CONFIG.load().clone()
}
