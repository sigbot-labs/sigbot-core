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

use serde::Deserialize;

use serde::Serialize;
use sigbot_utils::serde_models::deserialize_option_u32_from_string_or_number;
use sigbot_utils::serde_models::deserialize_u16_from_string_or_number;
use std::ops::Deref;
use url::Url;

// Cache Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheProperties {
    pub provider: CacheProvider,
    pub memory: MemoryProperties,
    pub redis: RedisProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CacheProvider {
    #[serde(rename = "Memory")]
    MEMORY,
    #[serde(rename = "Redis")]
    REDIS,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MemoryProperties {
    pub initial_capacity: Option<u32>,
    pub max_capacity: Option<u64>,
    pub ttl: Option<u64>,
    pub eviction_policy: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedisProperties {
    pub nodes: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Option<u64>,
    pub response_timeout: Option<u64>,
    pub retries: Option<u32>,
    pub max_retry_wait: Option<u64>,
    pub min_retry_wait: Option<u64>,
    pub read_from_replicas: Option<bool>,
}

// Storage provider configuration.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageProperties {
    pub provider: StorageProvider,
    #[serde(rename = "sqlite", default = "SqliteStorageProperties::default")]
    pub sqlite: SqliteStorageProperties,
    #[serde(rename = "postgres", default = "PostgresStorageProperties::default")]
    pub postgres: PostgresStorageProperties,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum StorageProvider {
    #[serde(rename = "SQLite")]
    SQLite,
    #[serde(rename = "Postgres")]
    Postgres,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SqliteStorageProperties {
    #[serde(rename = "dir")]
    pub dir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresPropertiesBase {
    /// Optional complete connection URL. It is primarily useful for mounted
    /// secrets and integration environments; explicit fields remain the
    /// readable default for checked-in YAML.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port", deserialize_with = "deserialize_u16_from_string_or_number")]
    pub port: u16,
    #[serde(rename = "database")]
    pub database: String,
    #[serde(rename = "schema")]
    pub schema: String,
    #[serde(rename = "username")]
    pub username: String,
    #[serde(rename = "password")]
    pub password: Option<String>,
    #[serde(deserialize_with = "deserialize_option_u32_from_string_or_number")]
    pub min_connections: Option<u32>,
    #[serde(deserialize_with = "deserialize_option_u32_from_string_or_number")]
    pub max_connections: Option<u32>,
    pub use_ssl: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PostgresStorageProperties {
    #[serde(flatten)]
    pub inner: PostgresPropertiesBase,
}

// Cache Properties impls.

impl Default for CacheProperties {
    fn default() -> Self {
        CacheProperties {
            provider: CacheProvider::MEMORY,
            memory: MemoryProperties::default(),
            redis: RedisProperties::default(),
        }
    }
}

impl Default for MemoryProperties {
    fn default() -> Self {
        MemoryProperties {
            initial_capacity: Some(32),
            max_capacity: Some(65535),
            ttl: Some(3600),
            eviction_policy: Some("lru".to_string()),
        }
    }
}

impl Default for RedisProperties {
    fn default() -> Self {
        RedisProperties {
            nodes: vec!["redis://127.0.0.1:6379".to_string()],
            username: None,
            password: None,
            connection_timeout: Some(3000),
            response_timeout: Some(6000),
            retries: Some(1),
            max_retry_wait: Some(65536),
            min_retry_wait: Some(1280),
            read_from_replicas: Some(false),
        }
    }
}

impl Default for StorageProperties {
    fn default() -> Self {
        StorageProperties {
            provider: StorageProvider::Postgres,
            sqlite: SqliteStorageProperties::default(),
            postgres: PostgresStorageProperties::default(),
        }
    }
}

impl Default for SqliteStorageProperties {
    fn default() -> Self {
        SqliteStorageProperties {
            dir: Some(String::from("/tmp/sigbot/storage/sqlite")),
        }
    }
}

impl Default for PostgresPropertiesBase {
    fn default() -> Self {
        PostgresPropertiesBase {
            url: None,
            host: String::from("127.0.0.1"),
            port: 5432,
            database: String::from("sigbot"),
            schema: String::from("sigbot"),
            username: String::from("postgres"),
            password: None,
            min_connections: Some(1),
            max_connections: Some(10),
            use_ssl: false,
        }
    }
}

impl Deref for PostgresStorageProperties {
    type Target = PostgresPropertiesBase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PostgresStorageProperties {
    pub fn connection_url(&self) -> anyhow::Result<String> {
        if let Some(url) = self.url.as_deref().filter(|url| !url.trim().is_empty()) {
            Url::parse(url).map_err(|_| anyhow::anyhow!("invalid configured PostgreSQL URL"))?;
            return Ok(url.to_owned());
        }
        let mut url = Url::parse("postgresql://localhost")?;
        url.set_host(Some(&self.host))?;
        url.set_port(Some(self.port))
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL port"))?;
        url.set_username(&self.username)
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL username"))?;
        url.set_password(self.password.as_deref())
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL password"))?;
        url.set_path(&self.database);
        if !self.schema.is_empty() {
            url.query_pairs_mut()
                .append_pair("options", &format!("-c search_path={}", self.schema));
        }
        Ok(url.into())
    }
}
