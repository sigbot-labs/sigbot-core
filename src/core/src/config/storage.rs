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

use serde::{Deserialize, Serialize};
use sigbot_utils::serde_models::{deserialize_option_u32_from_string_or_number, deserialize_u16_from_string_or_number};
use std::ops::Deref;

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
    #[serde(rename = "Memory", alias = "MEMORY", alias = "memory")]
    MEMORY,
    #[serde(rename = "Redis", alias = "REDIS", alias = "redis")]
    REDIS,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MemoryProperties {
    #[serde(alias = "initial-capacity")]
    pub initial_capacity: Option<u32>,
    #[serde(alias = "max-capacity")]
    pub max_capacity: Option<u64>,
    pub ttl: Option<u64>,
    #[serde(alias = "eviction-policy")]
    pub eviction_policy: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedisProperties {
    pub nodes: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(alias = "connection-timeout")]
    pub connection_timeout: Option<u64>,
    #[serde(alias = "response-timeout")]
    pub response_timeout: Option<u64>,
    pub retries: Option<u32>,
    #[serde(alias = "max-retry-wait")]
    pub max_retry_wait: Option<u64>,
    #[serde(alias = "min-retry-wait")]
    pub min_retry_wait: Option<u64>,
    #[serde(alias = "read-from-replicas")]
    pub read_from_replicas: Option<bool>,
}

// App DB Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AppDBProperties {
    #[serde(rename = "type")]
    pub db_type: AppDBType,
    #[serde(rename = "sqlite", default = "SqliteAppDBProperties::default")]
    pub sqlite: SqliteAppDBProperties,
    #[serde(rename = "postgres", default = "PostgresAppDBProperties::default")]
    pub postgres: PostgresAppDBProperties,
    #[serde(rename = "mongodb", default = "MongoAppDBProperties::default")]
    pub mongodb: MongoAppDBProperties,
    #[serde(rename = "timescaledb", default = "TimescaleDBAppDBProperties::default")]
    pub timescaledb: TimescaleDBAppDBProperties,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum AppDBType {
    #[serde(alias = "SQLITE")]
    SQLITE,
    #[serde(alias = "POSTGRESQL")]
    POSTGRESQL,
    #[serde(alias = "MONGODB")]
    MONGODB,
    #[serde(alias = "TIMESCALEDB")]
    TIMESCALEDB,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SqliteAppDBProperties {
    #[serde(rename = "dir")]
    pub dir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresPropertiesBase {
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
    #[serde(
        alias = "min-connections",
        deserialize_with = "deserialize_option_u32_from_string_or_number"
    )]
    pub min_connections: Option<u32>,
    #[serde(
        alias = "max-connections",
        deserialize_with = "deserialize_option_u32_from_string_or_number"
    )]
    pub max_connections: Option<u32>,
    #[serde(alias = "use-ssl")]
    pub use_ssl: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresAppDBProperties {
    #[serde(flatten)]
    pub inner: PostgresPropertiesBase,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MongoAppDBProperties {
    #[serde(rename = "url")]
    pub url: Option<String>,
    #[serde(rename = "database")]
    pub database: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimescaleDBAppDBProperties {
    #[serde(flatten)]
    pub inner: PostgresPropertiesBase,
}

impl Default for TimescaleDBAppDBProperties {
    fn default() -> Self {
        TimescaleDBAppDBProperties {
            inner: PostgresPropertiesBase {
                host: "localhost".to_string(),
                port: 5432,
                database: "sigbot_ts".to_string(),
                schema: "public".to_string(),
                username: "postgres".to_string(),
                password: None,
                min_connections: Some(5),
                max_connections: Some(20),
                use_ssl: false,
            },
        }
    }
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

// App DB Properties impls.

impl AppDBProperties {
    /// Historical TimescaleDB provider uses PostgreSQL repositories and one pool.
    /// Its legacy connection block stays authoritative only when that provider is selected.
    pub fn normalize_provider(&mut self) {
        if self.db_type == AppDBType::TIMESCALEDB {
            self.postgres.inner = self.timescaledb.inner.clone();
            self.db_type = AppDBType::POSTGRESQL;
        }
    }
}

impl Default for AppDBProperties {
    fn default() -> Self {
        AppDBProperties {
            db_type: AppDBType::POSTGRESQL,
            sqlite: SqliteAppDBProperties::default(),
            postgres: PostgresAppDBProperties::default(),
            mongodb: MongoAppDBProperties::default(),
            timescaledb: TimescaleDBAppDBProperties::default(),
        }
    }
}

impl Default for SqliteAppDBProperties {
    fn default() -> Self {
        SqliteAppDBProperties {
            dir: Some(String::from("/tmp/sigbot/appdb/sqlite")),
        }
    }
}

impl Default for PostgresPropertiesBase {
    fn default() -> Self {
        PostgresPropertiesBase {
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

impl Default for PostgresAppDBProperties {
    fn default() -> Self {
        PostgresAppDBProperties {
            inner: PostgresPropertiesBase::default(),
        }
    }
}

impl Deref for PostgresAppDBProperties {
    type Target = PostgresPropertiesBase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for MongoAppDBProperties {
    fn default() -> Self {
        MongoAppDBProperties {
            url: Some(String::from("mongodb://localhost:27017")),
            database: Some(String::from("sigbot")),
        }
    }
}
