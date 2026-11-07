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

use crate::mgmt::apm::logging::LogMode;
use anyhow::Result;
use arc_swap::ArcSwap;
use config::Config as SourceConfig;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use sigbot_utils::serde_models::{deserialize_option_u32_from_string_or_number, deserialize_u16_from_string_or_number};
use std::collections::BTreeMap;
use std::env;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use url::Url;
use validator::Validate;

pub use super::authguard::*;

pub use super::llm::*;

pub use super::component::*;

// Global program information.
pub const GIT_VERSION: &str = env!("GIT_VERSION");
pub const GIT_COMMIT_HASH: &str = env!("GIT_COMMIT_HASH");
pub const GIT_BUILD_DATE: &str = env!("GIT_BUILD_DATE");

lazy_static! {
    pub static ref VERSION: String = format!(
        "GitVersion: {}, GitHash: {}, GitBuildDate: {}",
        env!("GIT_VERSION"),
        env!("GIT_COMMIT_HASH"),
        env!("GIT_BUILD_DATE")
    );
}

// Common platform configuration.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerProperties {
    pub host: String,
    pub port: u16,
    pub context_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MgmtProperties {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    #[serde(default = "TokioConsoleProperties::default")]
    pub tokio_console: TokioConsoleProperties,
    #[serde(default = "PyroscopeAgentProperties::default")]
    pub pyroscope: PyroscopeAgentProperties,
    #[serde(default = "OtelProperties::default")]
    pub otel: OtelProperties,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TokioConsoleProperties {
    pub enabled: bool,
    pub server_bind: String,
    pub retention: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PyroscopeAgentProperties {
    pub enabled: bool,
    pub server_url: String,
    pub auth_token: Option<String>,
    pub tags: Option<Vec<(String, String)>>,
    pub sample_rate: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct OtelProperties {
    pub enabled: bool,
    pub endpoint: String,
    pub protocol: String,
    pub timeout: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct LoggingProperties {
    pub mode: LogMode,
    pub level: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SwaggerProperties {
    pub enabled: bool,
    pub ui_path: String,
    pub openapi_url: String,
}

impl Default for ServerProperties {
    fn default() -> Self {
        Self {
            host: String::from("127.0.0.1"),
            port: 9000,
            context_path: None,
        }
    }
}

impl ServerProperties {
    pub fn get_bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for MgmtProperties {
    fn default() -> Self {
        Self {
            enabled: true,
            host: String::from("127.0.0.1"),
            port: 9001,
            tokio_console: TokioConsoleProperties::default(),
            pyroscope: PyroscopeAgentProperties::default(),
            otel: OtelProperties::default(),
        }
    }
}

impl MgmtProperties {
    pub fn get_bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for TokioConsoleProperties {
    fn default() -> Self {
        Self {
            enabled: true,
            server_bind: String::from("0.0.0.0:6669"),
            retention: 60,
        }
    }
}

impl Default for PyroscopeAgentProperties {
    fn default() -> Self {
        Self {
            enabled: true,
            server_url: String::from("http://127.0.0.1:4040"),
            auth_token: None,
            tags: None,
            sample_rate: 0.1,
        }
    }
}

impl Default for OtelProperties {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: String::from("http://localhost:4317"),
            protocol: String::from("grpc"),
            timeout: Some(Duration::from_secs(10).as_millis() as u64),
        }
    }
}

impl Default for LoggingProperties {
    fn default() -> Self {
        Self {
            mode: LogMode::HUMAN,
            level: String::from("info"),
        }
    }
}

impl LoggingProperties {
    pub fn is_human_mode(&self) -> bool {
        self.mode == LogMode::HUMAN
    }
}

impl Default for SwaggerProperties {
    fn default() -> Self {
        Self {
            enabled: true,
            ui_path: String::from("/swagger-ui"),
            openapi_url: String::from("/api-docs/openapi.json"),
        }
    }
}

// Shared cache and storage configuration.

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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageProperties {
    pub provider: StorageProvider,
    #[serde(default = "SqliteStorageProperties::default")]
    pub sqlite: SqliteStorageProperties,
    #[serde(default = "PostgresStorageProperties::default")]
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
    pub dir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresPropertiesBase {
    #[serde(default)]
    pub url: Option<String>,
    pub host: String,
    #[serde(deserialize_with = "deserialize_u16_from_string_or_number")]
    pub port: u16,
    pub database: String,
    pub schema: String,
    pub username: String,
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

impl Default for CacheProperties {
    fn default() -> Self {
        Self {
            provider: CacheProvider::MEMORY,
            memory: MemoryProperties::default(),
            redis: RedisProperties::default(),
        }
    }
}

impl Default for MemoryProperties {
    fn default() -> Self {
        Self {
            initial_capacity: Some(32),
            max_capacity: Some(65535),
            ttl: Some(3600),
            eviction_policy: Some(String::from("lru")),
        }
    }
}

impl Default for RedisProperties {
    fn default() -> Self {
        Self {
            nodes: vec![String::from("redis://127.0.0.1:6379")],
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
        Self {
            provider: StorageProvider::Postgres,
            sqlite: SqliteStorageProperties::default(),
            postgres: PostgresStorageProperties::default(),
        }
    }
}

impl Default for SqliteStorageProperties {
    fn default() -> Self {
        Self {
            dir: Some(String::from("/tmp/sigbot/storage/sqlite")),
        }
    }
}

impl Default for PostgresPropertiesBase {
    fn default() -> Self {
        Self {
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

// App Properties.

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AppConfigProperties {
    #[validate(length(min = 1, max = 32))]
    pub service_name: String,
    #[serde(default = "ServerProperties::default")]
    pub server: ServerProperties,
    #[serde(default = "MgmtProperties::default")]
    pub mgmt: MgmtProperties,
    #[serde(default = "LoggingProperties::default")]
    pub logging: LoggingProperties,
    #[serde(default = "SwaggerProperties::default")]
    pub swagger: SwaggerProperties,
    #[serde(default)]
    pub authguard: super::authguard::AuthguardProperties,
    #[serde(default = "CacheProperties::default")]
    pub cache: CacheProperties,
    #[serde(default = "StorageProperties::default")]
    pub storage: StorageProperties,
    #[serde(default)]
    pub secrets: SecretsProperties,
    #[serde(rename = "llm", default = "LlmProperties::default")]
    pub llm: LlmProperties,
    #[serde(default = "ServicesProperties::default")]
    pub services: ServicesProperties,
}

// App Properties impls.

impl AppConfigProperties {
    pub fn default() -> AppConfigProperties {
        AppConfigProperties {
            service_name: String::from("sigbot"),
            server: ServerProperties::default(),
            mgmt: MgmtProperties::default(),
            logging: LoggingProperties::default(),
            swagger: SwaggerProperties::default(),
            authguard: super::authguard::AuthguardProperties::default(),
            cache: CacheProperties::default(),
            storage: StorageProperties::default(),
            secrets: SecretsProperties::default(),
            llm: LlmProperties::default(),
            services: ServicesProperties::default(),
        }
    }
}

// ---- App Configuration. ----

#[derive(Debug)]
pub struct AppConfig {
    pub inner: AppConfigProperties,
}

impl Deref for AppConfig {
    type Target = AppConfigProperties;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AppConfig {
    pub fn new(config: &AppConfigProperties) -> Arc<AppConfig> {
        Arc::new(AppConfig { inner: config.clone() })
    }

    pub fn validate(self) -> Result<AppConfig, anyhow::Error> {
        self.inner.validate()?;
        Ok(self)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct SecretsProperties {
    /// Named secrets are populated by a mounted YAML Secret or by
    /// `SIGBOT__SECRETS__VALUES__<NAME>` overrides. Domain configuration stores
    /// only the stable key, never an environment-variable name.
    pub values: BTreeMap<String, String>,
}

impl SecretsProperties {
    pub fn require(&self, key: &str) -> Result<&str> {
        let canonical = key.trim().to_ascii_lowercase();
        let value = self
            .values
            .get(&canonical)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("configured secret is missing: {canonical}"))?;
        Ok(value)
    }
}

// Global the single refreshable configuration instance.
// see: https://github.com/wl4g-collect/openobserve/blob/v0.10.9/src/config/src/config.rs#L186
static CONFIG: std::sync::OnceLock<ArcSwap<AppConfig>> = std::sync::OnceLock::new();

/// Configuration path selected by the process startup entry point.
static CONFIG_PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

pub struct ConfigLoader;
impl ConfigLoader {
    pub fn initialize(path: impl AsRef<std::path::Path>) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        let config = Self::load(&path)?;
        if let Some(current) = CONFIG_PATH.get() {
            anyhow::ensure!(current == &path, "configuration path is already initialized");
        } else {
            CONFIG_PATH
                .set(path)
                .map_err(|_| anyhow::anyhow!("configuration already initialized"))?;
        }
        CONFIG.get_or_init(|| ArcSwap::from(config.clone())).store(config);
        Ok(())
    }

    fn load_current() -> Result<Arc<AppConfig>> {
        if let Some(path) = CONFIG_PATH.get() {
            return Self::load(path);
        }
        if let Some(path) = env::var_os("SIGBOT_CFG_PATH") {
            return Self::load(std::path::Path::new(&path));
        }
        Self::load_sources(None)
    }

    fn load(path: impl AsRef<std::path::Path>) -> Result<Arc<AppConfig>> {
        Self::load_sources(Some(path.as_ref()))
    }

    fn load_sources(path: Option<&std::path::Path>) -> Result<Arc<AppConfig>> {
        let defaults =
            serde_json::to_string(&AppConfigProperties::default()).expect("configuration defaults must serialize");
        let mut builder =
            SourceConfig::builder().add_source(config::File::from_str(&defaults, config::FileFormat::Json));
        if let Some(path) = path {
            let value = SourceConfig::builder()
                .add_source(config::File::from(path).format(config::FileFormat::Yaml).required(true))
                .build()
                .and_then(|config| config.try_deserialize::<serde_json::Value>())
                .map_err(|_| anyhow::anyhow!("configuration YAML cannot be loaded"))?;
            let value = Self::normalize(value)?;
            builder = builder.add_source(config::File::from_str(
                &serde_json::to_string(&value)?,
                config::FileFormat::Json,
            ));
        }
        let environment = SourceConfig::builder()
            .add_source(
                config::Environment::with_prefix("SIGBOT")
                    .separator("__")
                    .prefix_separator("__")
                    .keep_prefix(false),
            )
            .build()
            .and_then(|source| source.try_deserialize::<serde_json::Value>())
            .map_err(|_| anyhow::anyhow!("configuration environment cannot be loaded"))?;
        let environment = Self::normalize_environment(environment)?;
        builder = builder.add_source(config::File::from_str(
            &serde_json::to_string(&environment).expect("environment configuration must serialize"),
            config::FileFormat::Json,
        ));
        // Errors may contain supplied values; never print raw deserialization errors.
        let value = builder
            .build()
            .map_err(|_| anyhow::anyhow!("configuration sources cannot be merged"))?;
        let properties = Self::deserialize(value)?;
        properties
            .validate()
            .map_err(|_| anyhow::anyhow!("application configuration constraints failed"))?;
        tracing::info!(service_name = %properties.service_name, "Configuration loaded");
        Ok(AppConfig::new(&properties))
    }

    fn deserialize(value: SourceConfig) -> Result<AppConfigProperties> {
        let properties: AppConfigProperties = serde_path_to_error::deserialize(value).map_err(|error| {
            // Paths name typed fields; the underlying error can contain secret values.
            let path = error.path().to_string();
            let safe_path = if path.len() <= 160
                && path
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.[]".contains(&byte))
            {
                path.as_str()
            } else {
                "<field>"
            };
            anyhow::anyhow!("configuration validation failed at {safe_path}; expected typed key/value")
        })?;
        Ok(properties)
    }

    fn normalize_environment(mut value: serde_json::Value) -> Result<serde_json::Value> {
        for (path, empty_allowed) in [("/cache/redis/nodes", false), ("/authguard/oauth/scopes", true)] {
            if let Some(nodes) = value.pointer_mut(path) {
                let text = nodes
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("{path} must be a comma-separated string"))?;
                let values = text
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(|item| serde_json::Value::String(item.to_owned()))
                    .collect::<Vec<_>>();
                anyhow::ensure!(empty_allowed || !values.is_empty(), "{path} cannot be empty");
                *nodes = serde_json::Value::Array(values);
            }
        }
        Self::normalize(value)
    }

    fn normalize(value: serde_json::Value) -> Result<serde_json::Value> {
        use serde_json::Value;
        Ok(match value {
            Value::Object(object) => {
                let mut result = serde_json::Map::new();
                for (key, value) in object {
                    anyhow::ensure!(
                        !key.is_empty()
                            && !key.starts_with('_')
                            && !key.ends_with('_')
                            && !key.contains("__")
                            && key
                                .bytes()
                                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
                        "configuration keys must use canonical snake_case"
                    );
                    let value = if matches!(key.as_str(), "tags" | "input_schema") {
                        value
                    } else {
                        Self::normalize(value)?
                    };
                    result.insert(key, value);
                }
                Value::Object(result)
            }
            Value::Array(values) => Value::Array(values.into_iter().map(Self::normalize).collect::<Result<_>>()?),
            Value::String(value) => Value::String(Self::expand(&value)?),
            value => value,
        })
    }
    fn expand(value: &str) -> Result<String> {
        let mut output = String::new();
        let mut rest = value;
        while let Some(start) = rest.find("${") {
            output.push_str(&rest[..start]);
            let reference = &rest[start + 2..];
            let end = reference
                .find('}')
                .ok_or_else(|| anyhow::anyhow!("unterminated environment reference"))?;
            let (key, fallback) = reference[..end]
                .split_once(':')
                .map(|(key, value)| (key, Some(value)))
                .unwrap_or((&reference[..end], None));
            anyhow::ensure!(
                !key.is_empty()
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'),
                "invalid environment reference"
            );
            let resolved = env::var(key)
                .ok()
                .or_else(|| fallback.map(str::to_owned))
                .ok_or_else(|| anyhow::anyhow!("required environment reference is missing"))?;
            output.push_str(&resolved);
            rest = &reference[end + 1..];
        }
        output.push_str(rest);
        Ok(output)
    }
}

pub fn get_config() -> Arc<AppConfig> {
    CONFIG
        .get_or_init(|| ArcSwap::from(ConfigLoader::load_current().expect("configuration cannot be loaded")))
        .load()
        .clone()
}

pub fn refresh_config() -> Result<(), anyhow::Error> {
    let config = ConfigLoader::load_current()?;
    CONFIG.get_or_init(|| ArcSwap::from(config.clone())).store(config);
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/config/config_loader_test.rs"]
mod tests;
