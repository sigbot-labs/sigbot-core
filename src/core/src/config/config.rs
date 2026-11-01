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

use anyhow::Result;
use arc_swap::ArcSwap;
use config::Config as SourceConfig;
use dotenv::dotenv;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use std::collections::BTreeMap;
use std::env;
use std::ops::Deref;
use std::sync::Arc;
use validator::Validate;

pub use super::authguard::*;

pub use super::llm::*;

pub use super::platform::*;

pub use super::services::*;

pub use super::storage::*;

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
static CONFIG: Lazy<ArcSwap<AppConfig>> = Lazy::new(|| ArcSwap::from(ConfigLoader::load()));

/// Sole configuration loading entry; public wrappers preserve the existing API.
pub struct ConfigLoader;
impl ConfigLoader {
    fn load() -> Arc<AppConfig> {
        dotenv().ok();
        let defaults =
            serde_json::to_string(&AppConfigProperties::default()).expect("configuration defaults must serialize");
        let mut builder =
            SourceConfig::builder().add_source(config::File::from_str(&defaults, config::FileFormat::Json));
        if let Ok(path) = env::var("SIGBOT_CFG_PATH") {
            if !path.trim().is_empty() {
                let value = SourceConfig::builder()
                    .add_source(config::File::with_name(path.trim()))
                    .build()
                    .and_then(|config| config.try_deserialize::<serde_json::Value>())
                    .unwrap_or_else(|_| panic!("configuration file cannot be loaded"));
                let value =
                    Self::normalize(value).expect("configuration file has conflicting keys or unresolved variables");
                let json = serde_json::to_string(&value).expect("configuration must serialize");
                builder = builder.add_source(config::File::from_str(&json, config::FileFormat::Json));
            }
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
            .unwrap_or_else(|_| panic!("configuration environment cannot be loaded"));
        let environment = Self::normalize_environment(environment)
            .unwrap_or_else(|_| panic!("configuration environment contains invalid typed values"));
        builder = builder.add_source(config::File::from_str(
            &serde_json::to_string(&environment).expect("environment configuration must serialize"),
            config::FileFormat::Json,
        ));
        // Errors may contain supplied values; never print raw deserialization errors.
        let value = builder
            .build()
            .unwrap_or_else(|_| panic!("configuration sources cannot be merged"));
        let properties = Self::deserialize(value).unwrap_or_else(|error| panic!("{error}"));
        properties
            .validate()
            .expect("application configuration constraints failed");
        tracing::info!(service_name = %properties.service_name, "Configuration loaded");
        AppConfig::new(&properties)
    }

    pub fn deserialize(value: SourceConfig) -> Result<AppConfigProperties> {
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

    pub fn normalize_environment(mut value: serde_json::Value) -> Result<serde_json::Value> {
        if let Some(nodes) = value.pointer_mut("/cache/redis/nodes") {
            let text = nodes
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("cache.redis.nodes must be a comma-separated string"))?;
            let values = text
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| serde_json::Value::String(item.to_owned()))
                .collect::<Vec<_>>();
            anyhow::ensure!(!values.is_empty(), "cache.redis.nodes cannot be empty");
            *nodes = serde_json::Value::Array(values);
        }
        Self::normalize(value)
    }

    pub fn normalize(value: serde_json::Value) -> Result<serde_json::Value> {
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
    CONFIG.load().clone()
}

pub fn refresh_config() -> Result<(), anyhow::Error> {
    CONFIG.store(ConfigLoader::load());
    Ok(())
}
