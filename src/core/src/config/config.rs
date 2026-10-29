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
use config::Config;
use dotenv::dotenv;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{env, ops::Deref, sync::Arc};
use validator::Validate;

pub use super::{authguard::*, llm::*, platform::*, services::*, storage::*};

// Global program information.
pub const GIT_VERSION: &str = env!("GIT_VERSION");
pub const GIT_COMMIT_HASH: &str = env!("GIT_COMMIT_HASH");
pub const GIT_BUILD_DATE: &str = env!("GIT_BUILD_DATE");

// Global static resources.
pub const DEFAULT_INDEX_HTML: &str = include_str!("../../../../static/index.html");
pub const DEFAULT_404_HTML: &str = include_str!("../../../../static/404.html");
pub const DEFAULT_403_HTML: &str = include_str!("../../../../static/403.html");

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
    #[serde(alias = "service-name")]
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
    #[serde(default = "AppDBProperties::default")]
    pub appdb: AppDBProperties,
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
            appdb: AppDBProperties::default(),
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
        let mut inner = config.clone();
        inner.appdb.normalize_provider();
        Arc::new(AppConfig { inner })
    }

    pub fn validate(self) -> Result<AppConfig, anyhow::Error> {
        self.inner.validate()?;
        Ok(self)
    }
}

// Global the single refreshable configuration instance.
// see: https://github.com/wl4g-collect/openobserve/blob/v0.10.9/src/config/src/config.rs#L186
static CONFIG: Lazy<ArcSwap<AppConfig>> = Lazy::new(|| ArcSwap::from(init()));

/// Sole configuration loading entry; public wrappers preserve the existing API.
pub struct ConfigLoader;
impl ConfigLoader {
    fn load(custom_config_json: Option<&str>) -> Arc<AppConfig> {
        dotenv().ok();
        let defaults =
            serde_json::to_string(&AppConfigProperties::default()).expect("configuration defaults must serialize");
        let mut builder = Config::builder().add_source(config::File::from_str(&defaults, config::FileFormat::Json));
        if let Ok(path) = env::var("SIGBOT_CFG_PATH") {
            if !path.trim().is_empty() {
                let value = Config::builder()
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
        builder = builder.add_source(
            config::Environment::with_prefix("SIGBOT")
                .separator("__")
                .prefix_separator("__")
                .keep_prefix(false),
        );
        // Errors may contain supplied values; never print raw deserialization errors.
        let value = builder
            .build()
            .unwrap_or_else(|_| panic!("configuration sources cannot be merged"));
        let mut properties = Self::deserialize(value).unwrap_or_else(|error| panic!("{error}"));
        if let Some(custom) = custom_config_json {
            if let Some(ref configurer) = **CUSTOM_CONFIGURER.load() {
                properties = configurer
                    .configure(&properties, custom)
                    .unwrap_or_else(|_| panic!("tenant configuration validation failed"));
            }
        }
        properties
            .validate()
            .expect("application configuration constraints failed");
        tracing::info!(service_name = %properties.service_name, "Configuration loaded");
        AppConfig::new(&properties)
    }

    pub fn deserialize(value: Config) -> Result<AppConfigProperties> {
        let mut properties: AppConfigProperties = serde_path_to_error::deserialize(value).map_err(|error| {
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
        properties.appdb.normalize_provider();
        Ok(properties)
    }

    pub fn normalize(value: serde_json::Value) -> Result<serde_json::Value> {
        use serde_json::Value;
        Ok(match value {
            Value::Object(object) => {
                let mut result = serde_json::Map::new();
                for (key, value) in object {
                    let canonical = key.replace('-', "_");
                    anyhow::ensure!(
                        !result.contains_key(&canonical),
                        "duplicate canonical configuration key"
                    );
                    let value = if matches!(canonical.as_str(), "tags" | "input_schema") {
                        value
                    } else {
                        Self::normalize(value)?
                    };
                    result.insert(canonical, value);
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

pub(crate) fn init() -> Arc<AppConfig> {
    ConfigLoader::load(None)
}
pub fn get_config() -> Arc<AppConfig> {
    CONFIG.load().clone()
}
pub fn refresh_config() -> Result<(), anyhow::Error> {
    CONFIG.store(init());
    Ok(())
}

pub trait CustomConfigConfigurer: Send + Sync {
    fn configure(&self, base_config: &AppConfigProperties, custom_config_json: &str) -> Result<AppConfigProperties>;
}
pub(crate) static CUSTOM_CONFIGURER: Lazy<ArcSwap<Option<Arc<dyn CustomConfigConfigurer>>>> =
    Lazy::new(|| ArcSwap::from(Arc::new(None)));
pub fn register_custom_configurer(configurer: Arc<dyn CustomConfigConfigurer>) {
    CUSTOM_CONFIGURER.store(Arc::new(Some(configurer)));
}
pub(crate) fn init_with_custom(custom_config_json: &str) -> Arc<AppConfig> {
    ConfigLoader::load(Some(custom_config_json))
}
