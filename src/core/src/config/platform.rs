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
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Server Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerProperties {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
    #[serde(alias = "context-path")]
    pub context_path: Option<String>,
}

// Management Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MgmtProperties {
    #[serde(rename = "enabled")]
    pub enabled: bool,
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
    #[serde(default = "TokioConsoleProperties::default", alias = "tokio-console")]
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
    //#[env_config(name = "MW_TOKIO_CONSOLE_SERVER_BIND", default = "0.0.0.0:6699")]
    #[serde(alias = "server-bind")]
    pub server_bind: String,
    pub retention: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PyroscopeAgentProperties {
    pub enabled: bool,
    #[serde(alias = "server-url")]
    pub server_url: String,
    #[serde(alias = "auth-token")]
    pub auth_token: Option<String>,
    pub tags: Option<Vec<(String, String)>>,
    #[serde(alias = "sample-rate")]
    pub sample_rate: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct OtelProperties {
    pub enabled: bool,
    pub endpoint: String,
    pub protocol: String,
    pub timeout: Option<u64>,
    // Notice: More OTEL custom configuration use to environment: OTEL_SPAN_xxx, see to: opentelemetry_sdk::trace::config::default()
}

// Logging Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct LoggingProperties {
    pub mode: LogMode,
    pub level: String,
}

// Swagger Properties.

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SwaggerProperties {
    pub enabled: bool,
    // pub title: String,
    // pub description: String,
    // pub version: String,
    // pub license_name: String,
    // pub license_url: String,
    // pub contact_name: String,
    // pub contact_email: String,
    // pub contact_url: String,
    // pub terms_of_service: String,
    // //pub security_definitions: vec![],
    pub ui_path: String,
    pub openapi_url: String,
}

impl Default for ServerProperties {
    fn default() -> Self {
        ServerProperties {
            host: String::from("127.0.0.1"),
            port: 9000,
            context_path: None,
        }
    }
}

impl ServerProperties {
    pub fn get_bind_addr(&self) -> String {
        self.host.to_owned() + ":" + &self.port.to_string()
    }
}

// Management Properties impls.

impl Default for MgmtProperties {
    fn default() -> Self {
        MgmtProperties {
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
        self.host.to_owned() + ":" + &self.port.to_string()
    }
}

impl Default for TokioConsoleProperties {
    fn default() -> Self {
        TokioConsoleProperties {
            enabled: true,
            server_bind: String::from("0.0.0.0:6669"),
            retention: 60,
        }
    }
}

impl Default for PyroscopeAgentProperties {
    fn default() -> Self {
        PyroscopeAgentProperties {
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
        OtelProperties {
            enabled: true,
            endpoint: String::from("http://localhost:4317"),
            protocol: String::from("grpc"),
            timeout: Some(Duration::from_secs(10).as_millis() as u64),
        }
    }
}

// Logging Properties impls.

impl Default for LoggingProperties {
    fn default() -> Self {
        LoggingProperties {
            mode: LogMode::HUMAN,
            level: "info".to_string(),
        }
    }
}

impl LoggingProperties {
    pub fn is_human_mode(&self) -> bool {
        self.mode == LogMode::HUMAN
    }
}

// Swagger Properties impls.

impl Default for SwaggerProperties {
    fn default() -> Self {
        SwaggerProperties {
            enabled: true,
            // title: "My Webnote API Server".to_string(),
            // description: "The My Webnote API Server".to_string(),
            // version: "1.0.0".to_string(),
            // license_name: "Apache 2.0".to_string(),
            // license_url: "https://www.apache.org/licenses/LICENSE-2.0".to_string(),
            // contact_name: "SigBot API".to_string(),
            // contact_email: "jameswong1376@gmail.com".to_string(),
            // contact_url: "https://github.com/wl4g/my-webnote".to_string(),
            // terms_of_service: "api/terms-of-service".to_string(),
            // //security_definitions: vec![],
            ui_path: "/swagger-ui".to_string(),
            openapi_url: "/api-docs/openapi.json".to_string(),
        }
    }
}
