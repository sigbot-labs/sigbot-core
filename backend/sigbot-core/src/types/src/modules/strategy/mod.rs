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

use crate::modules::messaging::messaging::MessagingInfo;
use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

pub mod models;
pub mod strategy;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct SigbotStrategyArgument {
    /// System-level environment variables set during strategy runner pod startup
    pub sys_environment: Option<HashMap<String, String>>,
    /// Execution mode: "STREAMING" (streaming) or "BATCH" (batch processing)
    pub run_mode: String,
    /// Messaging configuration for communication with the strategy runner
    pub messaging_config: Arc<MessagingInfo>,
}

impl SigbotStrategyArgument {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).context(format!("Failed to parse strategy info from JSON. - {}", json))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_from_json() {
        let json = r#"{
            "sys_environment": {"SIGBOT_VERSION": "V1.0.0"}}, 
            "messaging_config": {"name": "tenant101_messaging", "provider": "MQTT", "configuration": {"endpoint": "https://localhost:1883"}, "secrets": {"api_secret": "1234567890"}}}
            "#;
        let argument = SigbotStrategyArgument::from_json(json).unwrap();
        assert_eq!(
            argument.sys_environment,
            Some(HashMap::from([("SIGBOT_VERSION".to_string(), "V1.0.0".to_string())]))
        );
        assert_eq!(argument.messaging_config.name, Some("tenant101_messaging".to_string()));
        assert_eq!(
            argument.messaging_config.configuration,
            Some(HashMap::from([(
                "endpoint".to_string(),
                "https://localhost:1883".to_string()
            )]))
        );
    }
}
