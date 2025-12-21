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

use crate::modules::{messager::messager::MessagerConfiguration, notification::notification::NotificationInfo};
use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod notification;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct SigbotNotificationArgument {
    pub messager_config: Arc<MessagerConfiguration>,
    pub notification_config: Arc<NotificationInfo>,
}

impl SigbotNotificationArgument {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).context(format!("Failed to parse notification info from JSON. - {}", json))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_from_json() {
        let json = r#"{
            "messager_config": {"name": "tenant101_messager", "provider": "MQTT", "configuration": {"endpoint": "https://localhost:1883"}, "secrets": {"api_secret": "1234567890"}}}
            "notification_config": {"name": "tenant101_email", "provider": "EMAIL", "configuration": {"endpoint": "smtp.gmail.com:587"}, "secrets": {"api_secret": "1234567890"}}, 
            "#;
        let argument = SigbotNotificationArgument::from_json(json).unwrap();
        assert_eq!(argument.messager_config.name, Some("MQTT".to_string()));
        assert_eq!(
            argument.messager_config.configuration,
            Some(HashMap::from([(
                "endpoint".to_string(),
                "https://localhost:1883".to_string()
            )]))
        );
    }
}
