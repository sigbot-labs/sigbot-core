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

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::option::Option;

pub fn copy_properties<T: Serialize, U: for<'de> Deserialize<'de>>(
    dst: &mut U,
    src: &T,
) -> Result<(), serde_json::Error>
where
    U: Serialize + Clone,
{
    copy_properties_with_map(dst, src, None)
}

pub fn copy_properties_with_map<T: Serialize, U: for<'de> Deserialize<'de>>(
    dst: &mut U,
    src: &T,
    fields_map: Option<&HashMap<String, String>>,
) -> Result<(), serde_json::Error>
where
    U: Serialize + Clone,
{
    let src_value = serde_json::to_value(src)?;
    let dst_value = serde_json::to_value(&dst)?;

    if let (Value::Object(src_map), Value::Object(mut dst_map)) = (src_value, dst_value) {
        if let Some(map) = fields_map {
            for (src_key, dst_key) in map.iter() {
                if let Some(value) = src_map.get(src_key) {
                    dst_map.insert(dst_key.clone(), value.clone());
                }
            }
        } else {
            // If no fields_map is provided, copy all matching fields
            for (key, value) in src_map.iter() {
                if dst_map.contains_key(key) {
                    dst_map.insert(key.clone(), value.clone());
                }
            }
        }
        *dst = serde_json::from_value(Value::Object(dst_map))?;
    }

    Ok(())
}

// --- Deserialization helpers for environment variables. ---
// These functions allow deserializing numeric types from either strings or numbers,
// which is useful when reading configuration from environment variables (which are
// always strings) or from YAML/JSON files (which can be numbers).

/// Deserialize u16 from string or number (for environment variable support)
///
/// This is useful when reading configuration from environment variables where
/// numeric values are represented as strings (e.g., `PORT=8080`).
///
/// # Example
/// ```rust
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Config {
///     #[serde(deserialize_with = "sigbot_utils::serde_beans::deserialize_u16_from_string_or_number")]
///     port: u16,
/// }
/// ```
pub fn deserialize_u16_from_string_or_number<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    struct U16Visitor;

    impl<'de> de::Visitor<'de> for U16Visitor {
        type Value = u16;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a u16 number or a string containing a u16 number")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value > u16::MAX as u64 {
                Err(E::custom(format!("value {} exceeds u16::MAX", value)))
            } else {
                Ok(value as u16)
            }
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 || value > u16::MAX as i64 {
                Err(E::custom(format!("value {} is out of range for u16", value)))
            } else {
                Ok(value as u16)
            }
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<u16>()
                .map_err(|_| E::custom(format!("failed to parse '{}' as u16", value)))
        }
    }

    deserializer.deserialize_any(U16Visitor)
}

/// Deserialize Option<u32> from string or number (for environment variable support)
///
/// This is useful when reading configuration from environment variables where
/// numeric values are represented as strings (e.g., `MAX_CONNECTIONS=100`).
///
/// # Example
/// ```rust
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Config {
///     #[serde(deserialize_with = "sigbot_utils::serde_beans::deserialize_option_u32_from_string_or_number")]
///     max_connections: Option<u32>,
/// }
/// ```
pub fn deserialize_option_u32_from_string_or_number<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionU32Visitor;

    impl<'de> de::Visitor<'de> for OptionU32Visitor {
        type Value = Option<u32>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an optional u32 number or a string containing a u32 number")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            Ok(Some(deserializer.deserialize_any(U32Visitor)?))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value > u32::MAX as u64 {
                Err(E::custom(format!("value {} exceeds u32::MAX", value)))
            } else {
                Ok(Some(value as u32))
            }
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 || value > u32::MAX as i64 {
                Err(E::custom(format!("value {} is out of range for u32", value)))
            } else {
                Ok(Some(value as u32))
            }
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<u32>()
                .map(Some)
                .map_err(|_| E::custom(format!("failed to parse '{}' as u32", value)))
        }
    }

    struct U32Visitor;

    impl<'de> de::Visitor<'de> for U32Visitor {
        type Value = u32;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a u32 number or a string containing a u32 number")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value > u32::MAX as u64 {
                Err(E::custom(format!("value {} exceeds u32::MAX", value)))
            } else {
                Ok(value as u32)
            }
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 || value > u32::MAX as i64 {
                Err(E::custom(format!("value {} is out of range for u32", value)))
            } else {
                Ok(value as u32)
            }
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<u32>()
                .map_err(|_| E::custom(format!("failed to parse '{}' as u32", value)))
        }
    }

    deserializer.deserialize_any(OptionU32Visitor)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    use serde::{Deserialize, Serialize};
    use validator::Validate;

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Validate, Default)]
    pub struct TestBaseBean {
        pub id: Option<i64>,
        pub status: Option<i8>,
        pub created_by: Option<String>,
        pub created_at: Option<i64>,
        pub updated_by: Option<String>,
        pub updated_at: Option<i64>,
        #[serde(skip)]
        pub del_flag: Option<i32>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Validate, Default)]
    pub struct TestInternalUser {
        pub base: TestBaseBean,
        #[validate(length(min = 1, max = 64))]
        pub name: String,
        #[validate(email)]
        #[validate(length(min = 1, max = 64))]
        pub email: Option<String>,
        pub phone: Option<String>,
        pub password: Option<String>,
        pub lang: Option<String>,
        pub oidc_claims_sub: Option<String>,
        pub oidc_claims_name: Option<String>,
        pub oidc_claims_email: Option<String>,
        pub あ: bool,
        pub address: TestCommonAddress,
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Validate, Default)]
    pub struct TestExternalUser {
        #[validate(length(min = 1, max = 64))]
        pub name: String,
        #[validate(email)]
        #[validate(length(min = 1, max = 64))]
        pub email: Option<String>,
        pub phone: Option<String>,
        pub oidc_claims_sub: Option<String>,
        pub oidc_claims_name: Option<String>,
        pub oidc_claims_email: Option<String>,
        pub あ: bool,
        pub address: TestCommonAddress,
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Validate, Default)]
    pub struct TestCommonAddress {
        #[validate(length(min = 1, max = 64))]
        pub street: String,
        #[validate(length(min = 1, max = 64))]
        pub city: String,
        #[validate(length(min = 1, max = 64))]
        pub state: String,
        #[validate(length(min = 1, max = 64))]
        pub country: String,
        #[validate(range(min = 1, max = 64))]
        pub zip: u32,
    }

    #[test]
    fn test_internal_from_external() {
        let external_user = TestExternalUser {
            name: "Sally".to_string(),
            email: Some("jack@gmail.com".to_string()),
            phone: Some(String::from("+1-555-555-5555")),
            oidc_claims_sub: Some(String::from("100101")),
            oidc_claims_name: Some(String::from("James")),
            oidc_claims_email: Some(String::from("tom@gmail.com")),
            あ: true,
            address: TestCommonAddress {
                street: "123 Main St".to_string(),
                city: "Anytown".to_string(),
                state: "CA".to_string(),
                zip: 12345,
                country: "USA".to_string(),
            },
        };

        let expected = TestInternalUser {
            base: TestBaseBean::default(),
            name: "Sally".to_string(),
            email: Some("jack@gmail.com".to_string()),
            phone: Some(String::from("+1-555-555-5555")),
            password: None,
            lang: None,
            oidc_claims_sub: Some(String::from("100101")),
            oidc_claims_name: Some(String::from("James")),
            oidc_claims_email: Some(String::from("tom@gmail.com")),
            あ: true,
            address: TestCommonAddress {
                street: "123 Main St".to_string(),
                city: "Anytown".to_string(),
                state: "CA".to_string(),
                zip: 12345,
                country: "USA".to_string(),
            },
        };

        let user = &mut TestInternalUser::default();
        copy_properties(user, &external_user).unwrap();
        let user = user;
        assert_eq!(expected, user.to_owned());
    }

    #[test]
    fn test_internal_to_external() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        let internal_user = TestInternalUser {
            base: TestBaseBean {
                id: Some(1001),
                status: Some(1),
                created_by: Some(String::from("admin")),
                created_at: Some(now),
                updated_by: Some(String::from("admin")),
                updated_at: Some(now),
                del_flag: Some(0),
            },
            name: "Sally".to_string(),
            email: Some("jack@gmail.com".to_string()),
            phone: Some(String::from("+1-555-555-5555")),
            password: None,
            lang: None,
            oidc_claims_sub: Some(String::from("100101")),
            oidc_claims_name: Some(String::from("James")),
            oidc_claims_email: Some(String::from("tom@gmail.com")),
            あ: true,
            address: TestCommonAddress {
                street: "123 Main St".to_string(),
                city: "Anytown".to_string(),
                state: "CA".to_string(),
                zip: 12345,
                country: "USA".to_string(),
            },
        };

        let expected = TestExternalUser {
            name: "Sally".to_string(),
            email: Some("jack@gmail.com".to_string()),
            phone: Some(String::from("+1-555-555-5555")),
            oidc_claims_sub: Some(String::from("100101")),
            oidc_claims_name: Some(String::from("James")),
            oidc_claims_email: Some(String::from("tom@gmail.com")),
            あ: true,
            address: TestCommonAddress {
                street: "123 Main St".to_string(),
                city: "Anytown".to_string(),
                state: "CA".to_string(),
                zip: 12345,
                country: "USA".to_string(),
            },
        };

        let user = &mut TestExternalUser::default();
        // TODO: Notice: Currently, field assignment in nested structures is not supported, only shallow copy assignment is supported.
        copy_properties(user, &internal_user).unwrap();
        assert_eq!(expected, user.to_owned());
    }
}
