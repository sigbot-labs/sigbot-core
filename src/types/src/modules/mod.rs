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

use anyhow::{Error, Result};
use sigbot_utils::base64s;

pub mod backtest;
pub mod datafeed;
pub mod exchange;
pub mod messager;
pub mod notification;
pub mod order;
pub mod strategy;
pub mod wallet;
pub mod workflow;

pub fn decode_arg_config(configuration: &str) -> Result<String, Error> {
    // if there are base64 encoded, decode it first, otherwise return the original string.
    match base64s::Base64Helper::decode(configuration) {
        Ok(decoded_bytes) => {
            // Successfully decoded, try to convert to UTF-8 string
            match String::from_utf8(decoded_bytes) {
                Ok(decoded_str) => Ok(decoded_str),
                Err(_) => {
                    // Decoded but not valid UTF-8, return original string
                    Err(Error::msg(format!(
                        "Failed to convert the configuration to string: {}",
                        configuration
                    )))
                }
            }
        }
        Err(_) => {
            // Not base64 encoded, return original string
            Ok(configuration.to_string())
        }
    }
}
