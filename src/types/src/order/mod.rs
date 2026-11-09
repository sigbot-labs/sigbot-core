// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

pub mod execution;
pub mod execution_group;
pub mod intent;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum OrderProvider {
    #[serde(rename = "DEFAULT")]
    Default,
}

impl OrderProvider {
    pub fn of(provider: &str) -> Result<OrderProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "DEFAULT" => Ok(OrderProvider::Default),
            _ => Err(anyhow::anyhow!("Unsupported the order provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            OrderProvider::Default => "DEFAULT",
        }
    }
}
