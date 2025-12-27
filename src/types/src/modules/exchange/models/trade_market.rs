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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KlineModel {
    // open time in milliseconds
    pub open_time: u64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: f64,
    pub close_time: u64, // close time in milliseconds (optional)
}

#[derive(Clone, Debug)]
pub struct PriceModel {
    pub price: f64,
    pub time: u64,
}

/// Symbol search result model
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolInfo {
    /// Symbol/contract identifier (e.g., "AAPL" for stocks, conid for IBKR)
    pub symbol: String,
    /// Contract ID (IBKR specific) or exchange-specific identifier
    pub contract_id: Option<String>,
    /// Company/asset name
    pub name: Option<String>,
    /// Exchange name
    pub exchange: Option<String>,
    /// Security type (e.g., "STK", "OPT", "FUT")
    pub sec_type: Option<String>,
    /// Currency
    pub currency: Option<String>,
    /// Additional description
    pub description: Option<String>,
}

/// Order information model
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderInfo {
    /// Exchange order ID
    pub order_id: String,
    /// Symbol/contract identifier
    pub symbol: String,
    /// Order side (BUY/SELL)
    pub side: String,
    /// Order type (MARKET/LIMIT/etc)
    pub order_type: String,
    /// Order status (Submitted, Filled, Cancelled, etc)
    pub status: String,
    /// Quantity
    pub quantity: f64,
    /// Filled quantity
    pub filled_quantity: Option<f64>,
    /// Remaining quantity
    pub remaining_quantity: Option<f64>,
    /// Price (for limit orders)
    pub price: Option<f64>,
    /// Average fill price
    pub avg_price: Option<f64>,
    /// Order creation time (timestamp in milliseconds)
    pub create_time: Option<u64>,
    /// Last update time (timestamp in milliseconds)
    pub update_time: Option<u64>,
    /// Time in force (DAY, GTC, etc)
    pub time_in_force: Option<String>,
}
