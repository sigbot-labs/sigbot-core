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

#[derive(Clone, Debug)]
pub struct EntryTradePosition {
    pub open_pos: EntryPosition,
    pub stop_loss: Option<ExitTradePosition>,
    pub stop_profit: Option<ExitTradePosition>,
    pub description: String,
}

impl EntryTradePosition {
    pub fn validate(&self) -> Result<&EntryTradePosition, String> {
        self.open_pos.validate()?;
        if let Some(pos) = &self.stop_loss {
            pos.validate()?;
        }
        if let Some(pos) = &self.stop_profit {
            pos.validate()?;
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct EntryPosition {
    pub time: u64, // Trading signal referenced k-line price time.
    pub symbol: String,
    pub side: TradeSide,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub quantity: f64,
    pub maker_only: bool,
}

impl EntryPosition {
    pub fn validate(&self) -> Result<&EntryPosition, String> {
        if self.symbol.trim().is_empty() {
            return Err("Open position order must have symbol".to_string());
        }
        if self.quantity <= 0.0 {
            return Err("Open position order must be quantity > 0".to_string());
        }
        match self.order_type {
            OrderType::MARKET => {
                if self.price.is_some() {
                    return Err("Market order cannot have price".to_string());
                }
            }
            OrderType::LIMITED => match self.price {
                None => return Err("Limited order must have price".to_string()),
                Some(p) if p <= 0.0 => {
                    return Err("Limited order must be price > 0".to_string());
                }
                _ => {}
            },
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct ExitTradePosition {
    pub time: u64, // Trading signal referenced k-line price time.
    pub symbol: String,
    pub side: TradeSide,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub quantity_percent: f64,
}

impl ExitTradePosition {
    pub fn validate(&self) -> Result<&ExitTradePosition, String> {
        if self.symbol.trim().is_empty() {
            return Err("Stop position order must have symbol".to_string());
        }
        if self.quantity_percent <= 0.0 {
            return Err("Stop position order must be quantityPercent > 0".to_string());
        }
        match self.order_type {
            OrderType::MARKET => {
                if self.price.is_some() {
                    return Err("Market order cannot have price".to_string());
                }
            }
            OrderType::LIMITED => match self.price {
                None => return Err("Limited order must have price".to_string()),
                Some(p) if p <= 0.0 => {
                    return Err("Limited order must be price > 0".to_string());
                }
                _ => {}
            },
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub enum TradeSide {
    LONG,
    SHORT,
}

impl TradeSide {
    pub fn from_str(side: &str) -> Result<TradeSide, String> {
        match side.to_uppercase().as_str() {
            "BUY" | "LONG" => Ok(TradeSide::LONG),
            "SELL" | "SHORT" => Ok(TradeSide::SHORT),
            _ => Err("Invalid trade side".to_string()),
        }
    }

    pub fn to_side_str(&self) -> &str {
        match self {
            TradeSide::LONG => "BUY",
            TradeSide::SHORT => "SELL",
        }
    }

    pub fn to_pos_str(&self) -> &str {
        match self {
            TradeSide::LONG => "LONG",
            TradeSide::SHORT => "SHORT",
        }
    }
}

#[derive(Clone, Debug)]
pub enum OrderType {
    MARKET,
    LIMITED,
}

impl OrderType {
    pub fn from_str(order_type: &str) -> Result<OrderType, String> {
        match order_type.to_uppercase().as_str() {
            "MARKET" => Ok(OrderType::MARKET),
            "LIMITED" => Ok(OrderType::LIMITED),
            _ => Err("Invalid order type".to_string()),
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            OrderType::MARKET => "MARKET",
            OrderType::LIMITED => "LIMITED",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TradeResult {
    pub success: bool,
    pub order_id: u64,
    pub message: Option<String>,
    //pub trigger_signal: Option<TradeSignal>,
}
