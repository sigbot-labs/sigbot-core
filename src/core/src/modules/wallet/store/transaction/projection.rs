// SPDX-License-Identifier: GPL-3.0-only
//! Deterministic financial projection shared by database adapters.

use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use sigbot_types::modules::order::events::SigbotTradeEvent;
use sigbot_types::modules::order::{execution::ExecutionFact, intent::OrderSide};
use std::{collections::BTreeMap, str::FromStr};

pub struct TradeProjection {
    pub price: Decimal,
    pub quantity: Decimal,
    pub quote_quantity: Decimal,
    pub fee: Decimal,
    pub base_asset: String,
    pub quote_asset: String,
    pub fee_asset: String,
    pub buy: bool,
}

impl TradeProjection {
    pub fn from_execution(event: &ExecutionFact) -> Result<Self> {
        event.validate()?;
        let quote_quantity = match event.quote_quantity {
            Some(quantity) => quantity,
            None => event
                .price
                .checked_mul(event.quantity)
                .context("execution notional overflow")?,
        };
        Ok(Self {
            price: event.price,
            quantity: event.quantity,
            quote_quantity,
            fee: event.fee,
            base_asset: event.base_asset.clone(),
            quote_asset: event.quote_asset.clone(),
            fee_asset: event.fee_asset.clone(),
            buy: event.side == OrderSide::Buy,
        })
    }
    pub fn from_event(event: &SigbotTradeEvent) -> Result<Self> {
        ensure!(
            event.wallet_id > 0 && event.order_id > 0 && event.trade_id >= 0 && event.ts >= 0,
            "execution identifiers or timestamp are invalid"
        );
        ensure!(
            !event.tenant_id.is_empty() && !event.signal_id.is_empty(),
            "execution identity is required"
        );
        ensure!(
            event.side == "BUY" || event.side == "SELL",
            "execution side must be BUY or SELL"
        );
        let price = Decimal::from_str(&event.price.to_string()).context("invalid execution price")?;
        let quantity = Decimal::from_str(&event.qty.to_string()).context("invalid execution quantity")?;
        let fee = Decimal::from_str(&event.fee.to_string()).context("invalid execution fee")?;
        ensure!(
            price > Decimal::ZERO && quantity > Decimal::ZERO && fee >= Decimal::ZERO,
            "execution price/quantity must be positive; fee cannot be negative"
        );
        let (base_asset, quote_asset) = Self::parse_symbol(&event.symbol)?;
        let fee_asset = event.fee_asset.clone().unwrap_or_else(|| quote_asset.clone());
        ensure!(!fee_asset.is_empty(), "fee asset cannot be empty");
        Ok(Self {
            price,
            quantity,
            quote_quantity: price.checked_mul(quantity).context("execution notional overflow")?,
            fee,
            base_asset,
            quote_asset,
            fee_asset,
            buy: event.side == "BUY",
        })
    }

    pub fn movements(&self) -> Result<Vec<(String, Decimal, &'static str)>> {
        let sign = if self.buy { Decimal::ONE } else { -Decimal::ONE };
        let mut movements = vec![
            (self.base_asset.clone(), sign * self.quantity, "clearing"),
            (self.quote_asset.clone(), -sign * self.quote_quantity, "clearing"),
        ];
        if self.fee > Decimal::ZERO {
            movements.push((self.fee_asset.clone(), -self.fee, "fee"));
        }
        Ok(movements)
    }

    pub fn balance_deltas(&self) -> Result<BTreeMap<String, Decimal>> {
        let mut deltas = BTreeMap::new();
        for (asset, amount, _) in self.movements()? {
            let previous = deltas.entry(asset).or_insert(Decimal::ZERO);
            *previous = previous.checked_add(amount).context("balance delta overflow")?;
        }
        Ok(deltas)
    }

    fn parse_symbol(symbol: &str) -> Result<(String, String)> {
        if let Some((base, quote)) = symbol.split_once('/').or_else(|| symbol.split_once('-')) {
            ensure!(!base.is_empty() && !quote.is_empty(), "invalid instrument symbol");
            return Ok((base.to_owned(), quote.to_owned()));
        }
        for quote in ["USDT", "USDC", "BUSD", "USD", "EUR", "BTC", "ETH", "BNB"] {
            if let Some(base) = symbol.strip_suffix(quote).filter(|base| !base.is_empty()) {
                return Ok((base.to_owned(), quote.to_owned()));
            }
        }
        anyhow::bail!("instrument requires explicit base/quote metadata: {}", symbol)
    }
}

#[derive(Default, Debug)]
pub struct PositionProjection {
    pub size: Decimal,
    pub entry_price: Decimal,
    /// Remaining gross quote cost/proceeds. Keep this amount instead of
    /// reconstructing it from a rounded average entry price.
    pub cost_basis: Decimal,
    pub realized_pnl: Decimal,
}

impl PositionProjection {
    /// Replay in (event_time, exchange_trade_id, order_id, trade_id) order.
    pub fn apply(&mut self, buy: bool, price: Decimal, quantity: Decimal) -> Result<()> {
        let quote_quantity = price.checked_mul(quantity).context("position notional overflow")?;
        self.apply_quote(buy, price, quantity, quote_quantity)
    }

    pub fn apply_quote(&mut self, buy: bool, price: Decimal, quantity: Decimal, quote_quantity: Decimal) -> Result<()> {
        ensure!(
            price > Decimal::ZERO && quantity > Decimal::ZERO && quote_quantity > Decimal::ZERO,
            "invalid position fill"
        );
        let delta = if buy { quantity } else { -quantity };
        let old_size = self.size;
        let new_size = old_size.checked_add(delta).context("position size overflow")?;
        let mut realized_pnl = self.realized_pnl;
        let cost_basis = if old_size.is_zero() || old_size.is_sign_positive() == delta.is_sign_positive() {
            self.cost_basis
                .checked_add(quote_quantity)
                .context("position cost overflow")?
        } else {
            let closed = old_size.abs().min(quantity);
            // Allocating the entire remaining amount on a full close ensures
            // that earlier proportional rounding never leaves residual cost.
            let closed_cost = Self::allocate(self.cost_basis, closed, old_size.abs())?;
            let closed_quote = Self::allocate(quote_quantity, closed, quantity)?;
            let direction = if old_size.is_sign_positive() {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let pnl = closed_quote
                .checked_sub(closed_cost)
                .and_then(|value| value.checked_mul(direction))
                .context("realized pnl overflow")?;
            realized_pnl = realized_pnl.checked_add(pnl).context("realized pnl overflow")?;
            if new_size.is_zero() {
                Decimal::ZERO
            } else if old_size.is_sign_positive() != new_size.is_sign_positive() {
                quote_quantity
                    .checked_sub(closed_quote)
                    .context("reversal cost overflow")?
            } else {
                self.cost_basis
                    .checked_sub(closed_cost)
                    .context("remaining cost overflow")?
            }
        };
        let entry_price = if new_size.is_zero() {
            Decimal::ZERO
        } else {
            cost_basis
                .checked_div(new_size.abs())
                .context("average entry price overflow")?
        };
        self.entry_price = entry_price;
        self.cost_basis = cost_basis;
        self.realized_pnl = realized_pnl;
        self.size = new_size;
        Ok(())
    }

    fn allocate(total: Decimal, part: Decimal, whole: Decimal) -> Result<Decimal> {
        if part == whole {
            Ok(total)
        } else {
            total
                .checked_mul(part)
                .and_then(|amount| amount.checked_div(whole))
                .context("position cost allocation overflow")
        }
    }
}
