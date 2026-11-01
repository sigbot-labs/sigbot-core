// SPDX-License-Identifier: GPL-3.0-only
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;

pub use sigbot_types::exchange::instrument::InstrumentSpec as InstrumentRules;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskLimits {
    pub max_order_notional: Decimal,
    pub max_account_notional: Decimal,
    pub max_position_quantity: Decimal,
    pub max_market_age_ms: i64,
    pub fee_buffer_bps: Decimal,
}

pub struct RiskPolicy {
    pub limits: RiskLimits,
    pub instrument: InstrumentRules,
}

#[derive(Debug)]
pub struct OrderRiskRejection(pub String);
impl std::fmt::Display for OrderRiskRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for OrderRiskRejection {}

impl RiskPolicy {
    pub fn check_mode(wallet: ExecutionMode, adapter: ExecutionMode) -> Result<()> {
        if wallet != adapter {
            return Err(OrderRiskRejection("wallet execution mode differs from the exchange adapter".into()).into());
        }
        Ok(())
    }

    pub fn check(
        &self,
        intent: &TradeIntent,
        now: DateTime<Utc>,
        position: Decimal,
        pending_quantity: Decimal,
        account_notional: Decimal,
    ) -> Result<(String, Decimal)> {
        intent.validate(now, self.limits.max_market_age_ms)?;
        let rules = &self.instrument;
        ensure!(
            self.limits.max_order_notional > Decimal::ZERO
                && self.limits.max_account_notional >= self.limits.max_order_notional
                && self.limits.max_position_quantity > Decimal::ZERO
                && self.limits.fee_buffer_bps >= Decimal::ZERO
                && self.limits.fee_buffer_bps <= Decimal::from(10_000),
            "invalid hard risk limits"
        );
        ensure!(
            rules.exchange == intent.exchange && rules.symbol == intent.symbol,
            "unknown instrument rules"
        );
        ensure!(
            rules.quantity_step > Decimal::ZERO
                && rules.price_tick > Decimal::ZERO
                && rules.min_notional >= Decimal::ZERO
                && !rules.base_asset.is_empty()
                && !rules.quote_asset.is_empty()
                && rules.base_asset != rules.quote_asset,
            "invalid instrument precision/assets"
        );
        ensure!(
            pending_quantity >= Decimal::ZERO && account_notional >= Decimal::ZERO,
            "invalid exposure projection"
        );
        ensure!(
            intent.quantity % rules.quantity_step == Decimal::ZERO,
            "quantity violates exchange lot step"
        );
        if let Some(price) = intent.limit_price {
            ensure!(
                price % rules.price_tick == Decimal::ZERO,
                "price violates exchange tick"
            );
        }
        let price = intent.limit_price.unwrap_or(intent.market.price);
        let notional = price
            .checked_mul(intent.quantity)
            .ok_or_else(|| anyhow::anyhow!("notional overflow"))?;
        ensure!(
            notional >= rules.min_notional && notional <= self.limits.max_order_notional,
            "order notional outside hard limits"
        );
        let signed = if intent.side == OrderSide::Buy {
            intent.quantity
        } else {
            -intent.quantity
        };
        let target_position = position.checked_add(signed).context("position overflow")?;
        let reducing = intent.reduce_only
            && position != Decimal::ZERO
            && position.is_sign_positive() != signed.is_sign_positive()
            && intent.quantity <= position.abs();
        // A close must remain possible at the exposure ceiling. Pending orders
        // are already included in account_notional and separately bounded below.
        if !reducing {
            ensure!(
                account_notional
                    .checked_add(notional)
                    .context("account exposure overflow")?
                    <= self.limits.max_account_notional,
                "account exposure limit"
            );
        }
        ensure!(
            reducing
                || target_position
                    .abs()
                    .checked_add(pending_quantity)
                    .context("pending exposure overflow")?
                    <= self.limits.max_position_quantity,
            "position limit"
        );
        if intent.reduce_only {
            ensure!(
                position != Decimal::ZERO
                    && position.is_sign_positive() != signed.is_sign_positive()
                    && signed
                        .abs()
                        .checked_add(pending_quantity)
                        .context("reduce-only exposure overflow")?
                        <= position.abs(),
                "reduce-only intent would add or reverse exposure"
            );
        }
        let buffer = Decimal::ONE + self.limits.fee_buffer_bps / Decimal::from(10_000);
        Ok(if intent.side == OrderSide::Buy {
            (
                rules.quote_asset.clone(),
                notional.checked_mul(buffer).context("reservation overflow")?,
            )
        } else {
            // A spot sell delivers exactly the base quantity. Quote-denominated fees
            // are deducted from its proceeds, so buffering base would make closing
            // an entire position impossible. Non-quote fees require funded balances.
            (rules.base_asset.clone(), intent.quantity)
        })
    }
}
