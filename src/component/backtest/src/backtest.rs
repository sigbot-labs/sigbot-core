// SPDX-License-Identifier: GPL-3.0-only

use common_context::IProvider;
use sigbot_types::backtest::BacktestProvider;

pub trait IBacktest: IProvider {
    fn provider(&self) -> BacktestProvider;
}
