// SPDX-License-Identifier: GPL-3.0-only

use sigbot_strategy_runner::python::worker::worker::StrategyWorker;

fn main() -> anyhow::Result<()> {
    StrategyWorker::run()
}
