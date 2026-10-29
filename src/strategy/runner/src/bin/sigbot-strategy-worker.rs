// SPDX-License-Identifier: GPL-3.0-only
use sigbot_strategy_runner::executor::pyexec::worker::StrategyWorker;

fn main() -> anyhow::Result<()> {
    StrategyWorker::run()
}
