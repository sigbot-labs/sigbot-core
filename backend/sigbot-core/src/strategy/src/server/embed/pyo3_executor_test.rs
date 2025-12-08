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

//! PyO3 策略执行器使用示例
//!
//! 本文件展示了如何使用 PyO3 执行器来执行 Python 策略代码。
//! 用户可以从前端提交 Python 代码字符串，系统会通过 PyO3 解释器执行。

use crate::server::embed::pyo3_executor::{PyO3StrategyExecutor, StrategyContext};
use serde_json;
use std::collections::HashMap;

/// 示例 1: 基本策略执行
///
/// 展示如何执行简单的 Python 代码
pub fn example_basic_execution() {
    let executor = PyO3StrategyExecutor::new();

    let code = r#"
# 简单的策略计算
price = 100.0
volume = 1000
total_value = price * volume

result = {
    "price": price,
    "volume": volume,
    "total_value": total_value
}
"#;

    match executor.execute(code, None) {
        Ok(result) => {
            println!("执行成功: {}", result.success);
            println!("结果: {:?}", result.result);
            println!("耗时: {}ms", result.duration_ms);
        }
        Err(e) => {
            eprintln!("执行失败: {}", e);
        }
    }
}

/// 示例 2: 使用 Polars 进行数据分析
///
/// 展示如何使用 Polars 库进行数据处理
/// 注意：需要先安装 polars: pip install polars
pub fn example_polars_strategy() {
    let executor = PyO3StrategyExecutor::new();

    // 检查 polars 是否已安装
    if !executor.check_package_installed("polars") {
        eprintln!("警告: polars 未安装，请运行: pip install polars");
        return;
    }

    let code = r#"
import polars as pl
import json

# 创建示例数据
data = {
    "timestamp": [1, 2, 3, 4, 5],
    "price": [100.0, 101.0, 99.0, 102.0, 101.5],
    "volume": [1000, 1100, 900, 1200, 1050]
}

# 使用 Polars 创建 DataFrame
df = pl.DataFrame(data)

# 计算移动平均
df = df.with_columns([
    pl.col("price").rolling_mean(window_size=3).alias("ma_3")
])

# 计算收益率
df = df.with_columns([
    (pl.col("price").pct_change() * 100).alias("return_pct")
])

# 转换为字典以便返回
result = df.to_dict(as_series=False)
"#;

    match executor.execute(code, None) {
        Ok(result) => {
            println!("Polars 策略执行成功");
            println!("结果: {}", result.result.unwrap_or_default());
        }
        Err(e) => {
            eprintln!("执行失败: {}", e);
        }
    }
}

/// 示例 3: 使用 Pandas 进行数据分析
///
/// 展示如何使用 Pandas 库进行数据处理
/// 注意：需要先安装 pandas: pip install pandas
pub fn example_pandas_strategy() {
    let executor = PyO3StrategyExecutor::new();

    // 检查 pandas 是否已安装
    if !executor.check_package_installed("pandas") {
        eprintln!("警告: pandas 未安装，请运行: pip install pandas");
        return;
    }

    let code = r#"
import pandas as pd
import json

# 创建示例数据
data = {
    "timestamp": [1, 2, 3, 4, 5],
    "price": [100.0, 101.0, 99.0, 102.0, 101.5],
    "volume": [1000, 1100, 900, 1200, 1050]
}

# 使用 Pandas 创建 DataFrame
df = pd.DataFrame(data)

# 计算技术指标
df["ma_3"] = df["price"].rolling(window=3).mean()
df["return_pct"] = df["price"].pct_change() * 100

# 转换为字典以便返回
result = df.to_dict(orient="records")
"#;

    match executor.execute(code, None) {
        Ok(result) => {
            println!("Pandas 策略执行成功");
            println!("结果: {}", result.result.unwrap_or_default());
        }
        Err(e) => {
            eprintln!("执行失败: {}", e);
        }
    }
}

/// 示例 4: 使用市场数据和策略参数
///
/// 展示如何传入市场数据和策略参数
pub fn example_with_context() {
    let executor = PyO3StrategyExecutor::new();

    let code = r#"
import json

# 从上下文获取市场数据
if market_data:
    data = json.loads(market_data) if isinstance(market_data, str) else market_data
    prices = data.get("prices", [])
    volumes = data.get("volumes", [])
else:
    prices = []
    volumes = []

# 从上下文获取策略参数
ma_period = int(parameters.get("ma_period", "5"))
threshold = float(parameters.get("threshold", "0.02"))

# 简单的移动平均策略
if len(prices) >= ma_period:
    recent_prices = prices[-ma_period:]
    ma = sum(recent_prices) / len(recent_prices)
    current_price = prices[-1]
    
    # 计算信号
    if current_price > ma * (1 + threshold):
        signal = "BUY"
    elif current_price < ma * (1 - threshold):
        signal = "SELL"
    else:
        signal = "HOLD"
    
    result = {
        "signal": signal,
        "current_price": current_price,
        "ma": ma,
        "ma_period": ma_period
    }
else:
    result = {
        "signal": "HOLD",
        "reason": "Insufficient data"
    }
"#;

    // 准备上下文数据
    let market_data = serde_json::json!({
        "prices": [100.0, 101.0, 99.0, 102.0, 101.5, 103.0, 104.0],
        "volumes": [1000, 1100, 900, 1200, 1050, 1300, 1150]
    });

    let mut parameters = HashMap::new();
    parameters.insert("ma_period".to_string(), "5".to_string());
    parameters.insert("threshold".to_string(), "0.02".to_string());

    let context = StrategyContext {
        market_data: Some(serde_json::to_string(&market_data).unwrap()),
        parameters: Some(parameters),
        extra_data: None,
    };

    match executor.execute(code, Some(context)) {
        Ok(result) => {
            println!("带上下文的策略执行成功");
            println!("结果: {}", result.result.unwrap_or_default());
        }
        Err(e) => {
            eprintln!("执行失败: {}", e);
        }
    }
}

/// 示例 5: 复杂的量化策略
///
/// 展示一个更复杂的策略，结合多个技术指标
pub fn example_complex_strategy() {
    let executor = PyO3StrategyExecutor::new();

    let code = r#"
import json
import math

# 解析市场数据
if market_data:
    data = json.loads(market_data) if isinstance(market_data, str) else market_data
    prices = data.get("prices", [])
    volumes = data.get("volumes", [])
else:
    prices = [100.0, 101.0, 99.0, 102.0, 101.5, 103.0, 104.0, 103.5, 105.0]
    volumes = [1000, 1100, 900, 1200, 1050, 1300, 1150, 1200, 1250]

# 获取策略参数
short_period = int(parameters.get("short_period", "3"))
long_period = int(parameters.get("long_period", "5"))

# 计算短期和长期移动平均
if len(prices) >= long_period:
    short_ma = sum(prices[-short_period:]) / short_period
    long_ma = sum(prices[-long_period:]) / long_period
    
    # 计算 RSI (简化版)
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    avg_gain = sum(gains[-14:]) / 14 if len(gains) >= 14 else 0
    avg_loss = sum(losses[-14:]) / 14 if len(losses) >= 14 else 0
    
    if avg_loss > 0:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = 100
    
    # 生成交易信号
    signal = "HOLD"
    reason = []
    
    if short_ma > long_ma:
        reason.append("MA crossover bullish")
    else:
        reason.append("MA crossover bearish")
    
    if rsi > 70:
        reason.append("RSI overbought")
        signal = "SELL"
    elif rsi < 30:
        reason.append("RSI oversold")
        signal = "BUY"
    
    if short_ma > long_ma and rsi < 50:
        signal = "BUY"
    elif short_ma < long_ma and rsi > 50:
        signal = "SELL"
    
    result = {
        "signal": signal,
        "reasons": reason,
        "indicators": {
            "short_ma": short_ma,
            "long_ma": long_ma,
            "rsi": rsi,
            "current_price": prices[-1]
        }
    }
else:
    result = {
        "signal": "HOLD",
        "reason": "Insufficient data for calculation"
    }
"#;

    let mut parameters = HashMap::new();
    parameters.insert("short_period".to_string(), "3".to_string());
    parameters.insert("long_period".to_string(), "5".to_string());

    let context = StrategyContext {
        market_data: None, // 使用代码中的默认数据
        parameters: Some(parameters),
        extra_data: None,
    };

    match executor.execute(code, Some(context)) {
        Ok(result) => {
            println!("复杂策略执行成功");
            println!("结果: {}", result.result.unwrap_or_default());
        }
        Err(e) => {
            eprintln!("执行失败: {}", e);
        }
    }
}

/// 运行所有示例
pub fn run_all_examples() {
    println!("=== 示例 1: 基本策略执行 ===");
    example_basic_execution();
    println!();

    println!("=== 示例 2: Polars 策略 ===");
    example_polars_strategy();
    println!();

    println!("=== 示例 3: Pandas 策略 ===");
    example_pandas_strategy();
    println!();

    println!("=== 示例 4: 带上下文的策略 ===");
    example_with_context();
    println!();

    println!("=== 示例 5: 复杂量化策略 ===");
    example_complex_strategy();
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_execution() {
        example_basic_execution();
    }

    #[test]
    fn test_with_context() {
        example_with_context();
    }
}
