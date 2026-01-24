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

/// Evaluator Provider Enum
///
/// Represents different types of evaluator implementations for ANALYSIS stage nodes.
/// Evaluator nodes are responsible for AI-driven market analysis and hyperparameter calculation.
///
/// Architecture:
/// - **Evaluator Runner** (evaluator microservice): Handles ANALYSIS stage with MAS provider
/// - **Strategy Runner** (strategy microservice): Handles ANALYSIS stage with PYCODE provider
///
/// Both work together to execute the ANALYSIS stage of workflows, but with different approaches:
/// - MAS (Multi-Agent System): LLM-based multi-agent orchestration for dynamic analysis
/// - PYCODE: Python-based static strategy code execution
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum EvaluatorProvider {
    /// Multi-Agent System (MAS) provider
    /// Uses LLM-based multi-agent orchestration (ADK framework)
    /// for dynamic market analysis and hyperparameter calculation
    ///
    /// Agents such as:
    /// - BootAgent: Collects market statistics
    /// - LoaderAgent: Loads warm data (kline, news, etc.)
    /// - AlphaAgent: Analyzes data and calculates hyperparameters
    /// - AuditorAgent: Validates hyperparameters for compliance
    MAS,
}

impl EvaluatorProvider {
    pub fn of(provider: &str) -> Result<EvaluatorProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "MAS" => Ok(EvaluatorProvider::MAS),
            _ => Err(anyhow::anyhow!("Unsupported evaluator provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            EvaluatorProvider::MAS => "MAS",
        }
    }
}
