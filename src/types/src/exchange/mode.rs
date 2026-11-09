//! Execution behavior is independent of the deployment environment in MessageScope.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Live,
    Paper,
    /// Historical finite input and a virtual clock use Backtest's matching API.
    Backtest,
}

impl ExecutionMode {
    pub fn of(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "paper" => Ok(Self::Paper),
            "backtest" => Ok(Self::Backtest),
            _ => Err("unsupported execution mode".into()),
        }
    }

    pub fn as_string(&self) -> String {
        self.as_str().to_owned()
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Paper => "paper",
            Self::Backtest => "backtest",
        }
    }
}
