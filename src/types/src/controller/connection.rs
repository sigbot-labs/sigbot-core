use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SaveConnection {
    pub env: String,
    pub kind: String,
    pub name: String,
    pub provider: String,
    pub mode: String,
    pub endpoint: String,
    pub secret_ref: String,
}
