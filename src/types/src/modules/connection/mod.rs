use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveConnection {
    pub env: String,
    pub space_id: i64,
    pub kind: String,
    pub name: String,
    pub provider: String,
    pub mode: String,
    pub endpoint: String,
    pub secret_ref: String,
}
