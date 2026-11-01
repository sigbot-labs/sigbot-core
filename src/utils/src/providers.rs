//! Process-local provider keys. These never become message, trace or database IDs.
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub struct ProviderInstanceId;
impl ProviderInstanceId {
    pub fn configured<T: Serialize>(provider: &str, configuration: &T) -> Result<String> {
        let configuration = Self::canonical(serde_json::to_value(configuration)?);
        Ok(format!(
            "{}:{:x}",
            provider.to_ascii_lowercase(),
            Sha256::digest(serde_json::to_vec(&configuration)?)
        ))
    }

    fn canonical(value: Value) -> Value {
        match value {
            Value::Object(object) => {
                let ordered: std::collections::BTreeMap<_, _> = object
                    .into_iter()
                    .filter(|(key, _)| {
                        !matches!(
                            key.as_str(),
                            "traceparent" | "requestSpanId" | "request_span_id" | "attempt_no"
                        )
                    })
                    .map(|(key, value)| (key, Self::canonical(value)))
                    .collect();
                serde_json::to_value(ordered).expect("JSON object serialization is infallible")
            }
            Value::Array(values) => Value::Array(values.into_iter().map(Self::canonical).collect()),
            value => value,
        }
    }
}
