// SPDX-License-Identifier: GPL-3.0-only
//! Host keyring for immutable, restart-safe SDK submission payloads.
use anyhow::{ensure, Context, Result};
use openssl::{
    rand::rand_bytes,
    symm::{decrypt_aead, encrypt_aead, Cipher},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sigbot_types::modules::{messager::envelope::DurableMessage, order::intent::TradeIntent};
use std::collections::BTreeMap;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedKeyConfig {
    pub active_key: String,
    /// Key version -> environment variable containing a 32-byte hex key.
    pub keys: BTreeMap<String, String>,
}

pub struct PreparedPayloadCipher {
    active_key: String,
    keys: BTreeMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedPayload {
    version: u8,
    key_id: String,
    nonce: String,
    ciphertext: String,
    tag: String,
}

impl PreparedPayloadCipher {
    pub fn new(config: &PreparedKeyConfig) -> Result<Self> {
        ensure!(
            !config.keys.is_empty() && config.keys.len() <= 8,
            "invalid submission keyring size"
        );
        ensure!(
            config.keys.contains_key(&config.active_key),
            "active submission key is missing"
        );
        let mut keys = BTreeMap::new();
        for (id, name) in &config.keys {
            ensure!(!id.is_empty() && id.len() <= 64, "invalid submission key version");
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'),
                "submission key references must be environment names"
            );
            let raw = std::env::var(name).context("submission key environment variable missing")?;
            let key = hex::decode(raw.trim()).map_err(|_| anyhow::anyhow!("invalid submission encryption key"))?;
            ensure!(key.len() == 32, "submission key must contain 32 bytes");
            keys.insert(id.clone(), key);
        }
        Ok(Self {
            active_key: config.active_key.clone(),
            keys,
        })
    }

    pub fn seal<T: Serialize>(
        &self,
        provider: &str,
        message: &DurableMessage<TradeIntent>,
        venue_id: &str,
        value: &T,
    ) -> Result<serde_json::Value> {
        let mut nonce = [0u8; 12];
        rand_bytes(&mut nonce)?;
        let mut tag = [0u8; 16];
        let aad = Self::aad(provider, message, venue_id, &self.active_key)?;
        let mut plaintext = serde_json::to_vec(value)?;
        ensure!(plaintext.len() <= 128 * 1024, "prepared SDK payload exceeds limit");
        let result = encrypt_aead(
            Cipher::aes_256_gcm(),
            &self.keys[&self.active_key],
            Some(&nonce),
            &aad,
            &plaintext,
            &mut tag,
        );
        plaintext.fill(0);
        let ciphertext = result.map_err(|_| anyhow::anyhow!("submission payload encryption failed"))?;
        Ok(serde_json::to_value(SealedPayload {
            version: 1,
            key_id: self.active_key.clone(),
            nonce: hex::encode(nonce),
            ciphertext: hex::encode(ciphertext),
            tag: hex::encode(tag),
        })?)
    }

    pub fn open<T: DeserializeOwned>(
        &self,
        provider: &str,
        message: &DurableMessage<TradeIntent>,
        venue_id: &str,
        value: &serde_json::Value,
    ) -> Result<T> {
        let sealed: SealedPayload =
            serde_json::from_value(value.clone()).context("invalid sealed submission format")?;
        ensure!(
            sealed.version == 1 && sealed.ciphertext.len() <= 256 * 1024,
            "unsupported sealed submission format"
        );
        let key = self
            .keys
            .get(&sealed.key_id)
            .context("submission encryption key version unavailable")?;
        let nonce = hex::decode(&sealed.nonce).context("invalid submission nonce")?;
        let tag = hex::decode(&sealed.tag).context("invalid submission tag")?;
        ensure!(
            nonce.len() == 12 && tag.len() == 16,
            "invalid submission authentication material"
        );
        let ciphertext = hex::decode(&sealed.ciphertext).context("invalid submission ciphertext")?;
        let aad = Self::aad(provider, message, venue_id, &sealed.key_id)?;
        let mut plaintext = decrypt_aead(Cipher::aes_256_gcm(), key, Some(&nonce), &aad, &ciphertext, &tag)
            .map_err(|_| anyhow::anyhow!("submission payload authentication failed"))?;
        let result = serde_json::from_slice(&plaintext).map_err(|_| anyhow::anyhow!("invalid prepared SDK payload"));
        plaintext.fill(0);
        result
    }

    fn aad(provider: &str, message: &DurableMessage<TradeIntent>, venue_id: &str, key_id: &str) -> Result<Vec<u8>> {
        // Trace/attempt are deliberately excluded: retries retain the same business identity.
        Ok(serde_json::to_vec(&(
            1,
            provider,
            &message.scope,
            message.account_id,
            message.workflow_id,
            message.node_id,
            &message.run_id,
            message.seq,
            &message.payload,
            venue_id,
            key_id,
        ))?)
    }
}

impl Drop for PreparedPayloadCipher {
    fn drop(&mut self) {
        for key in self.keys.values_mut() {
            key.fill(0);
        }
    }
}
