// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

use super::ICache;
use crate::config::config::MemoryProperties;
use anyhow::Error;
use anyhow::Ok;
use async_trait::async_trait;
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use regex::Regex;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct StringMemoryCache {
    cache: Arc<Cache<String, Vec<u8>>>,
    expirations: Arc<Mutex<HashMap<String, u64>>>,
    expiration_sequence: AtomicU64,
}

impl StringMemoryCache {
    pub fn new(config: &MemoryProperties) -> Self {
        let mut builder = Cache::builder();
        if let Some(initial_capacity) = config.initial_capacity {
            builder = builder.initial_capacity(initial_capacity as usize);
        }
        if let Some(max_capacity) = config.max_capacity {
            builder = builder.max_capacity(max_capacity);
        }
        if let Some(ttl) = config.ttl {
            builder = builder.time_to_live(Duration::from_millis(ttl));
        }
        if let Some(eviction_policy) = &config.eviction_policy {
            match eviction_policy.to_uppercase().as_str() {
                "LRU" => {
                    builder = builder.eviction_policy(EvictionPolicy::lru());
                }
                "LFU" | "TINY_LFU" => {
                    builder = builder.eviction_policy(EvictionPolicy::tiny_lfu());
                }
                _ => {
                    builder = builder.eviction_policy(EvictionPolicy::default());
                }
            }
        }
        StringMemoryCache {
            cache: Arc::new(builder.build()),
            expirations: Arc::new(Mutex::new(HashMap::new())),
            expiration_sequence: AtomicU64::new(0),
        }
    }

    async fn store(&self, key: String, value: String, ttl: Option<Duration>) {
        let token = self.expiration_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let mut expirations = self.expirations.lock().await;
        self.cache.insert(key.clone(), value.into_bytes()).await;
        if let Some(ttl) = ttl {
            expirations.insert(key.clone(), token);
            let cache = Arc::clone(&self.cache);
            let expiration_map = Arc::clone(&self.expirations);
            tokio::spawn(async move {
                tokio::time::sleep(ttl).await;
                let mut expirations = expiration_map.lock().await;
                if expirations.get(&key) == Some(&token) {
                    cache.invalidate(&key).await;
                    expirations.remove(&key);
                }
            });
        } else {
            expirations.remove(&key);
        }
    }

    fn serialize_hash(hash: &HashMap<String, String>) -> String {
        serde_json::to_string(hash).unwrap_or_default()
    }

    fn deserialize_hash(s: &str) -> HashMap<String, String> {
        serde_json::from_str(s).unwrap_or_default()
    }
}

#[async_trait]
impl ICache<String> for StringMemoryCache {
    async fn get(&self, key: String) -> Result<Option<String>, Error> {
        self.cache
            .get(&key)
            .await
            .map(String::from_utf8)
            .transpose()
            .map_err(Error::msg)
    }

    /// Sets the given key to the specified value.
    ///
    async fn set(&self, key: String, value: String, seconds: Option<i32>) -> Result<bool, Error> {
        let ttl = match seconds {
            Some(seconds) if seconds > 0 => Some(Duration::from_secs(seconds as u64)),
            Some(_) => return Err(Error::msg("cache expiration seconds must be positive")),
            None => None,
        };
        self.store(key.clone(), value, ttl).await;
        tracing::info!("Inserted to key: {}, expire: {:?}s", key, seconds);
        Ok(true)
    }

    async fn set_nx(&self, key: String, value: Option<String>) -> Result<bool, Error> {
        if let Some(v) = value {
            match self.cache.contains_key(&key) {
                false => {
                    self.cache.insert(key.clone(), v.into_bytes()).await;
                    return Ok(true);
                }
                true => {
                    return Ok(false);
                }
            }
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, pattern_str: String) -> Result<Vec<String>, Error> {
        let pattern = Arc::new(Regex::new(&pattern_str).unwrap());
        let keys = self
            .cache
            .into_iter()
            .map(|(k, _)| k.deref().clone())
            .filter(|k| pattern.clone().is_match(k))
            .collect::<Vec<_>>();
        Ok(keys)
    }

    async fn hget(&self, key: String, field: Option<String>) -> Result<Option<String>, Error> {
        if let Some(hash_str) = self.get(key).await? {
            let hash = Self::deserialize_hash(&hash_str);
            match field {
                Some(f) => Ok(hash.get(&f).cloned()),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn hget_all(&self, key: String) -> Result<Option<HashMap<String, String>>, Error> {
        if let Some(hash_str) = self.get(key).await? {
            let hash = Self::deserialize_hash(&hash_str);
            Ok(Some(hash))
        } else {
            Ok(None)
        }
    }

    async fn hset(&self, key: String, field_values: Option<Vec<(String, String)>>) -> Result<bool, Error> {
        if let Some(fv) = field_values {
            let mut hash = if let Some(hash_str) = self.get(key.clone()).await? {
                Self::deserialize_hash(&hash_str)
            } else {
                HashMap::new()
            };
            for (field, value) in fv {
                hash.insert(field, value); // override put
            }
            self.cache.insert(key, Self::serialize_hash(&hash).into_bytes()).await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn hset_nx(&self, key: String, field: String, value: String) -> Result<bool, Error> {
        let mut hash = if let Some(hash_str) = self.get(key.clone()).await? {
            Self::deserialize_hash(&hash_str)
        } else {
            HashMap::new()
        };
        if let std::collections::hash_map::Entry::Vacant(e) = hash.entry(field) {
            e.insert(value);
            self.cache.insert(key, Self::serialize_hash(&hash).into_bytes()).await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn hkeys(&self, key: String) -> Result<Vec<String>, Error> {
        if let Some(hash_str) = self.get(key).await? {
            let hash = Self::deserialize_hash(&hash_str);
            let fields: Vec<String> = hash.into_keys().collect();
            Ok(fields)
        } else {
            Ok(vec![])
        }
    }

    #[allow(unused)]
    async fn hdel(&self, key: String, field: String) -> Result<bool, Error> {
        let result = self.hget_all(key.clone()).await?;
        match result {
            Some(mut hash) => {
                let removed = hash.remove(&field).is_some();
                if removed {
                    self.cache.insert(key, Self::serialize_hash(&hash).into_bytes()).await;
                }
                Ok(removed)
            }
            None => Ok(false),
        }
    }

    async fn expire(&self, key: String, milliseconds: i64) -> Result<bool, Error> {
        let mut expirations = self.expirations.lock().await;
        if !self.cache.contains_key(&key) {
            expirations.remove(&key);
            return Ok(false);
        }
        if milliseconds <= 0 {
            self.cache.invalidate(&key).await;
            expirations.remove(&key);
            return Ok(true);
        }
        let token = self.expiration_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        expirations.insert(key.clone(), token);
        drop(expirations);

        let cache = Arc::clone(&self.cache);
        let expiration_map = Arc::clone(&self.expirations);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(milliseconds as u64)).await;
            let mut expirations = expiration_map.lock().await;
            if expirations.get(&key) == Some(&token) {
                cache.invalidate(&key).await;
                expirations.remove(&key);
            }
        });
        Ok(true)
    }

    async fn get_bit(&self, key: String, offset: u64) -> Result<bool, Error> {
        if let Some(value) = self.cache.get(&key).await {
            let byte_offset = (offset / 8) as usize;
            let bit_offset = (offset % 8) as u8;
            if byte_offset < value.len() {
                let byte = value[byte_offset];
                Ok(((byte >> (7 - bit_offset)) & 1) == 1)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn set_bit(&self, key: String, offset: u64, value: bool) -> Result<bool, Error> {
        let mut bytes = self.cache.get(&key).await.unwrap_or_default();

        let byte_offset = (offset / 8) as usize;
        let bit_offset = (offset % 8) as u8;

        if byte_offset >= bytes.len() {
            bytes.resize(byte_offset + 1, 0);
        }

        let old_byte = bytes[byte_offset];
        let new_byte = if value {
            old_byte | (1 << (7 - bit_offset))
        } else {
            old_byte & !(1 << (7 - bit_offset))
        };

        bytes[byte_offset] = new_byte;
        self.cache.insert(key, bytes).await;

        Ok(((old_byte >> (7 - bit_offset)) & 1) == 1)
    }

    async fn del(&self, key: String) -> Result<bool, Error> {
        let mut expirations = self.expirations.lock().await;
        self.cache.invalidate(&key).await;
        expirations.remove(&key);
        Ok(true)
    }
}
