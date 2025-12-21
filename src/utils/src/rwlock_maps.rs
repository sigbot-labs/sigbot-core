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

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A concurrent-safe HashMap wrapper similar to Java's ConcurrentHashMap.
/// Uses Arc<RwLock<HashMap>> for thread-safe operations in async contexts.
///
/// # Example
/// ```rust
/// use sigbot_utils::maps::ConcurrentHashMap;
///
/// let map = ConcurrentHashMap::<String, String>::new();
///
/// map.compute_if_present("key".to_string(), |old_value| async move {
///     Some(format!("updated_{}", old_value))
/// }).await;
///
/// map.compute("key".to_string(), |old_value| async move {
///     match old_value {
///         Some(v) => Some(format!("updated_{}", v)),
///         None => Some("new_value".to_string()),
///     }
/// }).await;
/// ```
pub struct ConcurrentHashMap<K, V> {
    inner: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    /// Creates a new empty ConcurrentHashMap.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new ConcurrentHashMap with the specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
        }
    }

    /// If the key exists, apply the remapping function and update the value.
    /// Returns Some(new_value) if key existed and was updated, None if key didn't exist or was removed.
    ///
    /// # Example
    /// ```rust
    /// // Similar to: map.computeIfPresent(key, (k, v) -> { ... insert into ... on conflict ... })
    /// map.compute_if_present("key".to_string(), |old_value| async move {
    ///     // Perform database-like INSERT ... ON CONFLICT logic
    ///     Some(format!("updated_{}", old_value))
    /// }).await;
    /// ```
    pub async fn compute_if_present<F, Fut>(&self, key: K, creating_fn: F) -> Option<V>
    where
        F: FnOnce(V) -> Fut,
        Fut: std::future::Future<Output = Option<V>>,
    {
        self.compute(key, |old_value| async move {
            match old_value {
                Some(v) => creating_fn(v).await,
                None => None,
            }
        })
        .await
    }

    /// If the key is not present, compute the value using the provided function.
    /// Returns Some(existing_value) if key existed, or Some(new_value) if key was absent and computed.
    pub async fn compute_if_absent<F, Fut>(&self, key: K, creating_fn: F) -> Option<V>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = V>,
    {
        self.compute(key, |old_value| async move {
            match old_value {
                Some(v) => Some(v),
                None => Some(creating_fn().await),
            }
        })
        .await
    }

    /// Apply the remapping function whether key exists or not.
    /// Returns Some(new_value) if key was inserted/updated, None if key was removed or not inserted.
    ///
    /// # Example
    /// ```rust
    /// map.compute("key".to_string(), |old_value| async move {
    ///     match old_value {
    ///         Some(v) => Some(format!("updated_{}", v)),
    ///         None => Some("new_value".to_string()),
    ///     }
    /// }).await;
    /// ```
    pub async fn compute<F, Fut>(&self, key: K, creating_fn: F) -> Option<V>
    where
        F: FnOnce(Option<V>) -> Fut,
        Fut: std::future::Future<Output = Option<V>>,
    {
        let mut map = self.inner.write().await;
        let old_value = map.get(&key).cloned();
        if let Some(new_value) = creating_fn(old_value).await {
            map.insert(key, new_value.clone());
            Some(new_value)
        } else {
            map.remove(&key);
            None
        }
    }

    /// Gets the value associated with the key.
    pub async fn get(&self, key: &K) -> Option<V> {
        let map = self.inner.read().await;
        map.get(key).cloned()
    }

    /// Inserts a key-value pair into the map.
    /// Returns the previous value if the key existed, None otherwise.
    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        let mut map = self.inner.write().await;
        map.insert(key, value)
    }

    /// Removes a key from the map.
    /// Returns the value if the key existed, None otherwise.
    pub async fn remove(&self, key: &K) -> Option<V> {
        let mut map = self.inner.write().await;
        map.remove(key)
    }

    /// Checks if the map contains the key.
    pub async fn contains_key(&self, key: &K) -> bool {
        let map = self.inner.read().await;
        map.contains_key(key)
    }

    /// Returns the number of elements in the map.
    pub async fn len(&self) -> usize {
        let map = self.inner.read().await;
        map.len()
    }

    /// Checks if the map is empty.
    pub async fn is_empty(&self) -> bool {
        let map = self.inner.read().await;
        map.is_empty()
    }

    /// Clears all elements from the map.
    pub async fn clear(&self) {
        let mut map = self.inner.write().await;
        map.clear();
    }
}

impl<K, V> Default for ConcurrentHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for ConcurrentHashMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_hash_map() {
        let map = ConcurrentHashMap::new();
        assert_eq!(map.insert("key".to_string(), "value".to_string()).await, None);
        assert_eq!(map.get(&"key".to_string()).await.unwrap(), "value".to_string());
        assert_eq!(map.remove(&"key".to_string()).await.unwrap(), "value".to_string());
        assert_eq!(map.get(&"key".to_string()).await, None);
        assert_eq!(map.contains_key(&"key".to_string()).await, false);
        assert_eq!(map.len().await, 0);
        assert_eq!(map.is_empty().await, true);
        map.clear().await;
        assert_eq!(map.len().await, 0);
        assert_eq!(map.is_empty().await, true);
    }
}
