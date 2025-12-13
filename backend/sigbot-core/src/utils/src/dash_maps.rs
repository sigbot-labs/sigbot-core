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

use dashmap::DashMap;

/// A concurrent-safe HashMap wrapper similar to Java's ConcurrentHashMap.
/// Uses DashMap internally for high-performance concurrent operations with lock-free reads.
/// DashMap uses sharding (分段锁) to allow concurrent access to different segments,
/// providing better performance than RwLock<HashMap> in high-concurrency scenarios.
///
/// # Performance Notes
/// - DashMap uses lock-free reads for better read performance
/// - Write operations use fine-grained locking (per-shard) instead of global locking
/// - Better scalability than RwLock<HashMap> in high-concurrency scenarios
///
/// # Example
/// ```rust
/// use sigbot_utils::maps::ConcurrentHashMap;
///
/// let map = ConcurrentHashMap::<String, String>::new();
///
/// map.compute_if_present("key".to_string(), |old_value| {
///     Some(format!("updated_{}", old_value))
/// }).await;
///
/// map.compute("key".to_string(), |old_value| {
///     match old_value {
///         Some(v) => Some(format!("updated_{}", v)),
///         None => Some("new_value".to_string()),
///     }
/// }).await;
/// ```
pub struct ConcurrentHashMap<K, V> {
    inner: DashMap<K, V>,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    /// Creates a new empty ConcurrentHashMap.
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Creates a new ConcurrentHashMap with the specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
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
        let old_value = self.inner.get(&key).map(|entry| entry.value().clone());
        if let Some(new_value) = creating_fn(old_value).await {
            self.inner.insert(key, new_value.clone());
            Some(new_value)
        } else {
            self.inner.remove(&key);
            None
        }
    }

    /// Gets the value associated with the key.
    /// Note: This is a synchronous operation with DashMap (lock-free read).
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key).map(|entry| entry.value().clone())
    }

    /// Gets the value associated with the key (async version for API compatibility).
    pub async fn get_async(&self, key: &K) -> Option<V> {
        self.get(key)
    }

    /// Inserts a key-value pair into the map.
    /// Returns the previous value if the key existed, None otherwise.
    /// Note: This is a synchronous operation with DashMap.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    /// Inserts a key-value pair into the map (async version for API compatibility).
    pub async fn insert_async(&self, key: K, value: V) -> Option<V> {
        self.insert(key, value)
    }

    /// Removes a key from the map.
    /// Returns the value if the key existed, None otherwise.
    /// Note: This is a synchronous operation with DashMap.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.inner.remove(key).map(|(_, v)| v)
    }

    /// Removes a key from the map (async version for API compatibility).
    pub async fn remove_async(&self, key: &K) -> Option<V> {
        self.remove(key)
    }

    /// Checks if the map contains the key.
    /// Note: This is a synchronous operation with DashMap (lock-free read).
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// Checks if the map contains the key (async version for API compatibility).
    pub async fn contains_key_async(&self, key: &K) -> bool {
        self.contains_key(key)
    }

    /// Returns the number of elements in the map.
    /// Note: This is a synchronous operation with DashMap.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns the number of elements in the map (async version for API compatibility).
    pub async fn len_async(&self) -> usize {
        self.len()
    }

    /// Checks if the map is empty.
    /// Note: This is a synchronous operation with DashMap.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Checks if the map is empty (async version for API compatibility).
    pub async fn is_empty_async(&self) -> bool {
        self.is_empty()
    }

    /// Clears all elements from the map.
    /// Note: This is a synchronous operation with DashMap.
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Clears all elements from the map (async version for API compatibility).
    pub async fn clear_async(&self) {
        self.clear();
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

impl<K, V> Clone for ConcurrentHashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        let new_map = DashMap::new();
        for item in self.inner.iter() {
            new_map.insert(item.key().clone(), item.value().clone());
        }
        Self { inner: new_map }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_hash_map() {
        let map = ConcurrentHashMap::new();
        assert_eq!(map.insert_async("key".to_string(), "value".to_string()).await, None);
        assert_eq!(map.get_async(&"key".to_string()).await.unwrap(), "value".to_string());
        assert_eq!(map.remove_async(&"key".to_string()).await.unwrap(), "value".to_string());
        assert_eq!(map.get_async(&"key".to_string()).await, None);
        assert_eq!(map.contains_key_async(&"key".to_string()).await, false);
        assert_eq!(map.len_async().await, 0);
        assert_eq!(map.is_empty_async().await, true);
        map.clear_async().await;
        assert_eq!(map.len_async().await, 0);
        assert_eq!(map.is_empty_async().await, true);
    }

    #[test]
    fn test_sync_operations() {
        let map = ConcurrentHashMap::new();
        assert_eq!(map.insert("key".to_string(), "value".to_string()), None);
        assert_eq!(map.get(&"key".to_string()).unwrap(), "value".to_string());
        assert_eq!(map.contains_key(&"key".to_string()), true);
        assert_eq!(map.len(), 1);
        assert_eq!(map.is_empty(), false);
    }
}
