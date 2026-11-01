// SPDX-License-Identifier: GPL-3.0-only

use sigbot_core::cache::memory::StringMemoryCache;
use sigbot_core::cache::ICache;
use sigbot_core::config::config::MemoryProperties;
use std::time::Duration;

struct MemoryCacheTest;

impl MemoryCacheTest {
    fn cache() -> StringMemoryCache {
        StringMemoryCache::new(&MemoryProperties {
            initial_capacity: Some(8),
            max_capacity: Some(32),
            ttl: None,
            eviction_policy: Some("LRU".to_string()),
        })
    }
}

#[tokio::test]
async fn string_and_conditional_operations_preserve_values() {
    let cache = MemoryCacheTest::cache();
    assert!(cache.set("key1".to_string(), "value1".to_string(), None).await.unwrap());
    assert_eq!(cache.get("key1".to_string()).await.unwrap().as_deref(), Some("value1"));
    assert!(cache
        .set_nx("key2".to_string(), Some("value2".to_string()))
        .await
        .unwrap());
    assert!(!cache
        .set_nx("key2".to_string(), Some("replacement".to_string()))
        .await
        .unwrap());
    assert_eq!(cache.get("key2".to_string()).await.unwrap().as_deref(), Some("value2"));
}

#[tokio::test]
async fn keys_and_hash_operations_match_the_cache_contract() {
    let cache = MemoryCacheTest::cache();
    for key in ["key1", "key2", "key3"] {
        cache.set(key.to_string(), "value".to_string(), None).await.unwrap();
    }
    let mut keys = cache.keys("^key[1-3]$".to_string()).await.unwrap();
    keys.sort();
    assert_eq!(keys, ["key1", "key2", "key3"]);

    let hash = "hash".to_string();
    assert!(cache
        .hset_nx(hash.clone(), "field1".to_string(), "value1".to_string())
        .await
        .unwrap());
    assert!(cache
        .hset_nx(hash.clone(), "field2".to_string(), "value2".to_string())
        .await
        .unwrap());
    assert_eq!(
        cache
            .hget(hash.clone(), Some("field1".to_string()))
            .await
            .unwrap()
            .as_deref(),
        Some("value1")
    );
    let mut fields = cache.hkeys(hash.clone()).await.unwrap();
    fields.sort();
    assert_eq!(fields, ["field1", "field2"]);
    assert!(cache.hdel(hash.clone(), "field1".to_string()).await.unwrap());
    assert!(!cache.hdel(hash, "missing".to_string()).await.unwrap());
}

#[tokio::test]
async fn bit_operations_use_raw_bytes_without_utf8_loss() {
    let cache = MemoryCacheTest::cache();
    assert!(!cache.set_bit("bits".to_string(), 0, true).await.unwrap());
    assert!(cache.get_bit("bits".to_string(), 0).await.unwrap());
    assert!(!cache.get_bit("bits".to_string(), 1).await.unwrap());
    assert!(cache.set_bit("bits".to_string(), 0, false).await.unwrap());
    assert!(!cache.get_bit("bits".to_string(), 0).await.unwrap());
}

#[tokio::test]
async fn delete_removes_the_value() {
    let cache = MemoryCacheTest::cache();
    cache.set("key".to_string(), "value".to_string(), None).await.unwrap();
    assert!(cache.del("key".to_string()).await.unwrap());
    assert_eq!(cache.get("key".to_string()).await.unwrap(), None);
}

#[tokio::test]
async fn explicit_expiration_is_safe_for_missing_and_overwritten_keys() {
    let cache = MemoryCacheTest::cache();
    assert!(!cache.expire("missing".to_string(), 1).await.unwrap());

    cache
        .set("expiring".to_string(), "old".to_string(), None)
        .await
        .unwrap();
    assert!(cache.expire("expiring".to_string(), 20).await.unwrap());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(cache.get("expiring".to_string()).await.unwrap(), None);

    cache
        .set("replaced".to_string(), "old".to_string(), None)
        .await
        .unwrap();
    assert!(cache.expire("replaced".to_string(), 20).await.unwrap());
    cache
        .set("replaced".to_string(), "new".to_string(), None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(cache.get("replaced".to_string()).await.unwrap().as_deref(), Some("new"));
}

#[tokio::test]
async fn set_ttl_matches_redis_seconds_contract() {
    let cache = MemoryCacheTest::cache();
    assert!(cache
        .set("ttl".to_string(), "value".to_string(), Some(1))
        .await
        .unwrap());
    assert!(cache
        .set("invalid".to_string(), "value".to_string(), Some(0))
        .await
        .is_err());
    tokio::time::sleep(Duration::from_millis(1_050)).await;
    assert_eq!(cache.get("ttl".to_string()).await.unwrap(), None);
}
