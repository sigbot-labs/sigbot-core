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

//! Redis Cluster Test Utilities for E2E Tests
//!
//! Provides helper functions for Redis Cluster operations.

use anyhow::{Result, Context};
use super::MiddlewareE2EContext;

/// Redis test utilities
pub struct RedisTestUtils {
    pub redis_url: String,
    pub client: redis::Client,
}

impl RedisTestUtils {
    /// Create new Redis test utilities
    pub fn new(redis_url: String) -> Result<Self> {
        let client = redis::Client::open(redis_url.as_str())
            .context("Failed to create Redis client")?;
        Ok(Self { redis_url, client })
    }

    /// Create from MiddlewareE2EContext
    pub fn from_context(ctx: &MiddlewareE2EContext) -> Result<Self> {
        Self::new(ctx.redis_url.clone())
    }

    /// Get a connection from the client
    pub fn get_connection(&self) -> Result<redis::Connection> {
        self.client.get_connection()
            .context("Failed to get Redis connection")
    }

    /// Set a key-value pair
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut conn = self.get_connection()?;
        let _: () = redis::cmd("SET").arg(key).arg(value).query(&mut conn)?;
        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.get_connection()?;
        let result: Option<String> = redis::cmd("GET").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Delete a key
    pub fn del(&self, key: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("DEL").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Set a hash field
    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool> {
        let mut conn = self.get_connection()?;
        let result: bool = redis::cmd("HSET").arg(key).arg(field).arg(value).query(&mut conn)?;
        Ok(result)
    }

    /// Get a hash field
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>> {
        let mut conn = self.get_connection()?;
        let result: Option<String> = redis::cmd("HGET").arg(key).arg(field).query(&mut conn)?;
        Ok(result)
    }

    /// Get all hash fields
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>> {
        let mut conn = self.get_connection()?;
        let result: Vec<(String, String)> = redis::cmd("HGETALL").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Delete a hash field
    pub fn hdel(&self, key: &str, field: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("HDEL").arg(key).arg(field).query(&mut conn)?;
        Ok(result)
    }

    /// Set a key with expiration
    pub fn set_ex(&self, key: &str, value: &str, seconds: usize) -> Result<()> {
        let mut conn = self.get_connection()?;
        let _: () = redis::cmd("SETEX").arg(key).arg(seconds).arg(value).query(&mut conn)?;
        Ok(())
    }

    /// Get time-to-live for a key
    pub fn ttl(&self, key: &str) -> Result<i64> {
        let mut conn = self.get_connection()?;
        let result: i64 = redis::cmd("TTL").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Check if a key exists
    pub fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.get_connection()?;
        let result: bool = redis::cmd("EXISTS").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Increment a key
    pub fn incr(&self, key: &str) -> Result<i64> {
        let mut conn = self.get_connection()?;
        let result: i64 = redis::cmd("INCR").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Decrement a key
    pub fn decr(&self, key: &str) -> Result<i64> {
        let mut conn = self.get_connection()?;
        let result: i64 = redis::cmd("DECR").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Push to list (left)
    pub fn lpush(&self, key: &str, value: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("LPUSH").arg(key).arg(value).query(&mut conn)?;
        Ok(result)
    }

    /// Pop from list (right)
    pub fn rpop(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.get_connection()?;
        let result: Option<String> = redis::cmd("RPOP").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Get list length
    pub fn llen(&self, key: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("LLEN").arg(key).query(&mut conn)?;
        Ok(result)
    }

    /// Add to set
    pub fn sadd(&self, key: &str, value: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("SADD").arg(key).arg(value).query(&mut conn)?;
        Ok(result)
    }

    /// Check if member is in set
    pub fn sismember(&self, key: &str, value: &str) -> Result<bool> {
        let mut conn = self.get_connection()?;
        let result: bool = redis::cmd("SISMEMBER").arg(key).arg(value).query(&mut conn)?;
        Ok(result)
    }

    /// Publish to channel
    pub fn publish(&self, channel: &str, message: &str) -> Result<usize> {
        let mut conn = self.get_connection()?;
        let result: usize = redis::cmd("PUBLISH").arg(channel).arg(message).query(&mut conn)?;
        Ok(result)
    }

    /// Flush all data (test use only)
    pub fn flushall(&self) -> Result<()> {
        let mut conn = self.get_connection()?;
        let _: () = redis::cmd("FLUSHALL").query(&mut conn)?;
        Ok(())
    }

    /// Ping Redis
    pub fn ping(&self) -> Result<String> {
        let mut conn = self.get_connection()?;
        let result: String = redis::cmd("PING").query(&mut conn)?;
        Ok(result)
    }
}

/// Create Redis test utilities from MiddlewareE2EContext
pub fn create_redis_utils(ctx: &MiddlewareE2EContext) -> Result<RedisTestUtils> {
    RedisTestUtils::from_context(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::MiddlewareE2EContext;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_redis_basic_operations() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = RedisTestUtils::from_context(&ctx).unwrap();

        // Test set/get
        utils.set("test_key", "test_value").unwrap();
        let value = utils.get("test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        // Test hash operations
        utils.hset("test_hash", "field1", "value1").unwrap();
        let field_value = utils.hget("test_hash", "field1").unwrap();
        assert_eq!(field_value, Some("value1".to_string()));

        // Test expiration
        utils.set_ex("test_expire", "value", 60).unwrap();
        let ttl = utils.ttl("test_expire").unwrap();
        assert!(ttl > 0 && ttl <= 60);

        ctx.teardown();
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_redis_cluster_mode() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = RedisTestUtils::from_context(&ctx).unwrap();

        // Test cluster info
        let pong = utils.ping().unwrap();
        assert_eq!(pong, "PONG");

        ctx.teardown();
    }
}
