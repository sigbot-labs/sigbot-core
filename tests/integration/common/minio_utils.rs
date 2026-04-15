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

//! MinIO Test Utilities for E2E Tests
//!
//! Provides helper functions for MinIO object storage operations.

use anyhow::{Result, Context};
use super::MiddlewareE2EContext;

/// MinIO test utilities
pub struct MinioTestUtils {
    pub endpoint: String,
    pub root_user: String,
    pub root_password: String,
    pub client: reqwest::Client,
}

impl MinioTestUtils {
    /// Create new MinIO test utilities
    pub fn new(endpoint: String, root_user: String, root_password: String) -> Self {
        Self {
            endpoint,
            root_user,
            root_password,
            client: reqwest::Client::new(),
        }
    }

    /// Create from MiddlewareE2EContext
    pub fn from_context(ctx: &MiddlewareE2EContext, root_user: String, root_password: String) -> Self {
        Self::new(ctx.minio_endpoint.clone(), root_user, root_password)
    }

    /// Create a bucket
    pub async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        let url = format!("{}{}", self.endpoint, bucket_name);
        let response = self.client
            .put(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await
            .context("Failed to create bucket")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to create bucket: {}", response.status());
        }

        Ok(())
    }

    /// Check if bucket exists
    pub async fn bucket_exists(&self, bucket_name: &str) -> Result<bool> {
        let url = format!("{}{}", self.endpoint, bucket_name);
        let response = self.client
            .head(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await;

        match response {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    /// Upload an object
    pub async fn put_object(&self, bucket_name: &str, object_key: &str, content: &[u8]) -> Result<()> {
        let url = format!("{}{}/{}", self.endpoint, bucket_name, object_key);
        let response = self.client
            .put(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .body(content.to_vec())
            .send()
            .await
            .context("Failed to put object")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to put object: {}", response.status());
        }

        Ok(())
    }

    /// Upload a JSON object
    pub async fn put_json<T: serde::Serialize>(&self, bucket_name: &str, object_key: &str, data: &T) -> Result<()> {
        let json_content = serde_json::to_vec_pretty(data)?;
        self.put_object(bucket_name, object_key, &json_content).await
    }

    /// Get an object
    pub async fn get_object(&self, bucket_name: &str, object_key: &str) -> Result<Vec<u8>> {
        let url = format!("{}{}/{}", self.endpoint, bucket_name, object_key);
        let response = self.client
            .get(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await
            .context("Failed to get object")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to get object: {}", response.status());
        }

        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// Get a JSON object
    pub async fn get_json<T: for<'de> serde::Deserialize<'de>>(&self, bucket_name: &str, object_key: &str) -> Result<T> {
        let bytes = self.get_object(bucket_name, object_key).await?;
        let data: T = serde_json::from_slice(&bytes)?;
        Ok(data)
    }

    /// Delete an object
    pub async fn delete_object(&self, bucket_name: &str, object_key: &str) -> Result<()> {
        let url = format!("{}{}/{}", self.endpoint, bucket_name, object_key);
        let response = self.client
            .delete(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await
            .context("Failed to delete object")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to delete object: {}", response.status());
        }

        Ok(())
    }

    /// List objects in a bucket
    pub async fn list_objects(&self, bucket_name: &str) -> Result<Vec<String>> {
        let url = format!("{}{}?list-type=2", self.endpoint, bucket_name);
        let response = self.client
            .get(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await
            .context("Failed to list objects")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to list objects: {}", response.status());
        }

        let body = response.text().await?;

        // Parse XML response to extract object keys
        let mut objects = Vec::new();
        for line in body.lines() {
            if let Some(start) = line.find("<Key>") {
                if let Some(end) = line.find("</Key>") {
                    let key = &line[start + 5..end];
                    objects.push(key.to_string());
                }
            }
        }

        Ok(objects)
    }

    /// Delete a bucket
    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        let url = format!("{}{}", self.endpoint, bucket_name);
        let response = self.client
            .delete(&url)
            .basic_auth(&self.root_user, Some(&self.root_password))
            .send()
            .await
            .context("Failed to delete bucket")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to delete bucket: {}", response.status());
        }

        Ok(())
    }

    /// Check MinIO health
    pub async fn health_check(&self) -> Result<bool> {
        let health_url = format!("{}minio/health/live", self.endpoint);
        let response = self.client
            .get(&health_url)
            .send()
            .await;

        match response {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}

/// Create MinIO test utilities from MiddlewareE2EContext
pub fn create_minio_utils(ctx: &MiddlewareE2EContext, root_user: String, root_password: String) -> MinioTestUtils {
    MinioTestUtils::from_context(ctx, root_user, root_password)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::MiddlewareE2EContext;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_minio_basic_operations() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = MinioTestUtils::from_context(&ctx, "minioadmin".to_string(), "minioadmin".to_string());

        // Health check
        assert!(utils.health_check().await.unwrap());

        // Create bucket
        let bucket_name = "test-bucket";
        utils.create_bucket(bucket_name).await.unwrap();
        assert!(utils.bucket_exists(bucket_name).await.unwrap());

        // Put object
        let object_key = "test-object.txt";
        let content = b"Hello, MinIO!";
        utils.put_object(bucket_name, object_key, content).await.unwrap();

        // Get object
        let retrieved = utils.get_object(bucket_name, object_key).await.unwrap();
        assert_eq!(retrieved, content);

        // Delete object
        utils.delete_object(bucket_name, object_key).await.unwrap();

        // Delete bucket
        utils.delete_bucket(bucket_name).await.unwrap();

        ctx.teardown();
    }

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_minio_json_operations() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = MinioTestUtils::from_context(&ctx, "minioadmin".to_string(), "minioadmin".to_string());

        // Create bucket
        let bucket_name = "test-json-bucket";
        utils.create_bucket(bucket_name).await.unwrap();

        // Put JSON object
        let object_key = "test-data.json";
        let data = serde_json::json!({
            "name": "test",
            "value": 123
        });
        utils.put_json(bucket_name, object_key, &data).await.unwrap();

        // Get JSON object
        let retrieved: serde_json::Value = utils.get_json(bucket_name, object_key).await.unwrap();
        assert_eq!(retrieved["name"], "test");
        assert_eq!(retrieved["value"], 123);

        ctx.teardown();
    }
}
