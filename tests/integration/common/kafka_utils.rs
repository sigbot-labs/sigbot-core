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

//! Kafka Test Utilities for E2E Tests
//!
//! Provides helper functions for producing and consuming messages
//! from Kafka containers for testing the export results flow.

use anyhow::{Result, Context};
use serde_json::Value;
use super::MiddlewareE2EContext;

/// Kafka test utilities
pub struct KafkaTestUtils {
    pub bootstrap_servers: String,
    pub kafka_container_name: String,
}

impl KafkaTestUtils {
    /// Create new Kafka test utilities
    pub fn new(bootstrap_servers: String, kafka_container_name: String) -> Self {
        Self { bootstrap_servers, kafka_container_name }
    }

    /// Create from MiddlewareE2EContext
    pub fn from_context(ctx: &MiddlewareE2EContext) -> Self {
        Self {
            bootstrap_servers: ctx.kafka_bootstrap_servers.clone(),
            kafka_container_name: ctx.manager.get_kafka_container_name(),
        }
    }

    /// Create a topic using kafka-topics.sh
    pub async fn create_topic(&self, topic: &str, partitions: i32) -> Result<()> {
        use tokio::process::Command;

        let output = Command::new("docker")
            .args([
                "exec",
                &self.kafka_container_name,
                "/opt/bitnami/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--create",
                "--if-not-exists",
                "--topic", topic,
                "--partitions", &partitions.to_string(),
                "--replication-factor", "1",
            ])
            .output()
            .await
            .context("Failed to execute kafka-topics.sh")?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to create topic: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    /// Produce a message to Kafka topic
    pub async fn produce_message(&self, topic: &str, key: &str, value: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        use tokio::process::Command;

        let mut child = Command::new("docker")
            .args([
                "exec",
                &self.kafka_container_name,
                "/opt/bitnami/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", topic,
                "--property", "parse.key=true",
                "--property", "key.separator=:",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn kafka-console-producer.sh")?;

        // Write message to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(format!("{}:{}\n", key, value).as_bytes()).await?;
            drop(stdin);
        }

        // Wait for completion
        let result = child.wait_with_output().await?;
        if !result.status.success() {
            anyhow::bail!(
                "Failed to produce message: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }

        Ok(())
    }

    /// Produce JSON message to Kafka topic
    pub async fn produce_json(&self, topic: &str, key: &str, value: &Value) -> Result<()> {
        let json_str = serde_json::to_string(value)?;
        self.produce_message(topic, key, &json_str).await
    }

    /// Consume messages from Kafka topic (with timeout)
    pub async fn consume_messages(&self, topic: &str, max_messages: usize, timeout_secs: u64) -> Result<Vec<String>> {
        use tokio::process::Command;
        use tokio::time::{timeout, Duration};

        // Start consumer in background
        let child = Command::new("docker")
            .args([
                "exec",
                &self.kafka_container_name,
                "/opt/bitnami/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", topic,
                "--from-beginning",
                "--max-messages", &max_messages.to_string(),
                "--timeout-ms", "5000",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn kafka-console-consumer.sh")?;

        // Wait for consumer to complete with timeout
        let result = timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
            .await
            .context("Timeout waiting for consumer")?
            .context("Failed to wait for consumer")?;

        let stdout = String::from_utf8_lossy(&result.stdout);
        let messages: Vec<String> = stdout.lines().map(|s| s.to_string()).collect();

        Ok(messages)
    }

    /// List topics
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        use tokio::process::Command;

        let output = Command::new("docker")
            .args([
                "exec",
                &self.kafka_container_name,
                "/opt/bitnami/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--list",
            ])
            .output()
            .await
            .context("Failed to list topics")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let topics: Vec<String> = stdout
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(topics)
    }

    /// Describe topic
    pub async fn describe_topic(&self, topic: &str) -> Result<String> {
        use tokio::process::Command;

        let output = Command::new("docker")
            .args([
                "exec",
                &self.kafka_container_name,
                "/opt/bitnami/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--describe",
                "--topic", topic,
            ])
            .output()
            .await
            .context("Failed to describe topic")?;

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

/// Create Kafka test utilities from MiddlewareE2EContext
pub fn create_kafka_utils(ctx: &MiddlewareE2EContext) -> KafkaTestUtils {
    KafkaTestUtils::from_context(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::MiddlewareE2EContext;

    #[tokio::test]
    #[ignore = "Requires Docker environment"]
    async fn test_kafka_produce_consume() {
        let ctx = MiddlewareE2EContext::setup().await;
        let utils = KafkaTestUtils::from_context(&ctx);

        // Create test topic
        utils.create_topic("test-topic", 1).await.unwrap();

        // Produce message
        utils
            .produce_message("test-topic", "key1", "value1")
            .await
            .unwrap();

        // Consume message
        let messages = utils
            .consume_messages("test-topic", 1, 10)
            .await
            .unwrap();

        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("value1"));

        ctx.teardown();
    }
}
