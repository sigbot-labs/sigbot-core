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

//! E2E Test Common Modules
//!
//! This module provides common utilities and test helpers for E2E integration tests.
//! The flows and scenarios modules are separate to keep test organization clear.

pub mod assertions;
pub mod container_runtime;
pub mod fixtures;
pub mod kafka_utils;
pub mod middleware_containers;
pub mod minio_utils;
pub mod mock_exchange;
pub mod redis_utils;
pub mod real_database;
pub mod service_runner;
pub mod timescaledb_utils;

pub use middleware_containers::{
    MiddlewareContainerManager,
    MiddlewareE2EContext,
    PostgresContainerConfig,
    TimescaleDBContainerConfig,
    RedisClusterContainerConfig,
    KafkaContainerConfig,
    MinioContainerConfig,
    EmqxContainerConfig,
};
pub use mock_exchange::{MockExchangeClient, MockExchangeConfig};
pub use real_database::*;
pub use kafka_utils::{KafkaTestUtils, create_kafka_utils};
pub use redis_utils::{RedisTestUtils, create_redis_utils};
pub use minio_utils::{MinioTestUtils, create_minio_utils};
pub use timescaledb_utils::{TimescaleDBTestUtils, KlineRecord, create_timescaledb_utils};
pub use service_runner::{
    ServiceHandle,
    DatafeedServiceRunner,
    WalletServiceRunner,
    OrderServiceRunner,
    StrategyServiceRunner,
    EvaluatorServiceRunner,
    LogServiceRunner,
    ExportServiceRunner,
    NotificationServiceRunner,
};
