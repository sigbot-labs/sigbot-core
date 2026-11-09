// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

/// Local Strategy repository adapter. Distributed execution continues to use
/// PostgreSQL for row locking and atomic Inbox/Outbox commits.
pub struct StrategySQLiteRepository {
    pool: SqlitePool,
}

impl StrategySQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
