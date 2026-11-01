// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct ExporterSQLiteRepository {
    pool: SqlitePool,
}

impl ExporterSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
