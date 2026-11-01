// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct MessagerSQLiteRepository {
    pool: SqlitePool,
}

impl MessagerSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
