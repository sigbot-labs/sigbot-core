// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct OrderSQLiteRepository {
    pool: SqlitePool,
}

impl OrderSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
