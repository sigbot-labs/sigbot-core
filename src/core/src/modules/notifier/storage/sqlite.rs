// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct NotifierSQLiteRepository {
    pool: SqlitePool,
}

impl NotifierSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
