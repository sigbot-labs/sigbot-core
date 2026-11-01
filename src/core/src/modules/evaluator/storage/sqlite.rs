// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct EvaluatorSQLiteRepository {
    pool: SqlitePool,
}

impl EvaluatorSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
