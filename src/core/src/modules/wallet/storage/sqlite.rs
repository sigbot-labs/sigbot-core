// SPDX-License-Identifier: GPL-3.0-only

use sqlx::SqlitePool;

pub struct WalletSQLiteRepository {
    pool: SqlitePool,
}

impl WalletSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
