use serde::Serialize;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;

#[derive(Serialize)]
struct ScopeFilter {
    tenant_id: i64,
    id: Option<i64>,
}

#[derive(sqlx::FromRow, Debug)]
struct ScopedRow {
    id: i64,
    tenant_id: i64,
}

#[tokio::test]
async fn sqlite_query_binds_numeric_tenant_and_counts_only_visible_rows() -> anyhow::Result<()> {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
    sqlx::query(
        "CREATE TABLE scoped_items(id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, del_flag INTEGER NOT NULL)",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO scoped_items VALUES (1, 10, 0), (2, 20, 0), (3, 10, 1)")
        .execute(&pool)
        .await?;
    let filter = ScopeFilter {
        tenant_id: 10,
        id: None,
    };
    let result: anyhow::Result<(PageResponse, Vec<ScopedRow>)> =
        sigbot_core::dynamic_sqlite_query!(filter, "scoped_items", &pool, "id", PageRequest::new(1, 10), ScopedRow);
    let (_, rows) = result?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].tenant_id, 10);
    let filter = ScopeFilter {
        tenant_id: 10,
        id: Some(2),
    };
    let result: anyhow::Result<(PageResponse, Vec<ScopedRow>)> =
        sigbot_core::dynamic_sqlite_query!(filter, "scoped_items", &pool, "id", PageRequest::new(1, 10), ScopedRow);
    assert!(result?.1.is_empty());
    Ok(())
}
