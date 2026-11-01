use super::*;

#[test]
fn bindings_preserve_quotes_casts_comments_unicode_and_dollar_bodies() {
    let parameters = HashMap::from([
        ("id".into(), GenericValue::Int64(7)),
        ("id2".into(), GenericValue::Int64(8)),
    ]);
    let template = "SELECT ':id 中文', \"$id\", value::text, $$ :ignored $$, $body$ $ignored $body$ FROM t WHERE id=:id AND other=$id2 AND again=:id -- :ignored\n/* :ignored /* $nested */ */";
    let compiled = SqlParameters::compile(template, &parameters, ParameterStyle::Postgres).unwrap();
    assert_eq!(
        compiled.statement,
        template
            .replace("id=:id", "id=$1")
            .replace("other=$id2", "other=$2")
            .replace("again=:id", "again=$3")
    );
    assert_eq!(compiled.values.len(), 3);
    assert!(std::ptr::eq(compiled.values[0], compiled.values[2]));
    assert!(SqlParameters::compile("SELECT :missing", &parameters, ParameterStyle::Postgres).is_err());
    assert!(SqlParameters::compile("SELECT 'unterminated", &parameters, ParameterStyle::Postgres).is_err());
}

#[tokio::test]
async fn sqlite_binds_values_without_interpreting_them_as_sql() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let untrusted = "value'); DROP TABLE example; --";
    let parameters = HashMap::from([("value".into(), GenericValue::String(untrusted.into()))]);
    let rows: Vec<(String, String)> =
        crate::storage::SqlQueryExecutor::sqlite(&pool, "SELECT :value, ':value 中文'", &parameters)
            .await
            .unwrap();
    assert_eq!(rows, vec![(untrusted.into(), ":value 中文".into())]);
    let overflow = HashMap::from([("value".into(), GenericValue::Uint64(u64::MAX))]);
    assert!(
        crate::storage::SqlQueryExecutor::sqlite::<(i64,)>(&pool, "SELECT :value", &overflow)
            .await
            .is_err()
    );
}
