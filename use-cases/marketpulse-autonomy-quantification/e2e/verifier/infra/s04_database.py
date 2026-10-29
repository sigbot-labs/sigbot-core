import hashlib

import psycopg


def verify(context):
    path = context.root.parents[2] / "tooling/deploy/migrations/tests/wallet_invariants.sql"
    sql = "\n".join(line for line in path.read_text().splitlines() if not line.startswith("\\"))
    with psycopg.connect(context.secret("database_url_env"), autocommit=True, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            cursor.execute("SELECT version,sha256 FROM schema_migration ORDER BY version")
            versions = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM s_wallets WHERE id=9000000001")
            assert cursor.fetchone()[0] == 0, "database invariant fixture was not rolled back"
    return {"script_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "schema_migrations": versions,
            "real_postgres_constraints": ["immutable posting", "immutable journal", "balanced journal", "nonempty journal"],
            "fixture_rolled_back": True}
