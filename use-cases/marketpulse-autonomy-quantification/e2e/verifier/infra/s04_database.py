import hashlib
import importlib.util
import pathlib
import sqlite3
import tempfile

import psycopg


class DatabaseVerifier:
    def __init__(self, context):
        self.context = context
        self.repository = context.root.parents[2]
        module_path = self.repository / "migration/migrate.py"
        spec = importlib.util.spec_from_file_location("sigbot_migration", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.runner = module.MigrationRunner

    @staticmethod
    def baseline(root, provider, ddl, dml=""):
        target = root / "20261031000000" / provider
        target.mkdir(parents=True)
        (target / "init.ddl.sql").write_text(ddl)
        (target / "init.dml.sql").write_text(dml)

    def sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            database = root / "migration.sqlite"
            self.baseline(root, "sqlite", "CREATE TABLE sample(id INTEGER PRIMARY KEY);",
                          "INSERT INTO sample VALUES(1);")
            migration = self.runner(root, "sqlite", str(database))
            migration.run(False)
            migration.run(False)
            with sqlite3.connect(database) as connection:
                if connection.execute("SELECT COUNT(*) FROM sample").fetchone()[0] != 1:
                    raise AssertionError("SQLite baseline is not idempotent")
            (root / "20261031000000/sqlite/init.dml.sql").write_text("INSERT INTO sample VALUES(2);")
            try:
                migration.run(False)
            except ValueError as error:
                if "checksum drift" not in str(error):
                    raise
            else:
                raise AssertionError("SQLite migration checksum drift was accepted")

    def worker_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            database = pathlib.Path(directory) / "worker.sqlite"
            self.runner(self.repository / "migration", "sqlite", str(database)).run(False)
            with sqlite3.connect(database) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                tenant = "0192f7b2-5c00-7000-8000-000000000201"
                foreign_tenant = "0192f7b2-5c00-7000-8000-000000000202"
                workflow = "0192f7b2-5c00-7000-8000-000000000203"
                connection.execute(
                    "INSERT INTO workflow_info(id,env,tenant_id,name) VALUES(?,?,?,'worker owner')",
                    (workflow, "test", tenant),
                )
                statement = (
                    "INSERT INTO cluster_info(id,env,tenant_id,workflow_id,name,provider,conn_url,"
                    "auth_config,configuration,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
                )
                valid = (
                    "0192f7b2-5c00-7000-8000-000000000204", "test", tenant, workflow, "docker-local",
                    "docker", "unix:///var/run/docker.sock", '{"auth_type":"docker_anonymous"}',
                    '{"runtime_type":"docker","network":"tenant-one","environment_file":"tenant-1-default"}',
                    "test", "test",
                )
                connection.execute(statement, valid)
                try:
                    connection.execute(statement, ("0192f7b2-5c00-7000-8000-000000000205", "test",
                                                     foreign_tenant) + valid[3:])
                except sqlite3.IntegrityError:
                    pass
                else:
                    raise AssertionError("cross-tenant worker binding was accepted")

    def postgres(self):
        path = self.context.root / "config/sigbot/wallet_invariants.sql"
        with psycopg.connect(self.context.secret("database_url_env"), autocommit=True, connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute(path.read_text())
                cursor.execute("SELECT version,sha256 FROM mig_version ORDER BY version")
                versions = cursor.fetchall()
                cursor.execute("SELECT COUNT(*) FROM ledger_info WHERE id='0192f7b2-5c00-7000-8000-000000000104'")
                if cursor.fetchone()[0] != 0:
                    raise AssertionError("database invariant fixture was not rolled back")
        script = self.runner(self.repository / "migration", "postgres").postgres_script()
        baseline = self.repository / "migration/20261031000000/postgres"
        ddl_prefix = (baseline / "init.ddl.sql").read_text().strip()[:80]
        dml_prefix = (baseline / "init.dml.sql").read_text().strip()[:80]
        if "pg_advisory_xact_lock" not in script or script.index(ddl_prefix) > script.index(dml_prefix):
            raise AssertionError("PostgreSQL migration ordering or locking is missing")
        return path, versions

    def run(self):
        self.sqlite()
        self.worker_scope()
        path, versions = self.postgres()
        return {"script_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "schema_migrations": versions,
                "sqlite": ["idempotent baseline", "checksum drift", "worker scope foreign key"],
                "real_postgres_constraints": ["immutable posting", "immutable journal", "balanced journal",
                                              "nonempty journal"], "fixture_rolled_back": True}


def verify(context):
    return DatabaseVerifier(context).run()
