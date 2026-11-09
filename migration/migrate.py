"""Apply the single ordered Sigbot schema baseline for PostgreSQL or SQLite."""

import argparse
import hashlib
import os
import pathlib
import re
import sqlite3
import subprocess
import tempfile


class MigrationRunner:
    VERSION_PATTERN = re.compile(r"^[0-9]{14}$")
    PROVIDERS = ("postgres", "sqlite")

    def __init__(self, directory: pathlib.Path, provider: str, sqlite_url: str | None = None):
        if provider not in self.PROVIDERS:
            raise ValueError(f"unsupported migration provider: {provider}")
        self.directory = directory
        self.provider = provider
        self.sqlite_url = sqlite_url

    def baselines(self) -> list[tuple[str, pathlib.Path, pathlib.Path, str]]:
        baselines = []
        for version_dir in sorted(path for path in self.directory.iterdir() if path.is_dir()):
            if not self.VERSION_PATTERN.fullmatch(version_dir.name):
                continue
            provider_dir = version_dir / self.provider
            ddl = provider_dir / "init.ddl.sql"
            dml = provider_dir / "init.dml.sql"
            if not ddl.is_file() or not dml.is_file():
                raise ValueError(f"{provider_dir} must contain init.ddl.sql and init.dml.sql")
            digest = hashlib.sha256(ddl.read_bytes() + b"\0" + dml.read_bytes()).hexdigest()
            baselines.append((version_dir.name, ddl, dml, digest))
        if not baselines:
            raise ValueError(f"no {self.provider} migration baseline found")
        return baselines

    @staticmethod
    def literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def postgres_script(self) -> str:
        statements = [
            "\\set ON_ERROR_STOP on",
            "BEGIN;",
            "SELECT pg_advisory_xact_lock(1936287586);",
            "CREATE TABLE IF NOT EXISTS mig_version ("
            "version TEXT PRIMARY KEY, sha256 TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now());",
        ]
        for version, ddl, dml, digest in self.baselines():
            version_literal = self.literal(version)
            digest_literal = self.literal(digest)
            statements.extend(
                [
                    "DO $migration$ BEGIN IF EXISTS (SELECT 1 FROM mig_version "
                    f"WHERE version={version_literal} AND sha256 <> {digest_literal}) THEN "
                    "RAISE EXCEPTION 'migration checksum drift'; END IF; END $migration$;",
                    f"SELECT NOT EXISTS (SELECT 1 FROM mig_version WHERE version={version_literal}) AS apply \\gset",
                    "\\if :apply",
                    ddl.read_text(),
                    dml.read_text(),
                    f"INSERT INTO mig_version(version,sha256) VALUES({version_literal},{digest_literal});",
                    "\\endif",
                ]
            )
        statements.append("COMMIT;")
        return "\n".join(statements)

    @staticmethod
    def sqlite_statements(script: str):
        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                if statement.strip():
                    yield statement
                statement = ""
        remaining = "\n".join(
            line for line in statement.splitlines() if line.strip() and not line.lstrip().startswith("--")
        )
        if remaining.strip():
            raise ValueError("incomplete SQLite migration statement")

    def apply_sqlite(self) -> None:
        url = self.sqlite_url or os.environ.get("SIGBOT_MIGRATION_SQLITE_URL")
        if not url:
            raise ValueError("SQLite migration requires --sqlite-url or SIGBOT_MIGRATION_SQLITE_URL")
        path = url.removeprefix("sqlite://")
        connection = sqlite3.connect(path, isolation_level=None)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS mig_version ("
                "version TEXT PRIMARY KEY, sha256 TEXT NOT NULL, "
                "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            for version, ddl, dml, digest in self.baselines():
                applied = connection.execute(
                    "SELECT sha256 FROM mig_version WHERE version=?", (version,)
                ).fetchone()
                if applied and applied[0] != digest:
                    raise ValueError(f"migration checksum drift: {version}")
                if applied:
                    continue
                for statement in self.sqlite_statements(ddl.read_text() + "\n" + dml.read_text()):
                    connection.execute(statement)
                connection.execute(
                    "INSERT INTO mig_version(version,sha256) VALUES(?,?)", (version, digest)
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def run(self, render: bool) -> None:
        if self.provider == "sqlite":
            if render:
                for _, ddl, dml, _ in self.baselines():
                    print(ddl.read_text())
                    print(dml.read_text())
            else:
                self.apply_sqlite()
            return
        script = self.postgres_script()
        if render:
            print(script)
            return
        with tempfile.TemporaryFile(mode="w+") as stream:
            stream.write(script)
            stream.seek(0)
            subprocess.run(["psql", "--no-psqlrc", "--quiet"], stdin=stream, check=True)

    @classmethod
    def main(cls) -> None:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--provider", choices=cls.PROVIDERS, default="postgres")
        parser.add_argument("--sqlite-url")
        parser.add_argument("--render", action="store_true")
        options = parser.parse_args()
        cls(pathlib.Path(__file__).parent, options.provider, options.sqlite_url).run(options.render)


if __name__ == "__main__":
    MigrationRunner.main()
