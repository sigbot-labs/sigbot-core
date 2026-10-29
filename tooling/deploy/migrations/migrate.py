"""Apply ordered PostgreSQL schema migrations with drift detection and a single writer.

Uses psql (libpq standard PG* environment); credentials never appear in argv or output.
The runtime must use a different, non-owner database role.
"""

import argparse
import hashlib
import pathlib
import subprocess
import tempfile


class MigrationRunner:
    def __init__(self, directory: pathlib.Path):
        self.directory = directory

    @staticmethod
    def literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def migration_key(path: pathlib.Path) -> tuple:
        date, sequence = path.parent.name.split("-", 1)
        if len(date) != 8 or not date.isdigit() or not sequence.isdigit():
            raise ValueError("migration directory must be YYYYMMDD-number")
        return date, int(sequence), path.name

    def script(self) -> str:
        statements = [
            "\\set ON_ERROR_STOP on",
            "BEGIN;",
            "SELECT pg_advisory_xact_lock(1936287586);",
            "CREATE TABLE IF NOT EXISTS schema_migration ("
            "version TEXT PRIMARY KEY, sha256 TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now());",
        ]
        for path in sorted(self.directory.glob("*/*.ddl.sql"), key=self.migration_key):
            version = self.literal(str(path.relative_to(self.directory)))
            digest = self.literal(hashlib.sha256(path.read_bytes()).hexdigest())
            statements += [
                "DO $migration$ BEGIN IF EXISTS (SELECT 1 FROM schema_migration "
                f"WHERE version={version} AND sha256 <> {digest}) THEN "
                "RAISE EXCEPTION 'migration checksum drift'; END IF; END $migration$;",
                f"SELECT NOT EXISTS (SELECT 1 FROM schema_migration WHERE version={version}) AS apply \\gset",
                "\\if :apply",
                path.read_text(),
                f"INSERT INTO schema_migration(version,sha256) VALUES({version},{digest});",
                "\\endif",
            ]
        statements += ["COMMIT;"]
        return "\n".join(statements)

    def run(self, render: bool) -> None:
        script = self.script()
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
        parser.add_argument("--render", action="store_true", help="Print the credential-free psql script")
        options = parser.parse_args()
        cls(pathlib.Path(__file__).parent / "postgresql").run(options.render)


if __name__ == "__main__":
    MigrationRunner.main()
