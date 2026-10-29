import hashlib
import json
import os
import time

import psycopg

from common.storage import ArchiveStorage


def verify(context):
    run_id = os.environ.get(context.config["pipeline"]["run_id_env"])
    if not run_id:
        raise RuntimeError("a real pipeline run is required before log archive verification")
    storage = ArchiveStorage(context)
    deadline = time.monotonic() + context.timeout_seconds
    while time.monotonic() < deadline:
        with psycopg.connect(context.secret("database_url_env"), connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT object_key,sha256,byte_length,cursor FROM log_chunk WHERE run_id=%s AND node_id=5 ORDER BY cursor LIMIT 32", (run_id,))
                records = cursor.fetchall()
        if records:
            verified = []
            rust_verified = []
            for key, checksum, size, cursor in records:
                payload = storage.read(key)
                if len(payload) != size or hashlib.sha256(payload).hexdigest() != checksum:
                    raise AssertionError("immutable log object differs from its committed index")
                event = json.loads(payload)
                if event["scope"] != context.config["scope"] or event["run_id"] != run_id:
                    raise AssertionError("log object crossed workflow isolation")
                if "requestSpanId" in event:
                    raise AssertionError("ephemeral reply correlation leaked into immutable logs")
                if "marketpulse:" in event["payload"]["content"]:
                    verified.append({"object_key": key, "sha256": checksum, "cursor": cursor})
                if event["payload"]["source"].startswith("rust:"):
                    rust_verified.append({"object_key": key, "sha256": checksum, "cursor": cursor})
            if verified and rust_verified:
                return {"verified_objects": verified, "rust_objects": rust_verified,
                        "path": "real Python stdout + Rust tracing → logservice WAL → MinIO → PostgreSQL index"}
        time.sleep(0.5)
    raise AssertionError("both real strategy stdout and Rust tracing must reach the immutable object archive")
