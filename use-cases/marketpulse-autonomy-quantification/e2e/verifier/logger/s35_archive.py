import os

from common.rust import RustIntegration


def verify(context):
    archive = context.config["log_archive"]
    return RustIntegration.execute(context, "sigbot-logger", "tracing_archive", [
        "actual_tracing_offline_wal_restart_minio_index_and_cursor",
    ], {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env"),
        archive["access_key_env"]: os.environ[archive["access_key_env"]],
        archive["secret_key_env"]: os.environ[archive["secret_key_env"]]})
