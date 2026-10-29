import socket

import httpx
import psycopg


def verify(context):
    result = {}
    with httpx.Client(timeout=context.timeout_seconds) as client:
        for name in ("twitter", "truthsocial", "openai"):
            response = client.get(context.endpoint(name) + "/healthz")
            response.raise_for_status()
            assert response.json()["status"] == "ok"
            result[name] = "healthy"
    mqtt = context.config["mqtt"]
    with socket.create_connection((mqtt["host"], mqtt["port"]), timeout=context.timeout_seconds):
        result["mqtt_tcp"] = "reachable (protocol verified by pipeline scenario)"
    with psycopg.connect(context.secret("database_url_env"), connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('market_event'),to_regclass('messager_outbox'),to_regclass('workflow_checkpoint')")
            assert all(cursor.fetchone()), "runtime migrations have not been applied"
            result["postgres"] = "runtime schema present"
    return result
