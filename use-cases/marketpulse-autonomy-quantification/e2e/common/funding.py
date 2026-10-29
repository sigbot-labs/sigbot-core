"""An external opening-capital fixture; only the wallet writes financial tables."""
import json
import os
import time

import paho.mqtt.client as mqtt
import psycopg


class FundingFixture:
    @staticmethod
    def publish(context, run_id):
        scope = context.config["scope"]
        account_id = context.config["account_id"]
        # This isolated case deliberately assigns equal wallet/account IDs in both bootstraps.
        # A fixed external source identity makes repeated bootstrap idempotent.
        occurred = "2026-10-27T00:00:00Z"
        payload = {"version": 2, "scope": scope, "workflow_id": context.config["workflow_id"], "account_id": account_id,
            "node_id": 9, "target_node_id": 8, "run_id": run_id, "seq": 0, "attempt_no": 0, "kind": "event",
            "event_time": occurred, "available_time": occurred, "produced_at": occurred,
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-cccccccccccccccc-01", "payload": {
                "wallet_id": account_id, "account_id": account_id, "exchange": "BINANCE", "source": "e2e-funding-provider",
                "source_event_id": "marketpulse-opening-USDT", "asset": "USDT", "amount": "10000", "occurred_at": occurred}}
        settings = context.config["mqtt"]
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"marketpulse-funding-{context.run_id}")
        username = os.environ.get(settings["username_env"])
        if username:
            client.username_pw_set(username, os.environ.get(settings["password_env"]))
        client.connect(settings["host"], settings["port"], keepalive=30)
        client.loop_start()
        try:
            topic = f"internal/v2/{scope['env']}/{scope['tenant_id']}/{scope['space_id']}/account/{account_id}/execution/event"
            message = client.publish(topic, json.dumps(payload), qos=1)
            message.wait_for_publish(timeout=15)
            if not message.is_published():
                raise RuntimeError("opening funding fixture did not receive broker ACK")
            deadline = time.monotonic() + min(context.timeout_seconds, 30)
            while time.monotonic() < deadline:
                with psycopg.connect(context.secret("database_url_env"), connect_timeout=10) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) FROM wallet_journal WHERE wallet_id=%s AND source='funding/e2e-funding-provider' AND fact_ref='marketpulse-opening-USDT'", (account_id,))
                        if cursor.fetchone()[0] == 1:
                            return
                time.sleep(0.2)
            raise RuntimeError("real wallet did not commit opening funding fact")
        finally:
            client.disconnect()
            client.loop_stop()
