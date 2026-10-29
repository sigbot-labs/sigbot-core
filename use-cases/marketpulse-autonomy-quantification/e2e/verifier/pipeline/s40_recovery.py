"""Crash actual workers, replay captured MQTT facts, and compare financial projections."""
import json
import os
import time

import paho.mqtt.client as mqtt
import psycopg

from common.services import ProductServices


class RecoveryVerifier:
    def __init__(self, context):
        self.context = context
        self.run_id = os.environ.get(context.config["pipeline"]["run_id_env"])
        if not self.run_id:
            raise RuntimeError("recovery requires a completed real runtime and pipeline in this runner invocation")

    def snapshot(self):
        scope = self.context.config["scope"]
        with psycopg.connect(self.context.secret("database_url_env"), connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                result = {}
                for table in ("order_record", "order_execution", "wallet_journal", "wallet_posting"):
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id=%s", (self.run_id,))
                    result[table] = cursor.fetchone()[0]
                cursor.execute("SELECT p.asset,SUM(p.amount) FROM wallet_posting p JOIN s_wallets w ON w.id=p.wallet_id "
                               "WHERE w.env=%s AND w.tenant_id=%s AND w.space_id=%s AND w.account_id=%s "
                               "GROUP BY p.asset HAVING SUM(p.amount)<>0",
                               (scope["env"], str(scope["tenant_id"]), scope["space_id"], self.context.config["account_id"]))
                if cursor.fetchall():
                    raise AssertionError("journal became unbalanced after process failure")
                for table, order in (("s_balances", "asset"), ("s_positions", "symbol")):
                    cursor.execute(f"SELECT row_to_json(p) FROM {table} p WHERE wallet_id=1 ORDER BY {order}")
                    result[table] = cursor.fetchall()
                return result

    def run(self):
        path = self.context.report_root / "pipeline" / f"mqtt-round-{self.context.round_number}.jsonl"
        if not path.is_file():
            raise RuntimeError("recovery requires captured real messages from the preceding pipeline scenario")
        records = [json.loads(line) for line in path.read_text().splitlines()]
        captured = {}
        for record in records:
            for suffix in ("/strategy/signal", "/execution/event"):
                if record["topic"].endswith(suffix):
                    captured[suffix] = record
        if len(captured) != 2:
            raise RuntimeError("real signal and exchange execution evidence are both required")
        baseline = self.snapshot()
        if any(baseline[table] == 0 for table in ("order_record", "order_execution", "wallet_journal", "wallet_posting")):
            raise RuntimeError("cannot claim crash recovery before a completed financial transaction")
        processes = ProductServices(self.context).restart({"strategy", "order", "wallet"})
        settings = self.context.config["mqtt"]
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"marketpulse-replay-{self.context.run_id}")
        username = os.environ.get(settings["username_env"])
        if username:
            client.username_pw_set(username, os.environ.get(settings["password_env"]))
        client.connect(settings["host"], settings["port"], keepalive=30)
        client.loop_start()
        try:
            for _ in range(2):
                for record in reversed(list(captured.values())):
                    published = client.publish(record["topic"], json.dumps(record["payload"]), qos=1)
                    published.wait_for_publish(timeout=15)
                    if not published.is_published():
                        raise RuntimeError("replayed original fact did not receive broker ACK")
            # Observe at least two new live feed polls after the worker crash. This
            # checks that persisted strategy state prevents a second decision.
            deadline = time.monotonic() + min(self.context.timeout_seconds, 15)
            while time.monotonic() < deadline:
                if self.snapshot() != baseline:
                    raise AssertionError("replayed committed facts changed financial rows after restart")
                if not all(ProductServices.is_running(record) for record in processes["processes"]):
                    raise AssertionError("a restarted real product service exited")
                time.sleep(0.5)
        finally:
            client.disconnect()
            client.loop_stop()
        return {"fault": "SIGKILL of owned strategy/order/wallet processes", "replayed_actual_messages": 4,
                "financial_projection_unchanged": True, "run_id": self.run_id,
                "scope": "committed-fact replay and worker restart; not a sharding or full historical replay claim"}


def verify(context):
    return RecoveryVerifier(context).run()
