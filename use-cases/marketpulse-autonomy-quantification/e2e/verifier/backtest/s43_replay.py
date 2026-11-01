"""Read committed evidence from actual frozen-history replay and normal trading services."""
import json
import os
import time

import psycopg

from common.replay import ReplayFixture, ReplayServices
from common.services import ProductServices


class ReplayVerifier:
    def __init__(self, context):
        self.context = context

    def economics(self, connection, run_id):
        return connection.execute("SELECT symbol,side,price::text,quantity::text,fee::text,fee_asset "
                                  "FROM ord_execution WHERE run_id=%s ORDER BY executed_at,seq", (run_id,)).fetchall()

    def wait(self, target, services):
        deadline = time.monotonic() + self.context.timeout_seconds
        while time.monotonic() < deadline:
            with psycopg.connect(target.secret("database_url_env")) as connection:
                row = connection.execute("SELECT state,steps,virtual_time FROM bt_checkpoint WHERE run_id=%s",
                                         (target.run_id,)).fetchone()
            if row and row[0] == "complete":
                return row
            records = json.loads(services.manifest.read_text())
            if not all(ProductServices.is_running(record) for record in records):
                raise AssertionError("an actual historical replay participant exited; inspect its runtime log")
            time.sleep(0.2)
        raise AssertionError(f"actual historical replay never crossed its durable clock barrier; last checkpoint={row}")

    def run(self):
        first, economics = self.execute()
        second, repeated = self.execute(first["source"], 2)
        assert repeated == economics, "identical frozen inputs changed replay execution economics"
        assert first["steps"] == second["steps"], "identical frozen input changed clock admission count"
        return {"passes": [first, second], "equal_execution_economics": True,
                "comparison": "two isolated virtual-clock runs over identical frozen history, AI outputs and matching configuration"}

    def execute(self, history=None, pass_number=1):
        for manifest in (self.context.root / "reports").glob("*/replay/**/runtime/processes.json"):
            ProductServices.stop(manifest)
        target, history = ReplayFixture.prepare(self.context, history, pass_number)
        keys = [target.config["workflow"]["run_id_env"], target.config["mqtt"]["password_env"], target.config["mqtt"]["username_env"]]
        previous = {key: os.environ.get(key) for key in keys}
        services = ReplayServices(target, history)
        try:
            ReplayFixture.activate(target)
            services.start()
            state, steps, virtual_time = self.wait(target, services)
            with psycopg.connect(target.secret("database_url_env")) as connection:
                source = self.economics(connection, history["run_id"])
                result = self.economics(connection, target.run_id)
                assert source and result and len(result) == len(source), "replay must execute every acceptance observation"
                # Live message/query latency is not the historical virtual clock.
                # Verify the real frozen source prices, then compare two isolated
                # replays in run() instead of assuming their ticks equal live ticks.
                for symbol, _, price, _, _, _ in result:
                    assert connection.execute("SELECT EXISTS(SELECT 1 FROM mkt_event WHERE env=%s AND tenant_id=%s "
                        "AND run_id=%s AND symbol=%s AND event_type='market' AND available_at BETWEEN %s AND %s AND recorded_at<=%s "
                        "AND (payload->>'price')::numeric=%s::numeric)",
                        (history["scope"]["env"], history["scope"]["tenant_id"], history["run_id"],
                         symbol, history["from"], history["until"], history["frozen_at"], price)).fetchone()[0], "replay fill has no actual frozen source tick"
                pending = connection.execute("SELECT COUNT(*) FROM msg_outbox WHERE run_id=%s AND state<>'sent'",
                                             (target.run_id,)).fetchone()[0]
                assert pending == 0, "replay completed before outgoing effects reached the real broker"
                incomplete = connection.execute("SELECT COUNT(*) FROM ord_order WHERE run_id=%s AND state<>'filled'",
                                                (target.run_id,)).fetchone()[0]
                assert incomplete == 0, "replay completed before execution acknowledgement was reconciled"
                journals = connection.execute("SELECT COUNT(*) FROM wlt_journal WHERE run_id=%s AND source='execution'",
                                              (target.run_id,)).fetchone()[0]
                assert journals == len(result), "replay completed before the wallet booked real executions"
                assert not connection.execute("SELECT asset,SUM(amount) FROM wlt_posting WHERE run_id=%s GROUP BY asset HAVING SUM(amount)<>0",
                                              (target.run_id,)).fetchall(), "replay journal is unbalanced"
                assert connection.execute("SELECT COUNT(*) FROM eval_snapshot WHERE run_id=%s", (target.run_id,)).fetchone()[0] == 0, "replay reran the LLM"
                ai = connection.execute("SELECT payload->'payload' FROM msg_outbox WHERE run_id=%s AND topic LIKE '%%/evaluation/snapshot' ORDER BY seq",
                                        (target.run_id,)).fetchall()
                expected_ai = connection.execute("SELECT version,probabilities,as_of FROM eval_snapshot WHERE run_id=%s AND available_at BETWEEN %s AND %s "
                                                 "AND recorded_at<=%s ORDER BY available_at,version",
                                                 (history["run_id"], history["from"], history["until"], history["frozen_at"])).fetchall()
                assert ai and len(ai) == len(expected_ai), "not all frozen AI outputs were replayed"
                assert [(row[0]["version"], row[0]["probabilities"]) for row in ai] == [(row[0], row[1]) for row in expected_ai], "AI replay changed frozen probabilities"
                future = connection.execute("SELECT COUNT(*) FROM msg_outbox WHERE run_id=%s AND (payload->>'available_time')::timestamptz>%s",
                                            (target.run_id, history["until"])).fetchone()[0]
                assert future == 0, "wall clock leaked into historical message availability"
                baseline = (result, journals, connection.execute("SELECT COUNT(*) FROM wlt_posting WHERE run_id=%s", (target.run_id,)).fetchone()[0])
            services.restart({"backtest"})
            self.wait(target, services)
            with psycopg.connect(target.secret("database_url_env")) as connection:
                after = (self.economics(connection, target.run_id),
                         connection.execute("SELECT COUNT(*) FROM wlt_journal WHERE run_id=%s AND source='execution'", (target.run_id,)).fetchone()[0],
                         connection.execute("SELECT COUNT(*) FROM wlt_posting WHERE run_id=%s", (target.run_id,)).fetchone()[0])
                assert after == baseline, "restarting a complete historical run duplicated financial effects"
            return {"source": history, "run_id": target.run_id, "scope": target.config["scope"], "state": state,
                    "steps": steps, "virtual_time": virtual_time.isoformat(), "executions": len(result), "frozen_ai_outputs": len(ai),
                    "restart_idempotent": True, "runtime_manifest": str(services.manifest),
                    "implementation": "actual Rust replay source → MQTT → strategy/query/order/wallet with a durable clock barrier"}, result
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def verify(context):
    return ReplayVerifier(context).run()
