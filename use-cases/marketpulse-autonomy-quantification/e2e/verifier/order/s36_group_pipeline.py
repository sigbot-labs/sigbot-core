"""Exercise real strategy intents, Wallet rejection and compensating executions."""
import json
import os
import time
from decimal import Decimal

import paho.mqtt.publish as mqtt_publish
import psycopg

from common.broker import BrokerCredentials
from common.group import GroupFixture, GroupServices
from common.services import ProductServices


class GroupPipelineVerifier:
    def __init__(self, context):
        self.context = context

    def financial_state(self, target):
        with psycopg.connect(target.secret("database_url_env")) as connection:
            result = {}
            for table in ("order_record", "order_execution", "wallet_journal", "wallet_posting"):
                result[table] = connection.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id=%s", (target.run_id,)).fetchone()[0]
            result["balances"] = connection.execute("SELECT asset,available::text,locked::text FROM s_balances WHERE wallet_id=%s ORDER BY asset",
                                                    (target.config["account_id"],)).fetchall()
            return result

    def run(self):
        for manifest in (self.context.root / "reports").glob("*/groups/**/runtime/processes.json"):
            ProductServices.stop(manifest)
        target, accounts = GroupFixture.prepare(self.context)
        keys = [target.config["pipeline"]["run_id_env"], target.config["mqtt"]["password_env"], target.config["mqtt"]["username_env"]]
        previous = {key: os.environ.get(key) for key in keys}
        services = GroupServices(target, accounts)
        try:
            BrokerCredentials.initialize(target)
            os.environ[target.config["pipeline"]["run_id_env"]] = target.run_id
            services.start()
            deadline = time.monotonic() + self.context.timeout_seconds
            state = None
            while time.monotonic() < deadline:
                with psycopg.connect(target.secret("database_url_env")) as connection:
                    row = connection.execute("SELECT state FROM execution_group WHERE run_id=%s AND producer_node_id=5 AND seq=0",
                                             (target.run_id,)).fetchone()
                state = row[0] if row else None
                if state == "compensated":
                    break
                if state in {"compensation_failed", "completed", "held"}:
                    raise AssertionError(f"expected real Wallet rejection followed by successful compensation, got {state}")
                if not all(ProductServices.is_running(record) for record in json.loads(services.manifest.read_text())):
                    raise AssertionError("an actual multi-account product participant exited")
                time.sleep(0.2)
            else:
                raise AssertionError(f"real multi-account coordinator did not settle: {state}")
            with psycopg.connect(target.secret("database_url_env")) as connection:
                assert connection.execute("SELECT COUNT(*) FROM execution_group_leg WHERE run_id=%s", (target.run_id,)).fetchone()[0] == 2
                rows = connection.execute("SELECT side,price,quantity,fee FROM order_execution WHERE run_id=%s AND wallet_id=%s ORDER BY executed_at,seq",
                                          (target.run_id, accounts[0])).fetchall()
                assert len(rows) == 2 and [row[0] for row in rows] == ["BUY", "SELL"], "actual buy and reverse execution are both required"
                assert rows[0][2] == rows[1][2] == Decimal("0.001")
                assert connection.execute("SELECT COUNT(*) FROM order_execution WHERE run_id=%s AND wallet_id=%s", (target.run_id, accounts[1])).fetchone()[0] == 0
                assert connection.execute("SELECT COUNT(*) FROM order_record WHERE run_id=%s AND wallet_id=%s AND state='rejected'",
                                          (target.run_id, accounts[1])).fetchone()[0] == 1, "second wallet must reject the actual reservation"
                assert connection.execute("SELECT COUNT(*) FROM wallet_journal WHERE wallet_id=%s", (accounts[1],)).fetchone()[0] == 0
                assert connection.execute("SELECT COUNT(*) FROM wallet_journal WHERE run_id=%s AND source='execution'", (target.run_id,)).fetchone()[0] == 2
                assert not connection.execute("SELECT asset,SUM(amount) FROM wallet_posting WHERE run_id=%s GROUP BY asset HAVING SUM(amount)<>0",
                                              (target.run_id,)).fetchall()
                assert connection.execute("SELECT COUNT(*) FROM s_positions WHERE wallet_id=%s AND size<>0", (accounts[0],)).fetchone()[0] == 0
                expected_cash = Decimal("10000") - rows[0][1] * rows[0][2] + rows[1][1] * rows[1][2] - rows[0][3] - rows[1][3]
                available, locked = connection.execute("SELECT available,locked FROM s_balances WHERE wallet_id=%s AND asset='USDT'", (accounts[0],)).fetchone()
                assert available == expected_cash and locked == 0, "real fees and compensation cash flows disagree with Wallet"
                signals = connection.execute("SELECT topic,payload FROM messager_outbox WHERE run_id=%s AND topic LIKE '%%/strategy/signal' ORDER BY node_id,seq",
                                             (target.run_id,)).fetchall()
                assert len(signals) == 3, "two original intents and one compensation must be persisted"
                compensation = [payload for _, payload in signals if payload["node_id"] == 6]
                assert len(compensation) == 1 and compensation[0]["payload"]["reduce_only"]
                assert compensation[0]["payload"]["side"] == "SELL"
            baseline = self.financial_state(target)
            services.restart({"strategy", "order-first", "order-second", "wallet-first", "wallet-second"})
            settings = target.config["mqtt"]
            mqtt_publish.multiple([{"topic": topic, "payload": json.dumps(payload), "qos": 1} for _ in range(2) for topic, payload in signals],
                                  hostname=settings["host"], port=settings["port"], client_id=f"group-replay-{target.run_id}",
                                  auth={"username": os.environ[settings["username_env"]], "password": os.environ[settings["password_env"]]})
            deadline = time.monotonic() + min(15, self.context.timeout_seconds)
            while time.monotonic() < deadline:
                assert self.financial_state(target) == baseline, "duplicate group facts changed financial projections after restart"
                time.sleep(0.5)
            return {"run_id": target.run_id, "scope": target.config["scope"], "state": state, "accounts": accounts,
                    "actual_executions": 2, "compensation_intents": 1, "duplicate_financial_effects": 0,
                    "runtime_manifest": str(services.manifest),
                    "coverage": "same-venue two-account compensation with real Binance facts and product SimulatedExchange; not cross-venue acceptance"}
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def verify(context):
    return GroupPipelineVerifier(context).run()
