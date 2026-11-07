"""Observe actual product messages and committed SQL facts; never manufacture internal stages."""
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import httpx
import paho.mqtt.client as mqtt
import psycopg

from common.news.fixtures import TimelineFixtures


class PipelineVerifier:
    def __init__(self, context):
        self.context = context
        self.run_id = os.environ.get(context.config["workflow"]["run_id_env"], "")
        if not self.run_id:
            raise RuntimeError("a deployed real workflow run is required; set MARKETPULSE_WORKFLOW_RUN_ID")
        self.messages = []
        self.injected_ids = []
        self.errors = []
        self.ready = threading.Event()
        scope = context.config["scope"]
        self.base = f"internal/v2/{scope['env']}/{scope['tenant_id']}"
        self.workflow = f"{self.base}/workflow/{context.config['workflow_id']}"
        self.account = f"{self.base}/account/{context.config['account_id']}"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"marketpulse-observer-{context.run_id}",
                                  clean_session=False, manual_ack=True)
        settings = context.config["mqtt"]
        username = os.environ.get(settings["username_env"])
        if username:
            self.client.username_pw_set(username, os.environ.get(settings["password_env"]))
        self.client.on_connect = self.connected
        self.client.on_subscribe = self.subscribed
        self.client.on_message = self.received
        self.path = context.report_root / "workflow" / f"mqtt-round-{context.round_number}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connected(self, client, userdata, flags, reason, properties):
        if reason.is_failure:
            self.errors.append("MQTT connect rejected")
            self.ready.set()
            return
        client.subscribe([(self.workflow + "/#", 1), (self.account + "/#", 1)])

    def subscribed(self, client, userdata, mid, reasons, properties):
        if any(reason.is_failure for reason in reasons):
            self.errors.append("MQTT subscription rejected by ACL")
        self.ready.set()

    def received(self, client, userdata, message):
        try:
            value = json.loads(message.payload)
            if value.get("run_id") == self.run_id:
                assert value["version"] == 2
                assert value["scope"] == self.context.config["scope"]
                assert value["node_id"] > 0 and value["seq"] >= 0
                assert len(value["traceparent"].split("-")) == 4
                if value["kind"] == "request":
                    assert value["requestSpanId"] == value["traceparent"].split("-")[2]
                record = {"topic": message.topic, "payload": value, "qos": message.qos}
                with self.path.open("a") as handle:
                    handle.write(json.dumps(record) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                self.messages.append(record)
            client.ack(message.mid, message.qos)
        except Exception as error:
            self.errors.append(str(error))

    def snapshot(self):
        scope = self.context.config["scope"]
        with psycopg.connect(self.context.secret("database_url_env"), connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                counts = {}
                for table in ("eval_snapshot", "order_info", "ord_execution", "wlt_journal"):
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id=%s", (self.run_id,))
                    counts[table] = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM wlt_posting WHERE run_id=%s", (self.run_id,))
                counts["wlt_posting"] = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM wlt_journal WHERE run_id=%s AND source NOT LIKE 'funding/%%'", (self.run_id,))
                counts["wallet_execution_journal"] = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM order_info WHERE run_id=%s AND state='filled'", (self.run_id,))
                counts["filled_orders"] = cursor.fetchone()[0]
                cursor.execute("SELECT o.wallet_id,o.seq FROM order_info o LEFT JOIN ord_execution e "
                               "ON e.wallet_id=o.wallet_id AND e.exchange=o.exchange AND e.exchange_order_id=o.exchange_order_id "
                               "WHERE o.run_id=%s AND o.state='filled' GROUP BY o.wallet_id,o.producer_node_id,o.run_id,o.seq,o.intent "
                               "HAVING COALESCE(SUM(e.quantity),0)<>(o.intent->>'quantity')::NUMERIC", (self.run_id,))
                assert not cursor.fetchall(), "filled order quantity disagrees with actual executions"
                cursor.execute("SELECT asset,SUM(amount) FROM wlt_posting WHERE run_id=%s GROUP BY wallet_id,asset HAVING SUM(amount)<>0", (self.run_id,))
                assert not cursor.fetchall(), "wallet journal postings do not balance"
                cursor.execute("SELECT COUNT(*) FROM msg_outbox WHERE run_id=%s AND payload ? 'requestSpanId'", (self.run_id,))
                assert cursor.fetchone()[0] == 0, "ephemeral pairing was persisted"
                cursor.execute("SELECT COUNT(*) FROM mkt_event WHERE env=%s AND tenant_id=%s AND run_id=%s AND source_event_id=ANY(%s)",
                               (scope["env"], scope["tenant_id"], self.run_id, self.injected_ids))
                counts["mkt_event"] = cursor.fetchone()[0]
                for kind, suffix in (("snapshot", "%/feed/snapshot/result"), ("compute", "%/compute/result")):
                    cursor.execute("SELECT COUNT(*) FROM msg_outbox WHERE run_id=%s AND topic LIKE %s "
                                   "AND payload->'payload'->'data'->>'kind'=%s", (self.run_id, suffix, kind))
                    counts[kind + "_results"] = cursor.fetchone()[0]
                return counts

    def run(self):
        settings = self.context.config["mqtt"]
        self.client.connect(settings["host"], settings["port"], keepalive=30)
        self.client.loop_start()
        try:
            assert self.ready.wait(min(15, self.context.timeout_seconds)), "MQTT observer failed to subscribe"
            assert not self.errors, self.errors
            baseline = self.snapshot()
            fixtures = TimelineFixtures("twitter", self.context.root / "config")
            posts = fixtures.create()[:2]
            now = datetime.now(timezone.utc)
            base_id = (int(now.timestamp() * 1000) - 1288834974657) << 22
            for offset, post in enumerate(posts):
                post["id"] = str(base_id + offset)
                post["text"] = f"Synthetic Marketpulse acceptance observation {self.context.run_id}: {offset}"
                post["edit_history_post_ids"] = [post["id"]]
                post["created_at"] = (now - timedelta(seconds=offset + 1)).isoformat()
            self.injected_ids = [post["id"] for post in posts]
            with httpx.Client(timeout=15, headers=self.context.headers()) as client:
                # Newer sequence arrives first, older event follows, then duplicate.
                response = client.post(self.context.endpoint("twitter") + "/fixtures", json={"data": [posts[1], posts[0], posts[1]]})
                response.raise_for_status()
            required = {self.workflow + suffix for suffix in ("/feed/news", "/evaluation/snapshot", "/strategy/signal")}
            required |= {self.account + suffix for suffix in ("/order/event", "/execution/event", "/portfolio/snapshot",
                                                              "/wallet/reservation/request", "/wallet/reservation/result")}
            deadline = time.monotonic() + self.context.timeout_seconds
            counts = {}
            while time.monotonic() < deadline:
                assert not self.errors, self.errors
                observed = {message["topic"] for message in self.messages}
                if required <= observed:
                    counts = self.snapshot()
                    settled = (counts["order_info"] == counts["filled_orders"]
                               and counts["ord_execution"] == counts["filled_orders"]
                               and counts["wallet_execution_journal"] == counts["ord_execution"])
                    advanced = all(counts[name] > baseline[name] for name in
                                   ("order_info", "ord_execution", "wallet_execution_journal", "filled_orders"))
                    if all(counts.values()) and counts["mkt_event"] == 2 and settled and advanced:
                        break
                time.sleep(0.2)
            else:
                missing = sorted(required - {message["topic"] for message in self.messages})
                raise AssertionError(f"real internal pipeline did not finish; missing topics={missing}, committed counts={counts}")
            assert counts["mkt_event"] == 2, "duplicate news was persisted as a new fact"
            requests = {record["payload"].get("requestSpanId") for record in self.messages
                        if record["topic"].endswith("/wallet/reservation/request")}
            results = [record["payload"].get("requestSpanId") for record in self.messages
                       if record["topic"].endswith("/wallet/reservation/result")]
            assert requests and results and all(pairing and pairing in requests for pairing in results), "reservation reply did not echo its ephemeral producer span"
            return {"run_id": self.run_id, "observed_topics": sorted(required), "committed": counts,
                    "baseline_financial_counts": {name: baseline[name] for name in
                                                  ("order_info", "ord_execution", "wallet_execution_journal", "filled_orders")},
                    "mqtt_evidence": str(self.path), "scope": "actual Rust service chain; external fixtures only"}
        finally:
            self.client.disconnect()
            self.client.loop_stop()


def verify(context):
    return PipelineVerifier(context).run()
