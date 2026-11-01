"""Configure the product's frozen-history replay; this module never replays business logic."""
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import psycopg

from common.broker import BrokerCredentials
from common.context import UuidV7
from common.funding import FundingFixture
from common.runtime import RuntimeConfiguration
from common.services import ProductServices


class ReplayConfiguration(RuntimeConfiguration):
    def __init__(self, context, history):
        super().__init__(context)
        self.run_id = context.run_id
        self.history = history
        self.mode = "backtest"
        self.management_port = 19510

    def strategy(self):
        argument = super().strategy()
        config = json.loads(argument["properties"]["runtime_config"])
        config["virtual_clock"] = True
        config["configuration"]["acceptance_run_id"] = self.history["run_id"]
        config["edges"] = [edge for edge in config["edges"] if edge["source_node_id"] not in (1, 2)]
        config["edges"].append({"source_node_id": 3, "target_node_id": 5, "kind": "feed_news"})
        argument["properties"]["runtime_config"] = json.dumps(config)
        return argument

    def order(self):
        argument = super().order()
        config = json.loads(argument["properties"]["runtime_config"])
        config["exchange"]["endpoint"] = "http://127.0.0.1:18485"
        config["virtual_clock"] = True
        argument["properties"]["runtime_config"] = json.dumps(config)
        argument["properties"]["clock_mode"] = "virtual"
        return argument

    def subscriptions(self):
        scope = self.scope
        key = f"{scope['env']}:{scope['tenant_id']}"
        base = f"internal/v2/{scope['env']}/{scope['tenant_id']}"
        workflow = f"{base}/workflow/{self.workflow_id}"
        account = f"{base}/account/{self.account_id}"
        query = f"datafeed.query:{key}:{self.workflow_id}:3"
        wallet = f"wallet:{key}:{self.account_id}:8"
        routes = [
            (3, 5, workflow + "/feed/news", "strategy.5", "any"),
            (3, 5, workflow + "/feed/market", "strategy.5", "any"),
            (4, 5, workflow + "/evaluation/snapshot", "strategy.5", "any"),
            (5, 3, workflow + "/feed/snapshot/request", query, "any"),
            (3, 5, workflow + "/feed/snapshot/result", "strategy.5", "any"),
            (5, 3, workflow + "/compute/request", query, "any"),
            (3, 5, workflow + "/compute/result", "strategy.5", "any"),
            (5, 5, workflow + "/runtime/event", "strategy.5", "any"),
            (5, 6, workflow + "/strategy/signal", f"order:{key}:{self.workflow_id}:6", "any"),
            (6, 8, account + "/wallet/reservation/request", wallet + ".reservation", "any"),
            (8, 6, account + "/wallet/reservation/result", f"order:{key}:{self.workflow_id}:6:reservation", "any"),
            (7, 8, account + "/execution/event", "wallet.execution.8", "execution"),
            (7, 6, account + "/execution/event", f"order.execution:{key}:{self.workflow_id}:6", "execution"),
            (6, 8, account + "/order/event", wallet + ".status", "order_status"),
        ]
        return {"scope": scope, "run_id": self.run_id,
                "subscriptions": [{"producer_node_id": producer, "target_node_id": target, "topic": topic,
                                   "consumer": consumer, "payload_kind": kind}
                                  for producer, target, topic, consumer, kind in routes],
                "terminal_topics": [account + "/portfolio/snapshot", account + "/order/event"],
                "max_messages": 10000, "timeout_seconds": 45}

    def backtest(self):
        argument = super().backtest()
        argument["properties"].update(bind_addr="127.0.0.1:18485", clock_mode="virtual")
        matching = json.loads(argument["properties"]["matching_config"])
        matching["virtual_clock"] = True
        argument["properties"]["matching_config"] = json.dumps(matching)
        argument["properties"]["replay_config"] = json.dumps({
            "identity": {"scope": self.scope, "workflow_id": self.workflow_id, "node_id": 7, "run_id": self.run_id},
            "history": self.history, "data_node_id": 3, "evaluation_node_id": 4,
            "barrier": self.subscriptions(), "page_size": 16, "max_steps": 1000,
            "warmup_from": (datetime.fromisoformat(self.history["from"]) - timedelta(minutes=5)).isoformat(),
            "warmup_limit": 1000,
        })
        return argument

    def services(self):
        query = super().datafeed("binance", 3)
        query["datafeed_config"]["properties"].update(query_only="true", clock_mode="virtual")
        return [("wallet", "wallet", "wallet-manager-configuration", self.wallet()),
                ("order", "order", "order-manager-configuration", self.order()),
                ("strategy", "strategy", "strategy-runner-configuration", self.strategy()),
                ("binance", "datafeed", "datafeed-configuration", query),
                ("backtest", "backtest", "backtest-manager-configuration", self.backtest())]


class ReplayServices(ProductServices):
    def __init__(self, context, history):
        super().__init__(context)
        self.configuration = ReplayConfiguration(context, history)

    def seed(self):
        scope = self.context.config["scope"]
        with psycopg.connect(self.context.secret("database_url_env")) as connection:
            row = connection.execute("SELECT env,tenant_id::text,replay_of::text FROM wf_run WHERE run_id=%s",
                                     (self.configuration.run_id,)).fetchone()
            if row != (scope["env"], scope["tenant_id"], self.configuration.history["run_id"]):
                raise RuntimeError("historical replay run lacks its explicit source binding")
            self.upsert_service_bindings(connection)

    def before_start(self, name):
        if name == "backtest":
            FundingFixture.publish(self.context, self.configuration.run_id)


class ReplayFixture:
    @staticmethod
    def prepare(context, history=None, pass_number=1):
        scope = context.config["scope"]
        requested_run = os.environ.get("MARKETPULSE_REPLAY_SOURCE_RUN_ID")
        frozen = datetime.now(timezone.utc)
        with psycopg.connect(context.secret("database_url_env")) as connection:
            if history is None:
                source = connection.execute(
                    "SELECT run_id::text,workflow_id::text FROM wf_run r WHERE env=%s AND tenant_id=%s "
                    "AND (%s::text IS NULL OR run_id=%s) AND EXISTS(SELECT 1 FROM ord_order o WHERE o.run_id=r.run_id AND state='filled') "
                    "ORDER BY started_at DESC LIMIT 1", (scope["env"], scope["tenant_id"], requested_run, requested_run)).fetchone()
                if not source:
                    raise RuntimeError("replay requires a previously completed actual product pipeline")
                begin = connection.execute("SELECT MIN(available_at) FROM mkt_event WHERE env=%s AND tenant_id=%s AND run_id=%s",
                                           (scope["env"], scope["tenant_id"], source[0])).fetchone()[0]
                last = connection.execute("SELECT MAX(updated_at) FROM ord_order WHERE run_id=%s AND state='filled'", (source[0],)).fetchone()[0]
                if begin is None or last is None:
                    raise RuntimeError("historical source lacks persisted market facts and completed executions")
                until = min(frozen, last + timedelta(seconds=5))
                if until - begin > timedelta(hours=1):
                    raise RuntimeError("case replay source exceeds the bounded one-hour history interval")
                history = {"scope": scope, "run_id": source[0], "workflow_id": source[1],
                           "sources": ["twitter", "truthsocial", "binance"], "from": begin.isoformat(),
                           "until": until.isoformat(), "frozen_at": frozen.isoformat()}
            if history["scope"] != scope:
                raise RuntimeError("fixed replay history belongs to a different source scope")
            target_scope = {**scope, "env": f"replay-marketpulse-{context.run_id}-{context.round_number}-{pass_number}"}
            workflow = UuidV7.generate()
            workflow = connection.execute(
                "INSERT INTO wf_workflow(id,env,tenant_id,name,status,flow_json,created_by,updated_by) "
                "SELECT %s,%s,tenant_id,%s,'RUNNING',flow_json,'marketpulse-e2e','marketpulse-e2e' "
                "FROM wf_workflow WHERE id=%s RETURNING id::text",
                (workflow, target_scope["env"], "Marketpulse frozen replay", history["workflow_id"])).fetchone()[0]
            wallet = UuidV7.generate()
            account = UuidV7.generate()
            connection.execute("INSERT INTO wlt_wallet(id,tenant_id,account_id,exchange,mode,env) VALUES(%s,%s,%s,'BINANCE','backtest',%s)",
                               (wallet, scope["tenant_id"], account, target_scope["env"]))
            run_id = UuidV7.generate()
            connection.execute("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state,replay_of) VALUES(%s,%s,%s,%s,'running',%s)",
                               (run_id, workflow, target_scope["env"], scope["tenant_id"], history["run_id"]))
        config = {**context.config, "scope": target_scope, "workflow_id": workflow,
                  "account_id": account, "wallet_id": wallet}
        target = replace(context, config=config, run_id=run_id,
                         report_root=context.report_root / "replay" / f"round-{context.round_number}" / f"pass-{pass_number}")
        return target, history

    @staticmethod
    def activate(context):
        BrokerCredentials.initialize(context)
        os.environ[context.config["workflow"]["run_id_env"]] = context.run_id
