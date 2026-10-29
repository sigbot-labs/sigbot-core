"""Real product configuration for same-venue multi-account compensation."""
import json
from dataclasses import replace

import psycopg

from common.funding import FundingFixture
from common.runtime import RuntimeConfiguration
from common.services import ProductServices


class GroupConfiguration(RuntimeConfiguration):
    def __init__(self, context, accounts):
        super().__init__(context)
        self.run_id = context.run_id
        self.accounts = accounts
        self.management_port = 19610

    def wallet_node(self, index):
        node, order = 8 + 10 * index, 6 + 10 * index
        properties = self.base(node)
        properties["runtime_config"] = json.dumps({"scope": self.scope, "account_id": self.accounts[index],
                                                  "node_id": node, "order_node_ids": [order]})
        return {"messager_config": self.messager(f"wallet-{index}"), "properties": properties, "secrets": None}

    def order_node(self, index):
        argument = super().order()
        config = json.loads(argument["properties"]["runtime_config"])
        config.update(node_id=6 + 10 * index, wallet_node_id=8 + 10 * index, account_id=self.accounts[index],
                      execution_group_coordinator_ids=[6])
        config["exchange"]["endpoint"] = "http://127.0.0.1:18486"
        argument["properties"] = {**self.base(config["node_id"]), "runtime_config": json.dumps(config)}
        argument["messager_config"] = self.messager(f"order-{index}")
        return argument

    def strategy(self):
        argument = super().strategy()
        config = json.loads(argument["properties"]["runtime_config"])
        config.update(code=(self.context.root / "common/strategy/execution_group.py").read_text(), class_name="GroupStrategy",
                      configuration={"accounts": self.accounts},
                      edges=[{"source_node_id": 3, "target_node_id": 5, "kind": "feed_market"},
                             {"source_node_id": 5, "target_node_id": 6, "kind": "strategy_signal"},
                             {"source_node_id": 5, "target_node_id": 16, "kind": "strategy_signal"}])
        argument["properties"]["runtime_config"] = json.dumps(config)
        return argument

    def backtest(self):
        argument = super().backtest()
        argument["properties"]["bind_addr"] = "127.0.0.1:18486"
        return argument

    def services(self):
        return [("backtest", "backtest", "backtest-manager-configuration", self.backtest()),
                ("wallet-first", "wallet", "wallet-manager-configuration", self.wallet_node(0)),
                ("wallet-second", "wallet", "wallet-manager-configuration", self.wallet_node(1)),
                ("order-first", "order", "order-manager-configuration", self.order_node(0)),
                ("order-second", "order", "order-manager-configuration", self.order_node(1)),
                ("strategy", "strategy", "strategy-runner-configuration", self.strategy()),
                ("binance", "datafeed", "datafeed-configuration", self.datafeed("binance", 3))]


class GroupServices(ProductServices):
    def __init__(self, context, accounts):
        super().__init__(context)
        self.configuration = GroupConfiguration(context, accounts)

    def seed(self):
        with psycopg.connect(self.context.secret("database_url_env")) as connection:
            expected = (self.context.config["scope"]["env"], self.configuration.workflow_id)
            actual = connection.execute("SELECT env,workflow_id FROM workflow_run WHERE run_id=%s",
                                        (self.context.run_id,)).fetchone()
            if actual != expected:
                raise RuntimeError("multi-account runtime does not own its Workflow run")

    def before_start(self, name):
        if name == "binance":
            # Only account one receives external capital. Wallet two must reject
            # its actual reservation through the ordinary service transaction.
            FundingFixture.publish(self.context, self.context.run_id)


class GroupFixture:
    @staticmethod
    def prepare(context):
        scope = {**context.config["scope"], "env": f"group-marketpulse-{context.run_id}-{context.round_number}"}
        run_id = f"{context.run_id}-group-{context.round_number}"
        with psycopg.connect(context.secret("database_url_env")) as connection:
            workflow = connection.execute("INSERT INTO s_workflow(env,tenant_id,space_id,name,status) VALUES(%s,%s,%s,'Marketpulse multi-account compensation','RUNNING') RETURNING id",
                                          (scope["env"], scope["tenant_id"], scope["space_id"])).fetchone()[0]
            accounts = []
            for _ in range(2):
                wallet = connection.execute("SELECT nextval(pg_get_serial_sequence('s_wallets','id'))").fetchone()[0]
                connection.execute("INSERT INTO s_wallets(id,tenant_id,account_id,exchange,mode,env,space_id) VALUES(%s,%s,%s,'BINANCE','backtest',%s,%s)",
                                   (wallet, str(scope["tenant_id"]), wallet, scope["env"], scope["space_id"]))
                accounts.append(wallet)
            connection.execute("INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state) VALUES(%s,%s,%s,%s,%s,'running')",
                               (run_id, workflow, scope["env"], scope["tenant_id"], scope["space_id"]))
        target = replace(context, config={**context.config, "scope": scope, "workflow_id": workflow, "account_id": accounts[0]},
                         run_id=run_id, report_root=context.report_root / "groups" / f"round-{context.round_number}")
        return target, accounts
