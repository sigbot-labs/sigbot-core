"""Product service configuration, without embedding secrets or business implementations."""
from __future__ import annotations

import json
import os
from common.context import UuidV7


class RuntimeConfiguration:
    def __init__(self, context):
        self.context = context
        self.scope = context.config["scope"]
        run_id_env = context.config["workflow"]["run_id_env"]
        self.run_id = UuidV7.validate(os.environ.get(run_id_env, UuidV7.generate()))
        os.environ[run_id_env] = self.run_id
        self.workflow_id = context.config["workflow_id"]
        self.account_id = context.config["account_id"]
        self.wallet_id = context.config["wallet_id"]
        self.mode = "paper"
        self.management_port = 19490

    def messager(self, name):
        mqtt = self.context.config["mqtt"]
        return {"properties": {"mqtt_server": mqtt["host"], "mqtt_port": str(mqtt["port"]),
                "mqtt_username": os.environ.get(mqtt["username_env"], "marketpulse"),
                "mqtt_client_id": f"marketpulse-{name}-{self.scope['tenant_id']}-{self.workflow_id}-{self.run_id}",
                "mqtt_timeout": "30", "mqtt_qos": "1", "mqtt_clean_start": "false"},
                "secrets": {"mqtt_password_secret": "mqtt_password"}}

    def base(self, node_id):
        return {"execution_mode": self.mode, "env": self.scope["env"], "tenant_id": str(self.scope["tenant_id"]),
                "workflow_id": str(self.workflow_id), "node_id": str(node_id),
                "run_id": self.run_id}

    def datafeed(self, provider, node_id, transport="rest"):
        properties = self.base(node_id)
        properties.update(traceparent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        if provider == "binance":
            properties.update(symbol="BTCUSDT", poll_interval_ms="5000", binance_endpoint="https://api.binance.com",
                              snapshot_input_node_ids="5", compute_input_node_ids="5")
        else:
            properties.update(endpoint=self.context.endpoint(provider), transport=transport,
                              account_id="2244994945" if provider == "twitter" else "100000000000001",
                              token_secret="marketpulse_api_token", poll_interval_ms="2000")
        return {"messager_config": self.messager(provider), "datafeed_config": {
            "name": provider, "provider": provider.upper(), "properties": properties, "secrets": None, "description": "E2E external source"}}

    def evaluator(self):
        properties = self.base(4)
        properties.update(input_node_ids="1,2,3", llm_endpoint=self.context.endpoint("openai") + "/v1",
                          llm_token_secret="marketpulse_api_token", llm_model="marketpulse-fixture")
        return {"messager_config": self.messager("evaluator"), "properties": properties, "secrets": None}

    def strategy(self):
        edges = [{"source_node_id": node, "target_node_id": 5, "kind": kind}
                 for node, kind in [(1,"feed_news"),(2,"feed_news"),(3,"feed_market"),(4,"evaluation_snapshot")]]
        edges.append({"source_node_id": 5, "target_node_id": 6, "kind": "strategy_signal"})
        edges.extend([{"source_node_id": 5, "target_node_id": 3, "kind": "feed_snapshot_request"},
                      {"source_node_id": 3, "target_node_id": 5, "kind": "feed_snapshot_result"},
                      {"source_node_id": 5, "target_node_id": 3, "kind": "compute_request"},
                      {"source_node_id": 3, "target_node_id": 5, "kind": "compute_result"}])
        config = {"identity": {"scope": self.scope, "workflow_id": self.workflow_id, "node_id": 5, "run_id": self.run_id},
                  "account_id": self.account_id, "code": (self.context.root / "common/strategy/marketpulse.py").read_text(),
                  "class_name": "MarketpulseStrategy", "configuration": {}, "edges": edges}
        archive = self.context.config["log_archive"]
        config["log_archive"] = {"endpoint": archive["endpoint"], "bucket": archive["bucket"],
                                 "region": archive["region"], "access_key_secret": "minio_access_key",
                                 "secret_key_secret": "minio_secret_key", "allow_http": True,
                                 "max_buffer_bytes": 16_777_216,
                                 "spool_path": str(self.context.report_root / "runtime/strategy-logs.sqlite")}
        properties = self.base(5)
        properties["runtime_config"] = json.dumps(config)
        return {"messager_config": self.messager("strategy"), "run_mode": "V2", "sys_environment": {}, "properties": properties}

    def order(self):
        config = {"scope": self.scope, "workflow_id": self.workflow_id, "node_id": 6, "input_node_ids": [5],
            "execution_node_ids": [7],
            "wallet_node_id": 8,
            "account_id": self.account_id,
            "instrument": self.instrument(),
            "risk_limits": {"max_order_notional": "1000", "max_account_notional": "10000", "max_position_quantity": "1",
                "max_market_age_ms": 300000, "fee_buffer_bps": "10"},
            "exchange": {"mode": self.mode, "endpoint": "http://127.0.0.1:18484",
                "token_secret": "marketpulse_api_token", "timeout_seconds": 10}}
        properties = self.base(6)
        properties.update(runtime_config=json.dumps(config), refresh_interval_ms="250")
        return {"messager_config": self.messager("order"), "properties": properties, "secrets": None}

    def backtest(self):
        properties = self.base(7)
        properties.update(bind_addr="127.0.0.1:18484", matching_config=json.dumps({"scope": self.scope, "node_id": 7,
            "fee_bps": "5", "slippage_bps": "0", "instrument": self.instrument(),
            "token_secret": "marketpulse_api_token"}))
        return {"messager_config": self.messager("backtest"), "properties": properties, "secrets": None}

    @staticmethod
    def instrument():
        return {"exchange": "BINANCE", "symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT",
                "quantity_step": "0.00001", "price_tick": "0.01", "min_notional": "5"}

    def wallet(self):
        properties = self.base(8)
        properties.update(runtime_config=json.dumps({"scope": self.scope, "account_id": self.account_id, "node_id": 8,
                          "order_node_ids": [6]}), refresh_interval_ms="250")
        return {"messager_config": self.messager("wallet"), "properties": properties, "secrets": None}

    def services(self):
        # Consumers are started before producers so no first event is lost.
        return [("backtest", "backtest", "backtest-manager-configuration", self.backtest()),
                ("wallet", "wallet", "wallet-manager-configuration", self.wallet()),
                ("order", "order", "order-manager-configuration", self.order()),
                ("strategy", "strategy", "strategy-runner-configuration", self.strategy()),
                ("evaluator", "evaluator", "evaluator-runner-configuration", self.evaluator()),
                ("twitter", "datafeed", "datafeed-configuration", self.datafeed("twitter", 1)),
                ("truthsocial", "datafeed", "datafeed-configuration", self.datafeed("truthsocial", 2, "mcp")),
                ("binance", "datafeed", "datafeed-configuration", self.datafeed("binance", 3))]
