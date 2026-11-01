"""Product strategy for a real multi-account compensation scenario."""
from datetime import datetime, timedelta
from sigbot_sdk import Effects


class GroupStrategy:
    def on_hyperparameters(self, ctx, update):
        return Effects()

    def on_data(self, ctx, event):
        if event.data.kind != "market" or ctx.state["user"].get("submitted"):
            return Effects()
        ctx.state["user"]["submitted"] = True
        deadline = (datetime.fromisoformat(ctx.clock.now.replace("Z", "+00:00")) + timedelta(seconds=30)).isoformat()
        intents = []
        for index, account_id in enumerate(ctx.configuration.accounts):
            intents.append({"account_id": account_id, "wallet_id": ctx.configuration.wallets[index], "exchange": "BINANCE", "symbol": "BTCUSDT",
                "side": "BUY", "quantity": "0.001", "order_type": "MARKET", "limit_price": None, "reduce_only": False,
                "market": {"source": event.source, "event_time": event.event_time, "available_time": event.available_time,
                           "price": str(event.data.value.price)},
                "execution_group": {"group_seq": 0, "coordinator_node_id": 6, "leg_index": index, "leg_count": 2,
                                    "deadline": deadline, "failure_policy": "compensate_filled"}})
        print("marketpulse: two-account group submitted to the real order coordinators")
        return Effects(intents=intents)
