"""Strategy executed only by the product's isolated Rust/Python worker."""
from sigbot_sdk import Effects
from datetime import datetime, timedelta


class MarketpulseStrategy:
    def on_hyperparameters(self, ctx, update):
        ctx.state["position_controls"]["max_position_ratio"] = update.parameters.get("max_position_ratio", "0.20")
        ctx.state["user"]["evaluated"] = True
        if not ctx.state["user"].get("snapshot_ready"):
            ctx.market.snapshot({"sources": ["binance"], "symbols": ["BTCUSDT"], "mode": {"kind": "LATEST"},
                                 "max_age_ms": 300000, "max_skew_ms": 300000, "max_rows": 1})
        return Effects()

    def on_data(self, ctx, event):
        user = ctx.state["user"]
        if event.data.kind == "snapshot":
            user["snapshot_ready"] = True
            end = datetime.fromisoformat(ctx.clock.now.replace("Z", "+00:00"))
            ctx.compute.submit({"history_from": (end - timedelta(minutes=5)).isoformat(), "as_of": end.isoformat(),
                                "sources": ["binance"], "symbols": ["BTCUSDT"], "window_ms": 60000,
                                "group_by": ["symbol"], "aggregations": ["count", "mean", "first", "last"],
                                "predict": {"model_version": "mean-price-v1", "partitioning": "independent_symbols"},
                                "max_rows": 10, "max_bytes": 8192})
            return Effects()
        if event.data.kind == "compute":
            user["compute_ready"] = True
            return Effects()
        if event.data.kind == "news":
            marker = ctx.configuration.get("acceptance_run_id", ctx.identity.run_id)
            if f"marketpulse acceptance observation {marker}:" in event.data.value.text.lower():
                user["news_seen"] = True
                user["traded"] = False
            return Effects()
        if event.data.kind != "market" or not user.get("evaluated") or not user.get("compute_ready") or not user.get("news_seen") or user.get("traded"):
            return Effects()
        user["traded"] = True
        print("marketpulse: evaluated source facts produced a bounded trading intent")
        intent = {
            "account_id": ctx.portfolio.account_id, "wallet_id": ctx.portfolio.wallet_id,
            "exchange": "BINANCE", "symbol": event.symbol, "side": "BUY", "quantity": "0.001",
            "order_type": "MARKET", "limit_price": None, "reduce_only": False,
            "market": {"source": event.source, "event_time": event.event_time,
                       "available_time": event.available_time, "price": str(event.data.value.price)},
        }
        return Effects(intents=[intent])
