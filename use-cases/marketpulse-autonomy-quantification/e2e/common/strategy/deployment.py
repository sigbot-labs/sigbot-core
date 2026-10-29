"""Actual isolated strategy used only to observe Kubernetes worker lifecycle."""
from sigbot_sdk import Effects


class DeploymentStrategy:
    def on_hyperparameters(self, ctx, update):
        return Effects()

    def on_data(self, ctx, event):
        user = ctx.state["user"]
        user["callbacks"] = user.get("callbacks", 0) + 1
        return Effects()
