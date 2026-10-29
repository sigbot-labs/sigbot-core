from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-exchange", "public_market", [
        "two_real_public_venues_return_timestamped_native_trade_facts",
    ], {})
