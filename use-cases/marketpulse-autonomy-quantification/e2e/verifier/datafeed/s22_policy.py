from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-datafeed", "policy_news", [
        "official_federal_reserve_feed_is_read_only_and_dated",
        "official_treasury_feed_is_read_only_and_dated",
    ], {})
