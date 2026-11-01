from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-backtest", "resting_orders", [
        "limit_waits_for_a_later_crossing_tick_and_restart_cannot_duplicate_its_fill",
        "late_historical_ticks_cannot_fill_an_order_accepted_after_their_event_time",
    ], {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")})
