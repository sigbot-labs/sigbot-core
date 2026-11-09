from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-exporter", "export_delivery", [
        "real_kafka_ack_restart_dedup_out_of_order_cursor_and_scope",
    ], {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env"),
        "MARKETPULSE_KAFKA_BROKERS": context.endpoint("kafka")})
