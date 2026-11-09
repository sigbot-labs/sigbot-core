import json

from common.runtime import RuntimeConfiguration
from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-messager", "postgres_replay", [
        "real_broker_replay_preserves_identity_and_quarantine_ack_is_durable",
    ], {"SIGBOT_TEST_DATABASE_URL": context.secret("database_url_env"),
        "SIGBOT_TEST_MESSAGE_SCOPE": json.dumps(context.config["scope"]),
        "SIGBOT_TEST_MQTT_CONFIG": json.dumps(RuntimeConfiguration(context).messager("replay-contract"))})
