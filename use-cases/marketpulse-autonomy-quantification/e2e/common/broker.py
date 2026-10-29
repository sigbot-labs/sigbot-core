"""Issue local broker test credentials; these never authenticate the product UI/API."""
import os
import time

import jwt


class BrokerCredentials:
    @staticmethod
    def initialize(context):
        settings = context.config["mqtt"]
        key = os.environ.get(settings["signing_key_env"])
        if not key:
            return  # An externally managed broker may supply its own credential.
        if len(key) < 32:
            raise ValueError("isolated MQTT signing key must have at least 32 characters")
        scope = context.config["scope"]
        username = os.environ.get(settings["username_env"], "marketpulse")
        now = int(time.time())
        token = jwt.encode({"username": username, "iat": now, "exp": now + 14400,
                            "acl": [{"permission": "allow", "action": "all",
                                     "topic": f"internal/v2/{scope['env']}/{scope['tenant_id']}/{scope['space_id']}/#"},
                                    {"permission": "deny", "action": "all", "topic": "#"}]}, key, algorithm="HS256")
        os.environ[settings["username_env"]] = username
        os.environ[settings["password_env"]] = token
