"""Issue local broker test credentials; these never authenticate the product UI/API."""
import os
import threading
import time

import jwt
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


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
                                     "topic": f"internal/v2/{scope['env']}/{scope['tenant_id']}/#"},
                                    {"permission": "deny", "action": "all", "topic": "#"}]}, key, algorithm="HS256")
        os.environ[settings["username_env"]] = username
        os.environ[settings["password_env"]] = token


class DurableBrokerProbe:
    payload = b'{"durable_session_probe":true}'

    def __init__(self, context, topic, password):
        self.context = context
        self.topic = topic
        self.password = password
        self.client_id = f"durable-{context.run_id}"
        self.connected = threading.Event()
        self.subscribed = threading.Event()
        self.received = threading.Event()
        self.session_present = False
        self.client = None

    def connection(self, clean_start):
        settings = self.context.config["mqtt"]
        self.connected.clear()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id, protocol=mqtt.MQTTv5)
        client.username_pw_set(os.environ.get(settings["username_env"], "marketpulse"), self.password)
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_message = self.on_message
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 7 * 24 * 60 * 60
        client.connect(settings["host"], settings["port"], keepalive=30,
                       clean_start=clean_start, properties=properties)
        client.loop_start()
        if not self.connected.wait(15):
            client.loop_stop()
            raise AssertionError("persistent MQTT session did not receive CONNACK")
        self.client = client

    def on_connect(self, client, userdata, flags, reason, properties):
        if not reason.is_failure:
            self.session_present = bool(flags.session_present)
        self.connected.set()

    def on_subscribe(self, client, userdata, mid, reasons, properties):
        if all(not reason.is_failure for reason in reasons):
            self.subscribed.set()

    def on_message(self, client, userdata, message):
        if message.topic == self.topic and message.payload == self.payload:
            self.received.set()

    def establish(self):
        self.connection(mqtt.MQTT_CLEAN_START_FIRST_ONLY)
        self.client.subscribe(self.topic, qos=1)
        if not self.subscribed.wait(15):
            raise AssertionError("persistent MQTT subscription did not receive SUBACK")
        self.close()

    def resume(self):
        self.connection(False)
        if not self.session_present:
            raise AssertionError("EMQX lost the persistent session across restart")
        if not self.received.wait(20):
            raise AssertionError("EMQX did not replay the offline QoS 1 message after restart")

    def close(self):
        if self.client is not None:
            self.client.disconnect()
            self.client.loop_stop()
            self.client = None
