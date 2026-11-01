import os
import threading

import paho.mqtt.client as mqtt

from common.broker import DurableBrokerProbe
from deploy.deployer.docker import DockerController


class BrokerProbe:
    def __init__(self, context, suffix, password):
        self.context = context
        self.connected = threading.Event()
        self.completed = threading.Event()
        self.accepted = False
        self.reason = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"acl-{context.run_id}-{suffix}", protocol=mqtt.MQTTv5)
        self.client.username_pw_set(os.environ.get(context.config["mqtt"]["username_env"], "marketpulse"), password)
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, reason, properties):
        self.accepted = not reason.is_failure
        self.connected.set()

    def on_subscribe(self, client, userdata, mid, reasons, properties):
        self.reason = all(not reason.is_failure for reason in reasons)
        self.completed.set()

    def on_publish(self, client, userdata, mid, reason, properties):
        self.reason = not reason.is_failure
        self.completed.set()

    def connect(self):
        settings = self.context.config["mqtt"]
        self.client.connect(settings["host"], settings["port"], keepalive=30)
        self.client.loop_start()
        if not self.connected.wait(10):
            raise AssertionError("MQTT AuthN probe did not receive CONNACK")
        return self.accepted

    def operation(self, action, topic):
        if not self.connect():
            raise AssertionError("valid isolated credential was rejected")
        if action == "subscribe":
            self.client.subscribe(topic, qos=1)
        else:
            self.client.publish(topic, b'{"external_acl_probe":true}', qos=1)
        if not self.completed.wait(10):
            raise AssertionError(f"{action} ACL probe did not receive protocol acknowledgment")
        return self.reason

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


def verify(context):
    settings = context.config["mqtt"]
    password = os.environ.get(settings["password_env"])
    if not password:
        raise RuntimeError("ACL acceptance requires a real broker credential; anonymous configuration cannot pass")
    rejected = BrokerProbe(context, "invalid", "invalid-test-token")
    try:
        assert not rejected.connect(), "broker accepted an invalid credential"
    finally:
        rejected.close()
    scope = context.config["scope"]
    valid = f"internal/v2/{scope['env']}/{scope['tenant_id']}/probe/acl"
    forbidden = [f"internal/v2/{scope['env']}/0192f7b2-5c00-7000-8000-000000000099/probe/acl",
                 f"internal/v2/foreign/{scope['tenant_id']}/probe/acl"]
    for index, topic in enumerate([valid] + forbidden):
        for action in ("subscribe", "publish"):
            probe = BrokerProbe(context, f"{index}-{action}", password)
            try:
                allowed = probe.operation(action, topic)
                assert allowed == (index == 0), f"broker {action} did not enforce env/tenant isolation"
            finally:
                probe.close()
    durable_topic = f"internal/v2/{scope['env']}/{scope['tenant_id']}/probe/durable"
    durable = DurableBrokerProbe(context, durable_topic, password)
    try:
        durable.establish()
        publisher = BrokerProbe(context, "durable-publisher", password)
        try:
            if not publisher.connect():
                raise AssertionError("durable probe publisher was rejected")
            publisher.client.publish(durable_topic, DurableBrokerProbe.payload, qos=1)
            if not publisher.completed.wait(15) or not publisher.reason:
                raise AssertionError("durable probe publisher did not receive PUBACK")
        finally:
            publisher.close()
        DockerController(context).restart("emqx")
        durable.resume()
    finally:
        durable.close()
    return {"invalid_credentials_rejected": True, "own_scope_permitted": True,
            "foreign_env_tenant_publish_and_subscribe_denied": True, "broker": "actual EMQX MQTT 5 acknowledgments",
            "durable_qos1_session_survived_broker_restart": True,
            "scope": "local JWT broker identity; independent from Authguard UI/API authentication"}
