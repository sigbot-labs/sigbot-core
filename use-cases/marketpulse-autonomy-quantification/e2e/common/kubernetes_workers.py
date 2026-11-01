"""Provision isolated fixture infrastructure for the real Rust worker controller test."""
import json
import os
import re
import shlex
from pathlib import Path
from datetime import datetime, timedelta, timezone

import psycopg
import yaml

from common.carrier import WorkerCarrier
from common.containers import LocalContainerEngine
from common.context import UuidV7
from common.runtime import RuntimeConfiguration
from deploy.deployer.kubernetes import KubernetesController


class KubernetesWorkers(KubernetesController):
    namespace = "sigbot-marketpulse-workers"
    owner = "sigbot-marketpulse-worker-verification"

    def kubectl(self):
        return ["sudo", "-n", "kubectl", "--kubeconfig", "/etc/rancher/k3s/k3s.yaml"]

    def apply(self, resource):
        if resource["kind"] == "Secret":
            metadata = resource["metadata"]
            if metadata.get("namespace") != self.namespace:
                raise ValueError("fixture Secrets must remain in the owned worker namespace")
            version = self.command(self.kubectl() + ["-n", self.namespace, "get", "secret", metadata["name"],
                "--ignore-not-found", "-o", "jsonpath={.metadata.resourceVersion}"]).strip()
            if version:
                metadata["resourceVersion"] = version
            # stringData is write-only: apply can retain old data keys across a rename.
            # Replace the complete owned Secret, with resourceVersion fencing.
            self.command(self.kubectl() + ["replace" if version else "create", "-f", "-"],
                         input_text=json.dumps(resource))
            template = 'go-template={{range $key, $value := .data}}{{$key}}{{"\\n"}}{{end}}'
            keys = self.command(self.kubectl() + ["-n", self.namespace, "get", "secret", metadata["name"],
                                                "-o", template]).splitlines()
            expected = set(resource.get("data", {})) | set(resource.get("stringData", {}))
            if set(keys) != expected:
                raise RuntimeError("owned fixture Secret retained unexpected keys after replacement")
            self.runtime_secret_key_names = sorted(keys)
            return
        self.command(self.kubectl() + ["apply", "-f", "-"], input_text=json.dumps(resource))

    def middleware_ip(self, service):
        engine = LocalContainerEngine()
        prefix = engine.arguments()
        identifiers = self.command(prefix + ["ps", "--filter", "label=com.docker.compose.project=sigbot-marketpulse",
            "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.ID}}"]).splitlines()
        if len(identifiers) != 1:
            raise RuntimeError(f"expected one running {service} container owned by the marketpulse case")
        address = self.command(prefix + ["inspect", identifiers[0], "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"]).strip()
        if not address:
            raise RuntimeError(f"marketpulse {service} container has no routable address")
        return address

    @staticmethod
    def private_json(path, document):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(document, stream)
        os.chmod(path, 0o600)

    @staticmethod
    def expand(value):
        if isinstance(value, dict):
            return {key: KubernetesWorkers.expand(item) for key, item in value.items()}
        if isinstance(value, list):
            return [KubernetesWorkers.expand(item) for item in value]
        if isinstance(value, str):
            return re.sub(r"\$\{([^}:]+):([^}]*)\}", lambda match: os.environ.get(match[1], match[2]), value)
        return value

    def snapshot(self):
        messager = RuntimeConfiguration(self.context).messager("kubernetes")
        messager["properties"].update(mqtt_server=self.broker_ip, mqtt_port="1883")
        scope = self.context.config["scope"]
        feed = {"messager_config": messager, "datafeed_config": {"name": "kubernetes-twitter", "provider": "TWITTER",
                "properties": {"endpoint": f"http://mock-twitter.{self.namespace}.svc.cluster.local:8080",
                               "transport": "rest", "account_id": "2244994945", "poll_interval_ms": "2000",
                               "token_secret": "marketpulse_api_token"}}}
        runtime = {"identity": {"scope": scope, "workflow_id": self.context.config["workflow_id"],
                                "node_id": 2, "run_id": UuidV7.validate(self.context.run_id)},
                   "account_id": self.context.config["account_id"], "class_name": "DeploymentStrategy", "edges": [],
                   "configuration": {"deployment_revision": 1},
                   "code": (self.context.root / "common/strategy/deployment.py").read_text()}
        strategy = {"messager_config": messager, "run_mode": "V2", "sys_environment": {},
                    "properties": {"runtime_config": json.dumps(runtime)}}
        nodes = [{"id": identifier, "name": module, "stage": stage, "position": {"x": identifier, "y": 0},
                  "config": {"module": module, "argument_json": argument}}
                 for identifier, module, stage, argument in [(1, "datafeed", "TWITTER@INPUT", feed),
                                                              (2, "strategy", "PYCODE@EVALUATION", strategy)]]
        return {"execution_mode": "paper",
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "flow_json": {"nodes": nodes, "connections": [{"id": "news", "source": "1", "target": "2"}]}}

    def deploy(self):
        image = WorkerCarrier(self.context).build_and_import()
        current = self.command(self.kubectl() + ["get", "namespace", self.namespace, "--ignore-not-found", "-o", "json"])
        if current and json.loads(current)["metadata"].get("labels", {}).get("app.kubernetes.io/part-of") != self.owner:
            raise RuntimeError("worker namespace exists outside this case")
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace,
                    "labels": {"app.kubernetes.io/part-of": self.owner}}})
        self.apply({"apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": "sigbot-worker", "namespace": self.namespace},
                    "automountServiceAccountToken": False})
        self.database_ip = self.middleware_ip("postgres")
        self.broker_ip = self.middleware_ip("emqx")
        host_database = psycopg.conninfo.conninfo_to_dict(self.context.secret("database_url_env"))
        mqtt = self.context.config["mqtt"]
        secrets = {self.context.config["token_env"]: self.context.secret("token_env"),
                   "SIGBOT__SECRETS__VALUES__MARKETPULSE_API_TOKEN": self.context.secret("token_env"),
                   "SIGBOT__SECRETS__VALUES__MQTT_PASSWORD": os.environ[mqtt["password_env"]],
                   "SIGBOT__STORAGE__POSTGRES__PASSWORD": host_database["password"]}
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "sigbot-runtime", "namespace": self.namespace},
                    "type": "Opaque", "stringData": secrets})
        public = {"SIGBOT__STORAGE__PROVIDER": "Postgres", "SIGBOT__STORAGE__POSTGRES__HOST": self.database_ip,
                  "SIGBOT__STORAGE__POSTGRES__PORT": "5432", "SIGBOT__STORAGE__POSTGRES__DATABASE": host_database["dbname"],
                  "SIGBOT__STORAGE__POSTGRES__USERNAME": host_database["user"], "SIGBOT_CFG_VERBOSE": "false",
                  "SIGBOT__MGMT__OTEL__ENABLED": "false", "SIGBOT__MGMT__TOKIO_CONSOLE__ENABLED": "false",
                  "SIGBOT__LOGGING__LEVEL": "INFO"}
        self.apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "sigbot-runtime", "namespace": self.namespace}, "data": public})
        self.boundary(image["image"])
        connectivity = self.connectivity(image["image"])
        service = json.loads(self.command(self.kubectl() + ["-n", self.namespace, "get", "service", "mock-twitter", "-o", "json"]))
        pods = json.loads(self.command(self.kubectl() + ["-n", self.namespace, "get", "pods", "-l", "app=marketpulse-twitter-boundary", "-o", "json"]))
        destinations = {self.database_ip, self.broker_ip, service["spec"]["clusterIP"]}
        destinations.update(pod["status"]["podIP"] for pod in pods["items"]
                            if not pod["metadata"].get("deletionTimestamp") and pod["status"].get("podIP"))
        config = self.expand(yaml.safe_load((self.context.root.parents[2] / "etc/sigbot.yaml").read_text()))
        config["storage"]["provider"] = "Postgres"
        config["storage"]["postgres"].update(host=host_database["host"], port=int(host_database["port"]),
                                           database=host_database["dbname"], username=host_database["user"], password=host_database["password"])
        config["mgmt"]["otel"]["enabled"] = False
        config["mgmt"]["tokio_console"]["enabled"] = False
        scope = self.context.config["scope"]
        config["authguard"].update(service_tenant_id=scope["tenant_id"], service_env=scope["env"],
                                  service_principal_id="worker-acceptance-controller")
        workers = {"enabled": True, "cpu_millis": 500, "memory_mb": 384,
                   "pids_limit": 64, "reconciliation_timeout_seconds": 120}
        config["services"]["controller"]["workers"] = workers
        config["services"]["controller"]["images"]["sigbot"] = image["image"]
        directory = Path("/tmp/sigbot-marketpulse-workers")
        directory.mkdir(mode=0o700, exist_ok=True)
        self.private_json(directory / "config.json", config)
        self.private_json(directory / "snapshot.json", self.snapshot())
        variables = {"MARKETPULSE_WORKER_IMAGE": image["image"], "MARKETPULSE_WORKER_NAMESPACE": self.namespace,
                     "MARKETPULSE_WORKER_CONFIG_PATH": str(directory / "config.json"),
                     "MARKETPULSE_WORKER_SNAPSHOT_PATH": str(directory / "snapshot.json"),
                     "MARKETPULSE_WORKER_EGRESS_CIDRS": json.dumps(
                         [address + "/32" for address in sorted(destinations)]),
                     "KUBECONFIG": "/etc/rancher/k3s/k3s.yaml"}
        descriptor = os.open(directory / "test.env", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write("".join(f"export {key}={shlex.quote(value)}\n" for key, value in variables.items()))
        os.environ.update(variables)
        return {"namespace": self.namespace, "image": image, "connectivity": connectivity,
                "runtime_secret_key_names": self.runtime_secret_key_names,
                "test_environment": str(directory / "test.env"),
                "scope": "preparation only; actual product lifecycle acceptance is rust-deployment"}

    def boundary(self, image):
        labels = {"app": "marketpulse-twitter-boundary"}
        self.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "mock-twitter", "namespace": self.namespace},
                    "spec": {"selector": labels, "ports": [{"port": 8080, "targetPort": 8080}]}})
        self.apply({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "mock-twitter", "namespace": self.namespace},
            "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {"metadata": {"labels": labels},
                "spec": {"automountServiceAccountToken": False, "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True},
                    "containers": [{"name": "boundary", "image": image, "imagePullPolicy": "Never",
                        "command": ["/usr/bin/python3.12", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"],
                        "env": [{"name": self.context.config["token_env"], "valueFrom": {"secretKeyRef": {"name": "sigbot-runtime", "key": self.context.config["token_env"]}}},
                                {"name": "MARKETPULSE_FIXTURE_BASE_TIME", "value": (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()}],
                        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "300m", "memory": "256Mi"}},
                        "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "initialDelaySeconds": 2, "periodSeconds": 3}}]}}}})
        self.command(self.kubectl() + ["-n", self.namespace, "rollout", "status", "deployment/mock-twitter", "--timeout=90s"])

    def connectivity(self, image):
        name = "middleware-connectivity-" + self.context.run_id
        targets = [(self.database_ip, 5432), (self.broker_ip, 1883)]
        script = "import socket,json; targets=" + repr(targets) + "; [socket.create_connection(target,5).close() for target in targets]; print(json.dumps({'connected':targets}))"
        self.apply({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": self.namespace},
            "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 30, "automountServiceAccountToken": False,
                "containers": [{"name": "probe", "image": image, "imagePullPolicy": "Never", "command": ["/usr/bin/python3.12", "-c", script],
                    "resources": {"limits": {"cpu": "100m", "memory": "64Mi"}}}]}})
        try:
            self.command(self.kubectl() + ["-n", self.namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/" + name, "--timeout=40s"])
            result = self.command(self.kubectl() + ["-n", self.namespace, "logs", name])
            self.context.evidence("pod-connectivity.json", result)
            return json.loads(result)
        finally:
            self.command(self.kubectl() + ["-n", self.namespace, "delete", "pod", name, "--wait=false", "--ignore-not-found"])

    def cleanup(self):
        resource = self.command(self.kubectl() + ["get", "namespace", self.namespace, "--ignore-not-found", "-o", "json"])
        if resource:
            if json.loads(resource)["metadata"].get("labels", {}).get("app.kubernetes.io/part-of") != self.owner:
                raise RuntimeError("refusing to remove a worker namespace outside this case")
            self.command(self.kubectl() + ["delete", "namespace", self.namespace, "--wait=true",
                                          f"--timeout={max(1, int(self.context.timeout_seconds) - 5)}s"])
        for name in ("config.json", "snapshot.json", "test.env"):
            (Path("/tmp/sigbot-marketpulse-workers") / name).unlink(missing_ok=True)
