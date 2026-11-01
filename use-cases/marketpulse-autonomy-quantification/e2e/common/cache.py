"""Disposable authenticated Redis single-node and cluster boundaries for product tests."""
from dataclasses import replace
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time

from common.rust import RustIntegration
from common.containers import LocalContainerEngine


class RedisFixture:
    tests = ["test_set_and_get", "test_keys", "test_set_nx", "test_hset_and_hget", "test_hget_all",
             "test_hkeys", "test_hdel", "test_expire", "test_bit_operations", "test_del", "unauthenticated_client_is_rejected"]

    def __init__(self, context, nodes):
        self.context = replace(context, scenario=f"rust-cache-{nodes}")
        self.nodes = nodes
        self.engine = LocalContainerEngine("MARKETPULSE_CACHE_COMMAND").arguments()
        self.password = secrets.token_urlsafe(32)
        self.owned = []
        self.deadline = time.monotonic() + context.timeout_seconds
        self.labels = {"io.sigbot.case": "marketpulse-autonomy-quantification", "io.sigbot.fixture": "redis",
                       "io.sigbot.run_id": context.run_id}
        self.log = self.context.evidence("fixture.log", "")

    def command(self, arguments, *, cleanup=False):
        result = subprocess.run(self.engine + arguments, text=True, capture_output=True,
                                timeout=30 if cleanup else max(1, self.deadline - time.monotonic()))
        with self.log.open("a") as stream:
            stream.write((result.stdout + result.stderr).replace(self.password, "[REDACTED]"))
        if result.returncode:
            raise RuntimeError(f"owned Redis fixture command failed; inspect {self.log}")
        return result.stdout.strip()

    def cleanup(self):
        for identifier in reversed(self.owned):
            labels = json.loads(self.command(["inspect", "--format", "{{json .Config.Labels}}", identifier], cleanup=True))
            if any(labels.get(key) != value for key, value in self.labels.items()):
                raise RuntimeError("refusing to remove a Redis container whose ownership changed")
            self.command(["rm", "--force", "--volumes", identifier], cleanup=True)
        self.owned.clear()

    def run(self):
        image = os.environ["REDIS_IMAGE"]
        ports = [16401 + i for i in range(self.nodes)]
        result = None
        with tempfile.TemporaryDirectory(prefix="sigbot-redis-") as directory:
            directory = Path(directory)
            credentials = directory / "client.env"
            credentials.write_text(f"REDISCLI_AUTH={self.password}\n")
            credentials.chmod(0o600)
            try:
                for index, port in enumerate(ports):
                    config = directory / f"redis-{index}.conf"
                    contents = [f"port {port}", "bind 127.0.0.1", f"requirepass {self.password}",
                                "save \"\"", "appendonly no", "maxmemory 64mb", "maxmemory-policy allkeys-lru"]
                    if self.nodes > 1:
                        contents += ["cluster-enabled yes", "cluster-config-file /tmp/nodes.conf",
                                     "cluster-announce-ip 127.0.0.1", "cluster-node-timeout 5000"]
                    config.write_text("\n".join(contents) + "\n")
                    config.chmod(0o644)  # Parent directory is private; the container's unprivileged UID must read the mount.
                    arguments = ["run", "--detach", "--name", f"sigbot-redis-{self.context.run_id}-{self.context.round_number}-{self.nodes}-{index}",
                                 "--network", "host", "--memory", "128m", "--cpus", "0.5", "--pids-limit", "64",
                                 "--user", "1000:1000", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                                 "--tmpfs", "/tmp:rw,size=16m", "--env-file", str(credentials),
                                 "--volume", f"{config}:/etc/redis/fixture.conf:ro", "--entrypoint", "redis-server"]
                    for key, value in self.labels.items():
                        arguments += ["--label", f"{key}={value}"]
                    self.owned.append(self.command(arguments + [image, "/etc/redis/fixture.conf"]))
                    while True:
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=1):
                                break
                        except OSError:
                            if time.monotonic() >= self.deadline:
                                raise TimeoutError("owned Redis fixture did not listen")
                            time.sleep(0.1)
                if self.nodes > 1:
                    self.command(["exec", self.owned[0], "redis-cli", "--cluster", "create",
                                  *[f"127.0.0.1:{port}" for port in ports], "--cluster-replicas", "0", "--cluster-yes"])
                    while "cluster_state:ok" not in self.command(["exec", self.owned[0], "redis-cli", "-p", str(ports[0]), "cluster", "info"]):
                        if time.monotonic() >= self.deadline:
                            raise TimeoutError("Redis cluster did not become ready")
                        time.sleep(0.2)
                urls = [f"redis://:{self.password}@127.0.0.1:{port}" for port in ports]
                result = RustIntegration.execute(self.context, "sigbot-core", "integration", self.tests,
                    {"MARKETPULSE_REDIS_URL": ",".join(urls), "IT_REDIS_NODES": ",".join(urls), "IT_REDIS_PASSWORD": self.password})
                result.update(nodes=self.nodes, image=image, authenticated=True,
                              boundary="real Redis; owned disposable containers; loopback-only")
            finally:
                self.cleanup()
        result["cleanup_complete"] = True
        return result
