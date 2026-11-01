import json
import os
import shlex
import time

from deploy.deployer.base import Deployer
from common.containers import LocalContainerEngine


class DockerController(Deployer):
    services = (
        "web", "minio", "mock-twitter", "mock-truthsocial", "mock-openai",
        "postgres", "kafka", "emqx",
    )

    def __init__(self, context):
        super().__init__(context)
        self.engine = LocalContainerEngine()

    def prefix(self):
        command = os.environ.get("MARKETPULSE_COMPOSE_COMMAND")
        prefix = shlex.split(command) if command else self.engine.arguments(preserve_environment=True) + ["compose"]
        return prefix + [
            "--project-name", "sigbot-marketpulse", "-f", "deploy/docker-compose.yaml"]

    def deploy(self):
        self.context.secret("token_env")
        from deploy.deployer.artifacts import MinioCarrier
        MinioCarrier.prepare(self.context)
        command = self.prefix() + ["up", "-d"]
        if not self.context.skip_build:
            command.append("--build")
        self.command(command)
        self.wait_until_ready()
        from common.storage import ArchiveStorage, CaseDatabase
        CaseDatabase(self.context).bootstrap(self.migrate)
        ArchiveStorage(self.context).bootstrap()

    def wait_until_ready(self):
        deadline = time.monotonic() + self.context.timeout_seconds
        pending = set(self.services)
        while pending and time.monotonic() < deadline:
            for service in tuple(pending):
                try:
                    container_id = self.command(self.prefix() + ["ps", "-q", service]).strip()
                    if not container_id:
                        continue
                    document = json.loads(self.command(self.engine.arguments() + ["inspect", container_id]))[0]
                    state = document["State"]
                    status = str(state.get("Status", "")).lower()
                    health = str((state.get("Health") or {}).get("Status", "")).lower()
                    if status in {"exited", "dead"} or health == "unhealthy":
                        raise RuntimeError(f"{service} stopped before becoming ready")
                    if status == "running" and health in {"", "healthy"}:
                        pending.remove(service)
                except (json.JSONDecodeError, KeyError, RuntimeError):
                    if time.monotonic() >= deadline:
                        raise
            if pending:
                time.sleep(1)
        if pending:
            raise TimeoutError("containers did not become ready: " + ", ".join(sorted(pending)))

    def migrate(self, script):
        self.command(self.prefix() + ["exec", "-T", "postgres", "psql", "-U", "sigbot", "-d", "sigbot",
                                     "--no-psqlrc", "--quiet"], input_text=script)

    def restart(self, service):
        if service not in self.services:
            raise ValueError("refusing to restart a service outside the owned case")
        self.command(self.prefix() + ["restart", service])
        self.wait_until_ready()

    def cleanup(self):
        DockerCleaner(self.context).cleanup()


class DockerCleaner(DockerController):
    def cleanup(self):
        # Only this fixed project is removed; named images and other workloads are retained.
        self.command(self.prefix() + ["down", "--volumes", "--remove-orphans"])
