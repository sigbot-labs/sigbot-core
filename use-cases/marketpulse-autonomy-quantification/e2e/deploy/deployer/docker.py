import os
import shlex

from deploy.deployer.base import Deployer


class DockerDeployer(Deployer):
    def prefix(self):
        return shlex.split(os.environ.get("MARKETPULSE_COMPOSE_COMMAND", "docker compose")) + [
            "--project-name", "sigbot-marketpulse", "-f", "deploy/docker-compose.yaml"]

    def deploy(self):
        self.context.secret("token_env")
        command = self.prefix() + ["up", "-d", "--wait"]
        if not self.context.skip_build:
            command.append("--build")
        self.command(command)

    def cleanup(self):
        DockerCleaner(self.context).cleanup()


class DockerCleaner(DockerDeployer):
    def cleanup(self):
        # Only this fixed project is removed; named images and other workloads are retained.
        self.command(self.prefix() + ["down", "--volumes", "--remove-orphans"])
