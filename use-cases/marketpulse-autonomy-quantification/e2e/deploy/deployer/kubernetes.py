import base64
import json
import os
import shlex
import tempfile

from common.containers import LocalContainerEngine
from deploy.deployer.base import Deployer


class KubernetesController(Deployer):
    namespace = "sigbot-marketpulse-e2e"
    services = ("twitter", "truthsocial", "openai")

    def kubectl(self):
        return shlex.split(os.environ.get("MARKETPULSE_KUBECTL_COMMAND", "kubectl"))

    def images(self):
        return {service: os.environ[f"MARKETPULSE_{service.upper()}_IMAGE"] for service in self.services}

    def prepare_images(self):
        images = self.images()
        engine = LocalContainerEngine()
        prefix = engine.arguments(preserve_environment=True)
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        if not self.context.skip_build:
            for service, image in images.items():
                command = prefix + ["build", "--network", "host", "--tag", image,
                    "--file", f"deploy/mock-{service}-service/Dockerfile",
                    "--build-arg", f"PYTHON_IMAGE={os.environ['PYTHON_IMAGE']}"]
                if proxy:
                    command += ["--build-arg", f"HTTPS_PROXY={proxy}", "--build-arg", f"HTTP_PROXY={proxy}"]
                command.append(".")
                self.command(command)
        else:
            for image in images.values():
                self.command(prefix + ["image", "inspect", image])
        descriptor, archive = tempfile.mkstemp(prefix="sigbot-marketpulse-images-", suffix=".tar")
        os.close(descriptor)
        try:
            self.command(prefix + ["save", "--output", archive, *images.values()])
            self.command(["sudo", "-n", "k3s", "ctr", "-n", "k8s.io", "images", "import", archive])
        finally:
            os.unlink(archive)
        return images

    def deploy(self):
        images = self.prepare_images()
        namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace,
                     "labels": {"app.kubernetes.io/part-of": "sigbot-marketpulse-e2e"}}}
        self.command(self.kubectl() + ["apply", "-f", "-"], input_text=json.dumps(namespace))
        secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "marketpulse-boundaries", "namespace": self.namespace},
                  "type": "Opaque", "data": {"token": base64.b64encode(self.context.secret("token_env").encode()).decode()}}
        self.command(self.kubectl() + ["apply", "-f", "-"], input_text=json.dumps(secret))
        helm = shlex.split(os.environ.get("MARKETPULSE_HELM_COMMAND", "helm"))
        overrides = [item for service, image in images.items()
                     for item in ("--set-string", f"services.{service}.image={image}")]
        self.command(helm + ["upgrade", "--install", "marketpulse", "deploy/helm/marketpulse",
                     "--namespace", self.namespace, "--wait", "--timeout", f"{int(self.context.timeout_seconds)}s",
                     *overrides])

    def cleanup(self):
        KubernetesCleaner(self.context).cleanup()


class KubernetesCleaner(KubernetesController):
    def cleanup(self):
        data = self.command(self.kubectl() + ["get", "namespace", self.namespace, "-o", "json", "--ignore-not-found"])
        if not data.strip():
            return
        labels = json.loads(data)["metadata"].get("labels", {})
        if labels.get("app.kubernetes.io/part-of") != "sigbot-marketpulse-e2e":
            raise RuntimeError("refusing to delete a namespace not owned by this case")
        self.command(self.kubectl() + ["delete", "namespace", self.namespace, "--wait=false"])
