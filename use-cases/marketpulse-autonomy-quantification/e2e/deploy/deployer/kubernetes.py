import base64
import json
import os
import shlex

from deploy.deployer.base import Deployer


class KubernetesDeployer(Deployer):
    namespace = "sigbot-marketpulse-e2e"

    def kubectl(self):
        return shlex.split(os.environ.get("MARKETPULSE_KUBECTL_COMMAND", "kubectl"))

    def deploy(self):
        namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace,
                     "labels": {"app.kubernetes.io/part-of": "sigbot-marketpulse-e2e"}}}
        self.command(self.kubectl() + ["apply", "-f", "-"], input_text=json.dumps(namespace))
        secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "marketpulse-boundaries", "namespace": self.namespace},
                  "type": "Opaque", "data": {"token": base64.b64encode(self.context.secret("token_env").encode()).decode()}}
        self.command(self.kubectl() + ["apply", "-f", "-"], input_text=json.dumps(secret))
        helm = shlex.split(os.environ.get("MARKETPULSE_HELM_COMMAND", "helm"))
        self.command(helm + ["upgrade", "--install", "marketpulse", "deploy/helm/marketpulse",
                     "--namespace", self.namespace, "--wait", "--timeout", f"{int(self.context.timeout_seconds)}s"])

    def cleanup(self):
        KubernetesCleaner(self.context).cleanup()


class KubernetesCleaner(KubernetesDeployer):
    def cleanup(self):
        data = self.command(self.kubectl() + ["get", "namespace", self.namespace, "-o", "json", "--ignore-not-found"])
        if not data.strip():
            return
        labels = json.loads(data)["metadata"].get("labels", {})
        if labels.get("app.kubernetes.io/part-of") != "sigbot-marketpulse-e2e":
            raise RuntimeError("refusing to delete a namespace not owned by this case")
        self.command(self.kubectl() + ["delete", "namespace", self.namespace, "--wait=false"])
