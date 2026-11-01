from __future__ import annotations

import os
import shlex
import subprocess
from abc import ABC, abstractmethod


class Deployer(ABC):
    def __init__(self, context):
        self.context = context

    def command(self, argv, *, input_text=None):
        result = subprocess.run(argv, cwd=self.context.root, input=input_text, text=True,
                                capture_output=True, timeout=self.context.timeout_seconds, check=False,
                                env={**os.environ, "KUBECONFIG": "/etc/rancher/k3s/k3s.yaml"})
        if result.returncode:
            # Commands never contain credentials. Do not log stdout/stderr containing rendered Secrets.
            raise RuntimeError(f"deployment command failed ({result.returncode}): {shlex.join(argv)}")
        return result.stdout

    @abstractmethod
    def deploy(self):
        raise NotImplementedError

    @abstractmethod
    def cleanup(self):
        raise NotImplementedError


class ControllerFactory:
    @staticmethod
    def create(context):
        if context.deployer == "docker":
            from deploy.deployer.docker import DockerController
            return DockerController(context)
        if context.deployer == "kubernetes":
            from deploy.deployer.kubernetes import KubernetesController
            return KubernetesController(context)
        raise ValueError("unsupported deployer")
