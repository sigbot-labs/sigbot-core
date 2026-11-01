import os

from common.rust import RustIntegration


def verify(context):
    if not context.skip_build:
        raise RuntimeError("rust-deployment requires --reuse-build; build the real test with the coordinated product build")
    variables = ["MARKETPULSE_WORKER_IMAGE", "MARKETPULSE_WORKER_NAMESPACE", "MARKETPULSE_WORKER_CONFIG_PATH",
                 "MARKETPULSE_WORKER_SNAPSHOT_PATH", "MARKETPULSE_WORKER_EGRESS_CIDRS",
                 "MARKETPULSE_DATABASE_URL", "KUBECONFIG"]
    environment = {name: os.environ[name] for name in variables if name != "KUBECONFIG"}
    environment["KUBECONFIG"] = "/etc/rancher/k3s/k3s.yaml"
    return RustIntegration.execute(context, "sigbot-controller", "worker_lifecycle", [
        "real_kubernetes_worker_lifecycle_and_scope",
    ], environment, command_prefix=["sudo", "-n", "--preserve-env=" + ",".join(variables)])
