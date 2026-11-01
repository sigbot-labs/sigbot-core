from common.kubernetes_workers import KubernetesWorkers


def verify(context):
    return KubernetesWorkers(context).deploy()
