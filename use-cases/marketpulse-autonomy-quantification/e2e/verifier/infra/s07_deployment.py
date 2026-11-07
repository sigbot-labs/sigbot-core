import importlib.util
import subprocess


class DeploymentVerifier:
    def __init__(self, context):
        self.context = context
        self.repository = context.root.parents[2]
        spec = importlib.util.spec_from_file_location("sigbot_images", self.repository / "deploy/images.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.catalog = module.ImageCatalog

    def command(self, arguments, input_text=None):
        result = subprocess.run(
            arguments,
            cwd=self.repository,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=self.context.timeout_seconds,
            check=False,
        )
        if result.returncode:
            raise AssertionError(f"deployment validation failed: {' '.join(arguments)}\n{result.stderr[-2000:]}")
        return result.stdout

    @staticmethod
    def overrides(values, prefix=""):
        result = []
        for key, value in values.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                result.extend(DeploymentVerifier.overrides(value, path))
            else:
                result.extend(["--set-string", f"{path}={value}"])
        return result

    def run(self):
        deploy = self.repository / "deploy"
        redundant = [str(path.relative_to(self.repository)) for path in deploy.rglob("*")
                     if path.is_file() and any(token in path.name.lower() for token in ("aliyun", "global"))]
        if redundant:
            raise AssertionError(f"registry-specific deployment files are forbidden: {redundant}")
        catalogs = {source: self.catalog(source, overrides={}).environment() for source in ("global", "aliyun")}
        if catalogs["global"].keys() != catalogs["aliyun"].keys():
            raise AssertionError("global and aliyun image keys differ")
        for key, image in catalogs["aliyun"].items():
            if key != "IMAGE_SOURCE" and not image.split("/", 1)[0].endswith(".aliyuncs.com"):
                raise AssertionError(f"{key} bypasses the selected Aliyun registry")
        if catalogs["global"]["EMQX_IMAGE"] != "docker.io/emqx/emqx:6.3.2":
            raise AssertionError("EMQX must stay on the accepted current release")
        chart = self.repository / "deploy/helm/sigbot"
        templates = chart / "templates"
        root_templates = {path.name for path in templates.iterdir() if path.is_file()}
        if root_templates != {"_helpers.tpl"}:
            raise AssertionError(f"Helm templates must be domain-grouped: {root_templates}")
        expected_layers = {"api", "controller", "gateway", "middleware", "observability", "platform", "web"}
        actual_layers = {path.name for path in templates.iterdir() if path.is_dir()}
        if actual_layers != expected_layers:
            raise AssertionError(f"Helm domain layers differ: {sorted(actual_layers)}")
        bootstrap = chart / "bootstrap"
        scripts = [bootstrap / name for name in ("setup-secret.sh", "setup-secret-k8s.sh", "setup-secret-aws.sh",
                                                   "setup-secret-gcp.sh", "setup-secret-vault.sh")]
        if not (bootstrap / "sigbot-secrets.env.example").is_file() or any(not path.is_file() for path in scripts):
            raise AssertionError("multi-provider Secret bootstrap is incomplete")
        for script in scripts:
            self.command(["bash", "-n", str(script)])
        self.command(["helm", "dependency", "list", str(chart)])
        self.command(["helm", "lint", str(chart)])
        for source in ("global", "aliyun"):
            values = self.catalog(source, overrides={}).helm_values()
            rendered = self.command(["helm", "template", "sigbot", str(chart), *self.overrides(values),
                                     "--set", "sigbot.monitoring.enabled=true",
                                     "--set", "sigbot.monitoring.serviceMonitor.enabled=true",
                                     "--set", "sigbot.monitoring.grafana.enabled=true"])
            for component in ("backtest", "datafeed", "evaluator", "exporter", "logger", "notifier",
                              "strategy", "controller", "order", "wallet"):
                if f"dashboard-{component}" not in rendered:
                    raise AssertionError(f"missing Grafana dashboard: {component}")
            if "kind: ServiceMonitor" not in rendered:
                raise AssertionError("Prometheus ServiceMonitor was not rendered")
        case_chart = self.context.root / "deploy/helm/marketpulse"
        self.command(["helm", "lint", str(case_chart)])
        for source, images in catalogs.items():
            overrides = []
            for service in ("twitter", "truthsocial", "openai"):
                overrides.extend(["--set-string", f"services.{service}.image={images[f'MARKETPULSE_{service.upper()}_IMAGE']}"])
            self.command(["helm", "template", f"marketpulse-{source}", str(case_chart), *overrides])
        return {"image_sources": ["global", "aliyun"], "helm_charts": ["sigbot", "marketpulse"],
                "grafana_dashboards": 10, "secret_providers": ["kubernetes", "aws", "gcp", "vault"],
                "registry_specific_files": 0, "override_mode": "helm --set-string"}


def verify(context):
    return DeploymentVerifier(context).run()
