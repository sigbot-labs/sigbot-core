import ast
import importlib.util
import json
from pathlib import Path
import re


def verify(context):
    required = ["runner.py", "requirements.txt", "README.md", "README_CN.md", "common/context.py",
                "deploy/deployer/base.py", "deploy/deployer/docker.py", "deploy/deployer/kubernetes.py",
                "verifier/infra/s02_services.py", "verifier/web/s60_dashboard.py"]
    for filename in required:
        if not (context.root / filename).is_file():
            raise AssertionError(f"missing required file: {filename}")
    source_count = 0
    for path in context.root.rglob("*.py"):
        if {"reports", ".venv", "__pycache__"}.intersection(path.parts):
            continue
        source_text = path.read_text()
        ast.parse(source_text, filename=str(path))
        source_count += 1
        source_lines = sum(1 for line in source_text.splitlines()
                           if line.strip() and not line.lstrip().startswith("#"))
        if "app" in path.parts and source_lines > 800:
            raise AssertionError(f"source exceeds 800 lines: {path}")
    unexpected = [str(p.relative_to(context.root)) for p in (context.root / "common").rglob("*")
                  if p.is_file() and p.suffix != ".py" and "__pycache__" not in p.parts]
    if unexpected:
        raise AssertionError(f"common must contain only Python sources: {unexpected}")
    config_extensions = {".json", ".yaml", ".yml", ".sql", ".conf", ".toml", ".env", ".gz"}
    unexpected = [str(p.relative_to(context.root)) for p in (context.root / "config").rglob("*")
                  if p.is_file() and p.suffix not in config_extensions and "__pycache__" not in p.parts]
    if unexpected:
        raise AssertionError(f"config must contain declarative configuration: {unexpected}")
    verifier = context.root / "verifier"
    allowed_domains = {"infra", "workflow", "web", "backtest", "controller", "datafeed", "evaluator",
                       "exchange", "exporter", "logger", "messager", "notifier", "order", "strategy", "wallet"}
    actual_domains = {path.name for path in verifier.iterdir() if path.is_dir() and path.name != "__pycache__"}
    if actual_domains - allowed_domains:
        raise AssertionError(f"non-domain verifier directories: {sorted(actual_domains - allowed_domains)}")
    for path in verifier.rglob("*.py"):
        if not re.fullmatch(r"s\d{2}_[a-z][a-z0-9_]*\.py", path.name):
            raise AssertionError(f"invalid verifier scenario name: {path.relative_to(context.root)}")
        functions = [node.name for node in ast.parse(path.read_text()).body
                     if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
        if functions != ["verify"]:
            raise AssertionError(f"verifier must expose only verify(context): {path.relative_to(context.root)}")
    spec = importlib.util.spec_from_file_location("marketpulse_runner", context.root / "runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    matrix = json.loads((context.root / "config/sigbot/matrix.json").read_text())
    for deployer, scenarios in matrix.items():
        numbers = [int(runner.CaseRunner.scenarios[name].rsplit(".", 1)[-1][1:3]) for name in scenarios]
        if numbers != sorted(numbers):
            raise AssertionError(f"{deployer} verifier timeline is not ordered: {list(zip(scenarios, numbers))}")
    repository = context.root.parents[2]
    domains = {"component/backtest": "Backtest", "component/datafeed": "Datafeed",
               "component/evaluator": "Evaluator", "component/logger": "Logger",
               "component/strategy/runner": "Strategy", "component/exporter": "Exporter",
               "component/notifier": "Notifier", "controller": "Controller",
               "exchange": "Exchange", "messager": "Messager", "component/order": "Order",
               "component/wallet": "Wallet"}
    forbidden_storage_tokens = ("sqlx::", "PgPool", "QueryBuilder", "SELECT ", "INSERT INTO ",
                                "UPDATE ", "DELETE FROM ", "CREATE TABLE")
    frozen = {"component/strategy/runner/src/python"}
    largest = 0
    for domain, name in domains.items():
        source = repository / "src" / domain / "src"
        contract = source / f"{name.lower()}.rs"
        if not contract.is_file() or f"pub trait I{name}: IProvider" not in contract.read_text():
            raise AssertionError(f"missing shared Provider domain contract: {domain}")
        for path in source.rglob("*.rs"):
            text = path.read_text()
            lines = sum(1 for line in text.splitlines()
                        if line.strip() and not line.lstrip().startswith("//"))
            largest = max(largest, lines)
            relative = str(path.relative_to(repository))
            if path.name.endswith("_factory.rs") or lines > 800:
                raise AssertionError(f"nonconforming domain source: {path.relative_to(repository)}")
            if not any(relative.startswith(f"src/{item}/") for item in frozen):
                if any(token in text for token in forbidden_storage_tokens):
                    raise AssertionError(f"domain source owns database SQL: {path.relative_to(repository)}")
    if (repository / "src/core/src/sys").exists() or (repository / "src/core/src/store").exists():
        raise AssertionError("obsolete core sys/store package remains")
    return {"python_sources": source_count, "domain_contracts": len(domains),
            "largest_domain_source_lines": largest, "structure": "valid"}
