import ast
from pathlib import Path


def verify(context):
    required = ["runner.py", "requirements.txt", "README.md", "README_CN.md", "common/context.py",
                "deploy/deployer/base.py", "deploy/deployer/docker.py", "deploy/deployer/kubernetes.py",
                "verifier/infra/s00_infra.py", "verifier/web/s60_dashboard.py"]
    for filename in required:
        if not (context.root / filename).is_file():
            raise AssertionError(f"missing required file: {filename}")
    source_count = 0
    for path in context.root.rglob("*.py"):
        if "reports" in path.parts:
            continue
        ast.parse(path.read_text(), filename=str(path))
        source_count += 1
        if "app" in path.parts and len(path.read_text().splitlines()) > 800:
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
    return {"python_sources": source_count, "structure": "valid"}
