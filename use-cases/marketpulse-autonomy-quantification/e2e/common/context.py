from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class VerificationContext:
    root: Path
    report_root: Path
    run_id: str
    scenario: str
    round_number: int
    timeout_seconds: float
    deployer: str
    config: dict
    skip_build: bool = False

    @classmethod
    def load_config(cls, path: Path) -> dict:
        config = yaml.safe_load(path.read_text())
        allowed = {"endpoints", "scope", "workflow_id", "account_id", "token_env", "database_url_env", "mqtt", "pipeline", "log_archive"}
        unknown = set(config) - allowed
        if unknown:
            raise ValueError(f"unknown configuration keys: {sorted(unknown)}")
        return config

    def secret(self, key: str) -> str:
        name = self.config[key]
        value = os.environ.get(name)
        if not value:
            raise RuntimeError(f"required environment variable {name} is not set")
        return value

    def endpoint(self, name: str) -> str:
        return self.config["endpoints"][name].rstrip("/")

    def headers(self) -> dict:
        return {"authorization": f"Bearer {self.secret('token_env')}",
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}

    def evidence(self, name: str, data: str) -> Path:
        path = self.report_root / self.scenario / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data)
        return path
