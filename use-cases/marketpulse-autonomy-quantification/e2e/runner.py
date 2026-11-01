#!/usr/bin/env python3
"""Single case entrypoint: bounded scenarios, deployment, evidence and optional cleanup."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path



class CaseBootstrap:
    @classmethod
    def prepare(cls, root, deployer, browser_required):
        interpreter = Path(os.environ.get("E2E_PYTHON", root / ".venv/bin/python"))
        if not interpreter.is_absolute():
            interpreter = root / interpreter
        interpreter = interpreter.absolute()
        if not interpreter.exists():
            subprocess.run([sys.executable, "-m", "venv", str(interpreter.parent.parent)], check=True)
        requirements = root / "requirements.txt"
        digest = hashlib.sha256(requirements.read_bytes()).hexdigest()
        marker = interpreter.parent.parent / ".requirements.sha256"
        if not marker.is_file() or marker.read_text().strip() != digest:
            subprocess.run(
                [str(interpreter), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(requirements)],
                check=True,
            )
            marker.write_text(digest + "\n")
        browser = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE", os.environ.get("MARKETPULSE_CHROMIUM_PATH"))
        if browser and not (Path(browser).is_file() and os.access(browser, os.X_OK)):
            raise ValueError("configured Chromium executable is missing or not executable")
        if deployer == "docker" and browser_required and not browser:
            subprocess.run([str(interpreter), "-m", "playwright", "install", "chromium"], check=True)
        # Do not resolve either path: venv/python is commonly a symlink to the
        # system binary, but its path is what activates the virtual environment.
        if Path(sys.executable).absolute() != interpreter:
            os.execve(str(interpreter), [str(interpreter), str(Path(__file__).resolve()), *sys.argv[1:]], dict(os.environ))


class CaseRunner:
    scenarios = {
        "structure": "verifier.infra.s00_structure",
        "schemas": "verifier.infra.s01_schemas",
        "infra": "verifier.infra.s02_services",
        "acl": "verifier.infra.s03_acl",
        "database": "verifier.infra.s04_database",
        "rust-cache": "verifier.infra.s05_cache",
        "rust-contracts": "verifier.infra.s06_contracts",
        "deployment": "verifier.infra.s07_deployment",
        "kubernetes-prepare": "verifier.controller.s10_kubernetes",
        "rust-deployment": "verifier.controller.s11_lifecycle",
        "boundaries": "verifier.datafeed.s10_boundaries",
        "rust-boundaries": "verifier.datafeed.s11_ingestion",
        "rust-policy": "verifier.datafeed.s12_policy_news",
        "rust-market": "verifier.exchange.s13_public_market",
        "rust-extensions": "verifier.datafeed.s14_history",
        "rust-messaging": "verifier.messager.s20_delivery",
        "rust-effects": "verifier.strategy.s30_effects",
        "rust-owners": "verifier.order.s31_ownership",
        "rust-groups": "verifier.order.s32_execution_groups",
        "rust-resting": "verifier.backtest.s33_resting_orders",
        "rust-export": "verifier.exporter.s34_kafka",
        "rust-logs": "verifier.logservice.s35_archive",
        "runtime": "verifier.workflow.s40_runtime",
        "workflow": "verifier.workflow.s41_autonomy",
        "recovery": "verifier.workflow.s42_recovery",
        "replay": "verifier.backtest.s43_replay",
        "groups": "verifier.order.s44_compensation",
        "logs": "verifier.logservice.s45_delivery",
        "web": "verifier.web.s60_dashboard",
        "web-full": "verifier.web.s61_authenticated_workflow",
    }

    @classmethod
    def main(cls):
        root = Path(__file__).resolve().parent
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--scenario", action="append", help="repeat or comma-separate: " + ",".join(cls.scenarios))
        parser.add_argument("--rounds", type=int, default=1)
        parser.add_argument("--deployer", choices=["docker", "kubernetes"], default="docker")
        parser.add_argument("--timeout", type=float, default=600)
        parser.add_argument("--config", type=Path, default=root / "config/sigbot/config.yaml")
        parser.add_argument("--deploy", action="store_true")
        parser.add_argument("--reuse-environment", action="store_true")
        parser.add_argument("--skip-build", "--reuse-build", dest="skip_build", action="store_true")
        parser.add_argument("--cleanup-after-run", action="store_true")
        parser.add_argument("--cleanup", action="store_true")
        parser.add_argument("--cleanup-workers", action="store_true", help="remove only the owned Kubernetes worker acceptance namespace")
        parser.add_argument("--stop-runtime", action="store_true", help="stop only owned product processes; keep infrastructure and evidence")
        args = parser.parse_args()
        if args.rounds < 1 or args.timeout <= 0:
            parser.error("rounds and timeout must be positive")
        matrix = json.loads((root / "config/sigbot/matrix.json").read_text())
        selected = list(dict.fromkeys(name for group in (args.scenario or [",".join(matrix[args.deployer])])
                                     for name in group.split(",")))
        if set(selected) - cls.scenarios.keys():
            parser.error("unknown scenario")
        CaseBootstrap.prepare(root, args.deployer, bool({"web", "web-full"}.intersection(selected)))
        from common.broker import BrokerCredentials
        from common.context import UuidV7, VerificationContext
        from common.environment import CaseEnvironment
        from common.services import ProductServices
        from deploy.deployer.base import ControllerFactory
        report_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        while (root / "reports" / report_id).exists():
            time.sleep(0.05)
            report_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        run_id = UuidV7.generate()
        report_root = root / "reports" / report_id
        report_root.mkdir(parents=True, exist_ok=False)
        config = VerificationContext.load_config(args.config)
        context = VerificationContext(root, report_root, run_id, "deployment", 1, args.timeout, args.deployer, config, args.skip_build)
        should_deploy = args.deploy or not args.reuse_environment
        if args.deployer == "kubernetes" and should_deploy and not os.environ.get("MARKETPULSE_KUBERNETES_PREPARED"):
            environment = {**os.environ, "MARKETPULSE_KUBERNETES_PREPARED": "true"}
            prepare = [sys.executable, str(Path(__file__).resolve()), "--deployer", "docker", "--deploy",
                       "--scenario", "infra,database", "--timeout", str(args.timeout)]
            if args.skip_build:
                prepare.append("--reuse-build")
            subprocess.run(prepare, cwd=root, env=environment, check=True)
        CaseEnvironment.initialize(root, deploy=should_deploy)
        BrokerCredentials.initialize(context)
        deployer = ControllerFactory.create(context)
        results = []
        try:
            if args.stop_runtime:
                ProductServices.cleanup(root)
                results.append({"scenario": "stop-runtime", "status": "PASS"})
            elif args.cleanup_workers:
                from common.kubernetes_workers import KubernetesWorkers
                KubernetesWorkers(context).cleanup()
                results.append({"scenario": "cleanup-workers", "status": "PASS"})
            elif args.cleanup:
                ProductServices.cleanup(root)
                deployer.cleanup()
                results.append({"scenario": "cleanup", "status": "PASS"})
            else:
                if should_deploy:
                    deployer.deploy()
                for round_number in range(1, args.rounds + 1):
                    for scenario in selected:
                        started = time.monotonic()
                        context = VerificationContext(root, report_root, run_id, scenario, round_number,
                                                      args.timeout, args.deployer, config, args.skip_build)
                        print(f"START {scenario} round={round_number}", flush=True)
                        try:
                            evidence = importlib.import_module(cls.scenarios[scenario]).verify(context)
                            result = {"scenario": scenario, "round": round_number, "status": "PASS", "evidence": evidence}
                        except Exception as error:
                            result = {"scenario": scenario, "round": round_number, "status": "FAIL",
                                      "error": cls.redact(str(error), context)}
                        result["duration_seconds"] = round(time.monotonic() - started, 3)
                        results.append(result)
                        context.evidence(f"round-{round_number}.json", json.dumps(result, indent=2, ensure_ascii=False))
                        print(f"{result['status']} {scenario}", flush=True)
        except KeyboardInterrupt:
            results.append({"scenario": context.scenario, "round": context.round_number,
                            "status": "FAIL", "error": "verification interrupted before completion"})
        except Exception as error:
            results.append({"scenario": "deployment", "status": "FAIL", "error": cls.redact(str(error), context)})
        finally:
            if args.cleanup_after_run:
                try:
                    ProductServices.stop(report_root / "runtime" / "processes.json")
                    deployer.cleanup()
                except Exception as error:
                    results.append({"scenario": "cleanup", "status": "FAIL", "error": cls.redact(str(error), context)})
            lines = ["# Marketpulse verification", "", f"Report: `{report_id}`", f"Run: `{run_id}`", "",
                     "Only executed scenarios count. Boundary mocks are synthetic external systems; they do not prove the internal pipeline.", "",
                     "| Scenario | Round | Status |", "| --- | --- | --- |"]
            lines += [f"| {r['scenario']} | {r.get('round', '-')} | {r['status']} |" for r in results]
            (report_root / "summary.md").write_text("\n".join(lines) + "\n")
            (report_root / "results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
            print(f"Report: {report_root}", flush=True)
        return 0 if results and all(r["status"] == "PASS" for r in results) else 1

    @staticmethod
    def redact(value, context):
        import os
        for key in (context.config.get("token_env"), context.config.get("database_url_env"),
                    context.config.get("mqtt", {}).get("password_env"),
                    context.config.get("mqtt", {}).get("signing_key_env"),
                    context.config.get("log_archive", {}).get("access_key_env"),
                    context.config.get("log_archive", {}).get("secret_key_env")):
            secret = os.environ.get(key, "") if key else ""
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value


if __name__ == "__main__":
    raise SystemExit(CaseRunner.main())
