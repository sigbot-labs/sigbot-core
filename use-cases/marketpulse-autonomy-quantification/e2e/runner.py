#!/usr/bin/env python3
"""Single case entrypoint: bounded scenarios, deployment, evidence and optional cleanup."""
from __future__ import annotations

import argparse
import importlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from common.context import VerificationContext
from common.broker import BrokerCredentials
from common.services import ProductServices
from deploy.deployer.base import DeployerFactory


class CaseRunner:
    scenarios = {
        "structure": "verifier.infra.s01_structure",
        "schemas": "verifier.infra.s02_schemas",
        "infra": "verifier.infra.s00_infra",
        "acl": "verifier.infra.s03_acl",
        "database": "verifier.infra.s04_database",
        "legacy-migration": "verifier.infra.s06_legacy_migration",
        "kubernetes-prepare": "verifier.infra.s05_kubernetes_prepare",
        "rust-deployment": "verifier.deployer.s55_lifecycle",
        "boundaries": "verifier.datafeed.s10_boundaries",
        "rust-boundaries": "verifier.datafeed.s20_rust_boundaries",
        "rust-policy": "verifier.datafeed.s22_policy",
        "rust-extensions": "verifier.datafeed.s24_extensions",
        "rust-market": "verifier.exchange.s23_public_market",
        "rust-messaging": "verifier.messager.s21_delivery",
        "rust-owners": "verifier.order.s34_owners",
        "rust-groups": "verifier.order.s35_groups",
        "rust-effects": "verifier.strategy.s32_service_effects",
        "rust-resting": "verifier.backtest.s44_resting_orders",
        "rust-export": "verifier.exporter.s55_kafka",
        "rust-logs": "verifier.logservice.s51_tracing",
        "runtime": "verifier.pipeline.s25_runtime",
        "pipeline": "verifier.pipeline.s30_pipeline",
        "groups": "verifier.order.s36_group_pipeline",
        "recovery": "verifier.pipeline.s40_recovery",
        "replay": "verifier.backtest.s45_replay",
        "logs": "verifier.pipeline.s50_logs",
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
        parser.add_argument("--timeout", type=float, default=120)
        parser.add_argument("--config", type=Path, default=root / "config/sigbot/config.yaml")
        parser.add_argument("--deploy", action="store_true")
        parser.add_argument("--skip-build", "--reuse-build", dest="skip_build", action="store_true")
        parser.add_argument("--cleanup-after-run", action="store_true")
        parser.add_argument("--cleanup", action="store_true")
        parser.add_argument("--cleanup-workers", action="store_true", help="remove only the owned Kubernetes worker acceptance namespace")
        parser.add_argument("--stop-runtime", action="store_true", help="stop only owned product processes; keep infrastructure and evidence")
        args = parser.parse_args()
        if args.rounds < 1 or args.timeout <= 0:
            parser.error("rounds and timeout must be positive")
        selected = list(dict.fromkeys(name for group in (args.scenario or [",".join(cls.scenarios)])
                                     for name in group.split(",")))
        if set(selected) - cls.scenarios.keys():
            parser.error("unknown scenario")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        report_root = root / "reports" / run_id
        report_root.mkdir(parents=True, exist_ok=False)
        config = VerificationContext.load_config(args.config)
        context = VerificationContext(root, report_root, run_id, "deployment", 1, args.timeout, args.deployer, config, args.skip_build)
        BrokerCredentials.initialize(context)
        deployer = DeployerFactory.create(context)
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
                if args.deploy:
                    deployer.deploy()
                for round_number in range(1, args.rounds + 1):
                    round_run_id = f"{run_id}-r{round_number}"
                    for scenario in selected:
                        started = time.monotonic()
                        context = VerificationContext(root, report_root, round_run_id, scenario, round_number,
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
            lines = ["# Marketpulse verification", "", f"Run: `{run_id}`", "",
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
