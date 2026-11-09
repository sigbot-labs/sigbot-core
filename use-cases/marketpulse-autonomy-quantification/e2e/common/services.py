"""Launch existing product CLI binaries; no business behavior lives in this harness."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import yaml
import psycopg

from common.runtime import RuntimeConfiguration
from common.funding import FundingFixture
from common.storage import ArchiveStorage


class ProductServices:
    def __init__(self, context):
        self.context = context
        self.configuration = RuntimeConfiguration(context)
        self.repository = context.root.parents[2]
        self.binary = Path(os.environ.get("MARKETPULSE_SIGBOT_BINARY", self.repository / "target/debug/sigbot")).resolve()
        self.manifest = context.report_root / "runtime" / "processes.json"

    def seed(self):
        scope = self.context.config["scope"]
        if scope["env"] != "e2e":
            raise ValueError("the supplied seed is restricted to the isolated e2e environment")
        with psycopg.connect(self.context.secret("database_url_env"), connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT env,tenant_id::text,account_id::text,mode FROM ledger_info WHERE id=%s", (self.configuration.wallet_id,))
                existing = cursor.fetchone()
                if existing is not None and existing != ("e2e", scope["tenant_id"], self.configuration.account_id, self.configuration.mode):
                    raise RuntimeError("seed wallet ID belongs to another environment; use a dedicated case database")
                cursor.execute("SELECT env,tenant_id::text FROM workflow_info WHERE id=%s", (self.configuration.workflow_id,))
                existing = cursor.fetchone()
                if existing is not None and existing != ("e2e", scope["tenant_id"]):
                    raise RuntimeError("seed workflow ID belongs to another environment; use a dedicated case database")
                cursor.execute((self.context.root / "config/sigbot/init_01.dml.sql").read_text())
                cursor.execute("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES(%s,%s,%s,%s,'running') ON CONFLICT DO NOTHING",
                               (self.configuration.run_id, self.configuration.workflow_id, scope["env"], scope["tenant_id"]))
                cursor.execute("SELECT workflow_id::text,env,tenant_id::text FROM wf_run WHERE run_id=%s", (self.configuration.run_id,))
                if cursor.fetchone() != (self.configuration.workflow_id, scope["env"], scope["tenant_id"]):
                    raise RuntimeError("run_id already belongs to a different Workflow scope")
                cursor.execute("UPDATE wf_run SET state='failed',ended_at=now() WHERE env=%s AND tenant_id=%s AND workflow_id=%s AND run_id<>%s AND state IN ('queued','running')",
                               (scope["env"], scope["tenant_id"], self.configuration.workflow_id, self.configuration.run_id))

    def start(self):
        records = []
        if self.manifest.exists():
            records = json.loads(self.manifest.read_text())
            if any(record.get("run_id") != self.configuration.run_id for record in records):
                raise RuntimeError("runtime manifest belongs to a different run_id")
            if len(records) == len(self.configuration.services()) and all(self.is_running(record) for record in records):
                os.environ[self.context.config["workflow"]["run_id_env"]] = self.configuration.run_id
                return {"run_id": self.configuration.run_id, "processes": records, "reused": True}
        if not self.binary.is_file():
            raise RuntimeError(f"real sigbot binary is missing: {self.binary}; build the product before runtime verification")
        worker = self.binary.with_name("sigbot-strategy-worker")
        if not worker.is_file():
            raise RuntimeError(f"real isolated strategy worker is missing: {worker}")
        self.seed()
        self.manifest.parent.mkdir(parents=True, exist_ok=True)
        ArchiveStorage(self.context).bootstrap()
        environment = {**os.environ, "SIGBOT_CFG_VERBOSE": "false", "SIGBOT_CFG_PATH": str(self.repository / "etc/sigbot.yaml"),
            "SIGBOT__MGMT__HOST": "127.0.0.1", "SIGBOT__MGMT__OTEL__ENABLED": "false",
            "SIGBOT__MGMT__TOKIO_CONSOLE__ENABLED": "false", "SIGBOT__LOGGING__LEVEL": "INFO",
            "SIGBOT__STORAGE__POSTGRES__MIN_CONNECTIONS": "1",
            "SIGBOT__STORAGE__POSTGRES__MAX_CONNECTIONS": "3",
            "SIGBOT__AUTHGUARD__SERVICE_TENANT_ID": self.configuration.scope["tenant_id"],
            "SIGBOT__AUTHGUARD__SERVICE_ENV": self.configuration.scope["env"],
            "SIGBOT__AUTHGUARD__SERVICE_PRINCIPAL_ID": "marketpulse-e2e-runtime",
            "SIGBOT_STRATEGY_WORKER_BINARY": str(worker), "HTTPS_PROXY": "http://127.0.0.1:8800"}
        connection = psycopg.conninfo.conninfo_to_dict(self.context.secret("database_url_env"))
        for field, target in {"host": "HOST", "port": "PORT", "dbname": "DATABASE", "user": "USERNAME", "password": "PASSWORD"}.items():
            if field in connection:
                environment["SIGBOT__STORAGE__POSTGRES__" + target] = connection[field]
        environment["SIGBOT__STORAGE__PROVIDER"] = "Postgres"
        environment["SIGBOT__SECRETS__VALUES__MARKETPULSE_API_TOKEN"] = self.context.secret("token_env")
        environment["SIGBOT__SECRETS__VALUES__MINIO_ACCESS_KEY"] = self.context.secret(
            self.context.config["log_archive"]["access_key_env"])
        environment["SIGBOT__SECRETS__VALUES__MINIO_SECRET_KEY"] = self.context.secret(
            self.context.config["log_archive"]["secret_key_env"])
        mqtt_password = environment.get(self.context.config["mqtt"]["password_env"])
        if not mqtt_password:
            raise RuntimeError("runtime requires a scoped authenticated MQTT credential")
        environment["SIGBOT__SECRETS__VALUES__MQTT_PASSWORD"] = mqtt_password
        processes = [record for record in records if self.is_running(record)]
        try:
            for index, (name, command, config) in enumerate(self.configuration.services()):
                if any(record["name"] == name for record in processes):
                    continue
                self.before_start(name)
                configuration = yaml.safe_load((self.repository / "etc/sigbot.yaml").read_text())
                messager = config.pop("messager_config")
                messager["provider"] = "MQTT"
                if command == "datafeed":
                    component = config.pop("datafeed_config")
                    component["providers"] = config.pop("providers", [])
                else:
                    component = config
                components = configuration.setdefault("services", {}).setdefault("component", {})
                components.update({"messager": messager, command: component})
                config_path = self.manifest.parent / f"{name}.yaml"
                config_path.write_text(yaml.safe_dump(configuration))
                config_path.chmod(0o600)
                argv = [str(self.binary), command, "--config", str(config_path)]
                log_path = self.manifest.parent / f"{name}.log"
                with log_path.open("ab") as log:
                    process = subprocess.Popen(argv, cwd=self.repository, env={**environment, "SIGBOT__MGMT__PORT": str(self.configuration.management_port + index)},
                                               stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                processes.append({"name": name, "pid": process.pid, "binary": str(self.binary), "log": str(log_path),
                                  "run_id": self.configuration.run_id,
                                  "start_ticks": Path(f"/proc/{process.pid}/stat").read_text().split()[21]})
                self.manifest.write_text(json.dumps(processes, indent=2))
                time.sleep(0.5)
                if process.poll() is not None:
                    raise RuntimeError(f"real {name} process exited with {process.returncode}; inspect {log_path}")
            FundingFixture.publish(self.context, self.configuration.run_id)
            os.environ[self.context.config["workflow"]["run_id_env"]] = self.configuration.run_id
            return {"run_id": self.configuration.run_id, "processes": processes,
                    "mode": self.configuration.mode, "execution": "real product binaries and isolated matching API; live public Binance market facts"}
        except Exception:
            self.stop(self.manifest)
            raise

    def before_start(self, name):
        pass

    @staticmethod
    def is_running(entry):
        process = Path(f"/proc/{entry['pid']}")
        try:
            executable = os.readlink(process / "exe")
            return (executable in {entry["binary"], entry["binary"] + " (deleted)"}
                    and (process / "stat").read_text().split()[21] == entry["start_ticks"])
        except (FileNotFoundError, ProcessLookupError):
            return False

    def restart(self, names):
        if not self.manifest.is_file():
            raise RuntimeError("recovery requires this run's real runtime process manifest")
        records = json.loads(self.manifest.read_text())
        selected = [record for record in records if record["name"] in names]
        if len(selected) != len(names) or not all(self.is_running(record) for record in selected):
            raise RuntimeError("cannot fault an absent or unrelated product process")
        for entry in selected:
            os.killpg(entry["pid"], signal.SIGKILL)
        deadline = time.monotonic() + 10
        while any(self.is_running(entry) for entry in selected) and time.monotonic() < deadline:
            time.sleep(0.05)
        if any(self.is_running(entry) for entry in selected):
            raise RuntimeError("faulted product process did not terminate")
        return self.start()

    @staticmethod
    def cleanup(root):
        for manifest in (root / "reports").glob("**/runtime/processes.json"):
            ProductServices.stop(manifest)

    @staticmethod
    def stop(manifest):
        if not manifest.exists():
            return
        records = json.loads(manifest.read_text())
        for entry in records:
            try:
                if not ProductServices.is_running(entry):
                    continue  # PID was reused; never stop an unrelated process.
                os.killpg(entry["pid"], signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            alive = [entry for entry in records if ProductServices.is_running(entry)]
            if not alive:
                break
            time.sleep(0.1)
        for entry in records:
            if ProductServices.is_running(entry):
                os.killpg(entry["pid"], signal.SIGKILL)
        manifest.unlink(missing_ok=True)
