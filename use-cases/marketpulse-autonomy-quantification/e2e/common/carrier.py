"""Build a local ABI-matched verification carrier; this is not a release image."""
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from packaging.requirements import Requirement
from common.rust import RustIntegration


class WorkerCarrier:
    def __init__(self, context):
        self.context = context
        self.repository = context.root.parents[2]
        self.directory = Path(os.environ.get("MARKETPULSE_CARRIER_ROOT", "/tmp/sigbot-marketpulse-carrier"))
        self.rootfs = self.directory / "rootfs"
        self.elf = set()

    def copy(self, source, target=None):
        source = Path(source)
        target = self.rootfs / str(target or source).lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target, follow_symlinks=True)
        with source.open("rb") as stream:
            if stream.read(4) == b"\x7fELF":
                self.elf.add(source)

    def dependencies(self):
        pending = ["fastapi", "uvicorn", "mcp", "a2a-sdk", "jsonschema", "websockets"]
        installed = {}
        while pending:
            name = pending.pop()
            distribution = importlib.metadata.distribution(name)
            canonical = distribution.metadata["Name"].lower().replace("_", "-")
            if canonical in installed:
                continue
            installed[canonical] = distribution.version
            base = Path(distribution.locate_file("")).resolve()
            files = [Path(distribution.locate_file(item)).resolve() for item in distribution.files or []]
            if not files:
                # Debian egg-info omits RECORD/SOURCES; use its package manager's
                # actual file manifest instead of silently dropping runtime imports.
                metadata = [path for path in base.iterdir() if path.name.lower().replace("_", "-").startswith(canonical + "-")
                            and path.name.endswith((".egg-info", ".dist-info"))]
                if len(metadata) != 1:
                    raise RuntimeError(f"cannot resolve installed dependency file manifest: {canonical}")
                owners = subprocess.run(["dpkg-query", "-S", str(metadata[0])], capture_output=True, text=True,
                                        timeout=10, check=True).stdout.splitlines()
                package = owners[0].split(": ", 1)[0]
                listing = subprocess.run(["dpkg-query", "-L", package], capture_output=True, text=True, timeout=10, check=True)
                files = [Path(filename).resolve() for filename in listing.stdout.splitlines()]
            for source in files:
                if source.is_file() and source.is_relative_to(base) and "__pycache__" not in source.parts:
                    self.copy(source, Path("/opt/marketpulse/dependencies") / source.relative_to(base))
            for specification in distribution.requires or []:
                requirement = Requirement(specification)
                if requirement.marker is None or requirement.marker.evaluate({"extra": ""}):
                    pending.append(requirement.name)
        return installed

    def stage(self):
        if sys.version_info[:2] != (3, 12):
            raise RuntimeError("local carrier requires the verified host Python 3.12 ABI")
        binary = self.repository / "target/debug/sigbot"
        source_status = {}
        for name, package in (("sigbot", "sigbot-cmd"), ("sigbot-strategy-worker", "sigbot-strategy-runner")):
            sources = RustIntegration.sources(self.repository, package, include_tests=False)
            source_status[name] = (self.repository / "target/debug" / name).stat().st_mtime < max(path.stat().st_mtime for path in sources)
        newer_sources = any(source_status.values())
        pinned = os.environ.get("MARKETPULSE_CARRIER_BINARY_SHA256")
        if pinned and hashlib.sha256(binary.read_bytes()).hexdigest() != pinned:
            raise RuntimeError("the product binary no longer matches the explicitly selected acceptance baseline")
        if newer_sources and not pinned:
            raise RuntimeError("the sigbot binary predates runtime source changes; complete the coordinated build first")
        self.directory.mkdir(parents=True, exist_ok=True)
        marker = self.directory / ".owner"
        if self.rootfs.exists():
            if self.rootfs.is_symlink() or not marker.is_file() or marker.read_text() != "sigbot-marketpulse-carrier":
                raise RuntimeError("refusing to replace a staging directory outside this case")
            shutil.rmtree(self.rootfs)
        marker.write_text("sigbot-marketpulse-carrier")
        self.rootfs.mkdir()
        for binary in ("sigbot", "sigbot-strategy-worker"):
            self.copy(self.repository / "target/debug" / binary, "/usr/local/bin/" + binary)
        self.copy("/usr/bin/python3.12")
        stdlib = Path(sysconfig.get_path("stdlib"))
        for source in stdlib.rglob("*"):
            if source.is_file() and "__pycache__" not in source.parts:
                self.copy(source)
        packages = self.dependencies()
        for directory in ("common/news", "config/twitter", "deploy/mock-twitter-service/app"):
            for source in (self.context.root / directory).rglob("*"):
                if source.is_file() and "__pycache__" not in source.parts:
                    self.copy(source, Path("/opt/marketpulse") / source.relative_to(self.context.root))
        self.copy(self.repository / "etc/sigbot.yaml", "/etc/sigbot/config.yaml")
        for filename in ("/etc/ssl/certs/ca-certificates.crt", "/etc/nsswitch.conf"):
            self.copy(filename)
        for binary in list(self.elf):
            result = subprocess.run(["ldd", str(binary)], capture_output=True, text=True, check=False, timeout=10)
            if "not found" in result.stdout:
                raise RuntimeError(f"unresolved host ABI dependency: {binary.name}")
            for filename in re.findall(r"(?:=>\s*)?(/[^\s()]+)", result.stdout):
                self.copy(filename)
        for name, contents in {"passwd": "sigbot:x:1000:1000:Sigbot:/tmp:/usr/bin/python3.12\n",
                               "group": "sigbot:x:1000:\n"}.items():
            (self.rootfs / "etc" / name).write_text(contents)
        for name in ("tmp", "var/lib/sigbot/storage", "var/lib/sigbot/logs"):
            (self.rootfs / name).mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.context.root / "deploy/worker-carrier/Dockerfile", self.directory / "Dockerfile")
        fingerprints = {str(path.relative_to(self.rootfs)): hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in sorted(self.rootfs.rglob("*")) if path.is_file()}
        fingerprints["Dockerfile"] = hashlib.sha256((self.directory / "Dockerfile").read_bytes()).hexdigest()
        if pinned and fingerprints["usr/local/bin/sigbot"] != pinned:
            raise RuntimeError("the product binary changed while preparing the carrier")
        digest = hashlib.sha256(json.dumps(fingerprints, sort_keys=True).encode()).hexdigest()
        image = f"localhost/sigbot-marketpulse-carrier:{digest[:20]}"
        provenance = {"image": image, "rootfs_sha256": digest, "python": sys.version.split()[0],
                      "explicit_binary_baseline": pinned, "newer_runtime_sources_exist": newer_sources,
                      "source_freshness_by_binary": {name: not newer for name, newer in source_status.items()},
                      "packages": packages, "binary_sha256": {name: fingerprints["usr/local/bin/" + name]
                      for name in ("sigbot", "sigbot-strategy-worker")},
                      "scope": "host ABI matched debug verification carrier; not a portable release artifact"}
        (self.directory / "provenance.json").write_text(json.dumps(provenance, indent=2))
        return provenance

    def build_and_import(self):
        provenance = self.stage()
        commands = [
            ["sudo", "-n", "podman", "build", "--network=none", "--memory=768m", "--cpu-quota=100000",
             "--tag", provenance["image"], str(self.directory)],
            ["sudo", "-n", "podman", "save", "--format=oci-archive", "--output", str(self.directory / "image.oci"), provenance["image"]],
            ["sudo", "-n", "k3s", "ctr", "-n", "k8s.io", "images", "import", str(self.directory / "image.oci")],
        ]
        for index, command in enumerate(commands):
            result = subprocess.run(command, capture_output=True, text=True, timeout=self.context.timeout_seconds,
                                    env={**os.environ, "KUBECONFIG": "/etc/rancher/k3s/k3s.yaml"})
            evidence = self.context.evidence(f"carrier-{index}.log", result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"carrier command failed; inspect {evidence}")
        self.context.evidence("carrier-provenance.json", json.dumps(provenance, indent=2))
        return provenance
