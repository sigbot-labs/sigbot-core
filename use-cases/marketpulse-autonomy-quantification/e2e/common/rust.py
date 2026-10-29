"""Execute product integration tests, including verified reuse of compiled binaries."""
import hashlib
import json
import os
import re
import shlex
import subprocess
from pathlib import Path


class RustIntegration:
    @staticmethod
    def compiler_inputs(repository, binary):
        dependency_file = binary.with_name(binary.name + ".d")
        if not dependency_file.is_file():
            return None
        declaration = dependency_file.read_text().replace("\\\n", "").split("\n", 1)[0]
        _, separator, dependencies = declaration.partition(": ")
        if not separator:
            return None
        return [repository / path for path in shlex.split(dependencies)]

    @staticmethod
    def sources(repository, package, include_tests=True):
        metadata = subprocess.run(["cargo", "metadata", "--offline", "--no-deps", "--format-version", "1"],
                                  cwd=repository, capture_output=True, text=True, timeout=30, check=True)
        packages = {item["name"]: item for item in json.loads(metadata.stdout)["packages"]}
        sources = [repository / "Cargo.toml", repository / "Cargo.lock"]
        pending, visited = [package], set()
        while pending:
            name = pending.pop()
            if name in visited:
                continue
            visited.add(name)
            item = packages[name]
            root = Path(item["manifest_path"]).parent
            sources.extend([root / "Cargo.toml", root / "build.rs"])
            sources.extend(path for path in (root / "src").rglob("*") if path.suffix in {".rs", ".py"})
            if name == package and include_tests:
                sources.extend((root / "tests").rglob("*.rs"))
            pending.extend(dependency["name"] for dependency in item["dependencies"]
                           if dependency["name"] in packages and dependency["kind"] != "dev")
        return [path for path in sources if path.is_file()]

    @staticmethod
    def execute(context, package, target, tests, extra_environment, command_prefix=None):
        repository = context.root.parents[2]
        environment = {**os.environ, "CARGO_BUILD_JOBS": "1", "CARGO_PROFILE_DEV_DEBUG": "0",
                       "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_INCREMENTAL": "0",
                       "HTTPS_PROXY": "http://127.0.0.1:8800", **extra_environment}
        binary = None
        if context.skip_build:
            # Shared DTO/SDK changes invalidate every dependent binary too. An
            # own-crate-only mtime check could silently reuse a stale contract.
            latest_source = max(path.stat().st_mtime for path in RustIntegration.sources(repository, package))
            candidates = sorted((repository / "target/debug/deps").glob(target + "-*"),
                                key=lambda path: path.stat().st_mtime, reverse=True)
            for candidate in candidates:
                if not candidate.is_file() or not os.access(candidate, os.X_OK):
                    continue
                if latest_source > candidate.stat().st_mtime:
                    continue
                inputs = RustIntegration.compiler_inputs(repository, candidate)
                if inputs is None or any(not path.is_file() or path.stat().st_mtime > candidate.stat().st_mtime
                                         for path in inputs):
                    continue
                listing = subprocess.run([str(candidate), "--list", "--ignored"], cwd=repository,
                                         capture_output=True, text=True, timeout=15, check=False)
                if listing.returncode == 0 and all(name + ": test" in listing.stdout for name in tests):
                    binary = candidate
                    break
            if binary is None:
                raise RuntimeError(f"compiled {package}/{target} with the required tests is missing; build it first")
            command = [str(binary), "--ignored", "--nocapture", "--test-threads=1"]
        else:
            target_path = repository / "target"
            size = sum(path.stat().st_size for path in target_path.rglob("*") if path.is_file()) if target_path.exists() else 0
            if size > 10 * 1024 ** 3:
                subprocess.run(["cargo", "clean"], cwd=repository, check=True, timeout=context.timeout_seconds)
            command = ["cargo", "test", "-p", package, "--test", target, "--", "--ignored", "--nocapture", "--test-threads=1"]
        command = [*(command_prefix or []), *command]
        result = subprocess.run(command, cwd=repository, env=environment, capture_output=True, text=True,
                                timeout=context.timeout_seconds, check=False)
        output = result.stdout + result.stderr
        for key in (context.config["token_env"], context.config["database_url_env"],
                    context.config["mqtt"]["password_env"], context.config["mqtt"]["signing_key_env"]):
            if environment.get(key):
                output = output.replace(environment[key], "[REDACTED]")
        for key, value in extra_environment.items():
            if value and key.endswith(("_DATABASE_URL", "_PASSWORD", "_TOKEN", "_SECRET", "_KEY")):
                output = output.replace(value, "[REDACTED]")
        evidence = context.evidence(target + ".log", output)
        passed = re.search(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", result.stdout)
        if result.returncode or not passed or int(passed.group(1)) < len(tests):
            raise AssertionError(f"all required real {package} tests must execute and pass; inspect {evidence}")
        # With --nocapture a test may print endpoint evidence between its name
        # and `ok`. The successful aggregate above already requires zero failed
        # or ignored tests; separately require every requested test to have run.
        if not all(re.search(r"(?m)^test \S*" + re.escape(name) + r" \.\.\. ", result.stdout) for name in tests):
            raise AssertionError(f"required test names were not observed; inspect {evidence}")
        return {"command": command, "evidence": str(evidence), "tests": tests, "executed_test_count": int(passed.group(1)),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest() if binary else None}
