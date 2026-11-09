"""Optional verified MinIO binary carrier when the official registry image is unavailable."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from common.containers import LocalContainerEngine


class MinioCarrier:
    @staticmethod
    def prepare(context):
        source = os.environ.get("MARKETPULSE_MINIO_BINARY")
        if not source:
            return
        binary = Path(source).resolve(strict=True)
        provenance = json.loads((context.root / "config/minio/provenance.json").read_text())
        with binary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != provenance["sha256"]:
            raise ValueError("MinIO binary differs from its official pinned checksum")
        dockerfile = context.root / "deploy/minio/Dockerfile"
        certificate = Path("/etc/ssl/certs/ca-certificates.crt")
        inputs = digest.encode() + dockerfile.read_bytes() + certificate.read_bytes()
        image = os.environ["MARKETPULSE_CARRIER_REPOSITORY"] + ":minio-" + hashlib.sha256(inputs).hexdigest()[:20]
        with tempfile.TemporaryDirectory(prefix="sigbot-minio-carrier-") as directory:
            directory = Path(directory)
            shutil.copy2(binary, directory / "minio")
            shutil.copy2(certificate, directory)
            shutil.copy2(dockerfile, directory)
            (directory / "data").mkdir()
            command = LocalContainerEngine().arguments() + ["build", "--network=none", "--memory=256m",
                "--cpu-quota=50000", "--tag", image, str(directory)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=context.timeout_seconds)
            evidence = context.evidence("minio-carrier-build.log", result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"verified MinIO carrier build failed; inspect {evidence}")
        os.environ["MARKETPULSE_MINIO_IMAGE"] = image
        context.evidence("minio-carrier.json", json.dumps({"image": image, "binary_sha256": digest,
            "source_url": provenance["source_url"], "scope": "verified official binary in a local carrier; not an official registry image"}, indent=2))
