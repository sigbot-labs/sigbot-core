"""Case-owned image selection and generated external-boundary test credentials."""
import importlib.util
import os
from pathlib import Path
import secrets
import shlex
from urllib.parse import quote, unquote, urlsplit


class CaseEnvironment:
    @staticmethod
    def initialize(root, deploy=False):
        repository = root.parents[2]
        spec = importlib.util.spec_from_file_location("sigbot_image_catalog", repository / "deploy/images.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        catalog = module.ImageCatalog(os.environ.get("IMAGE_SOURCE", "global"))
        os.environ.update(catalog.environment())
        directory = root / ".runtime"
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / "fixture.env"
        if path.exists():
            for line in path.read_text().splitlines():
                key, _, value = line.partition("=")
                os.environ.setdefault(key, shlex.split(value)[0])
        if not deploy:
            return
        if os.environ.get("MARKETPULSE_DATABASE_URL"):
            database = urlsplit(os.environ["MARKETPULSE_DATABASE_URL"])
            if database.hostname != "127.0.0.1" or database.path != "/sigbot" or database.username != "sigbot":
                raise ValueError("deployment requires the isolated loopback sigbot database")
            if database.password:
                password = unquote(database.password)
                if os.environ.get("MARKETPULSE_POSTGRES_PASSWORD", password) != password:
                    raise ValueError("database URL and PostgreSQL Secret differ")
                os.environ.setdefault("MARKETPULSE_POSTGRES_PASSWORD", password)
        defaults = {
            "MARKETPULSE_MOCK_TOKEN": secrets.token_urlsafe(32),
            "MARKETPULSE_POSTGRES_PASSWORD": secrets.token_urlsafe(32),
            "MARKETPULSE_MQTT_SIGNING_KEY": secrets.token_urlsafe(48),
            "MARKETPULSE_MQTT_USERNAME": "marketpulse",
            "MARKETPULSE_MINIO_ACCESS_KEY": "marketpulse",
            "MARKETPULSE_MINIO_SECRET_KEY": secrets.token_urlsafe(32),
        }
        for key, value in defaults.items():
            os.environ.setdefault(key, value)
        os.environ.setdefault("MARKETPULSE_DATABASE_URL", "postgresql://sigbot:" + quote(os.environ["MARKETPULSE_POSTGRES_PASSWORD"], safe="") + "@127.0.0.1:15439/sigbot")
        defaults["MARKETPULSE_DATABASE_URL"] = os.environ["MARKETPULSE_DATABASE_URL"]
        if not path.exists():
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as stream:
                stream.write("\n".join(f"{key}={shlex.quote(os.environ[key])}" for key in defaults) + "\n")
