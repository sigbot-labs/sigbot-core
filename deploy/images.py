#!/usr/bin/env python3
"""One image catalog for builds, Compose, Helm and use-case deployment."""
import argparse
import json
import os
import shlex


class ImageCatalog:
    mirror_registry = "registry.cn-shenzhen.aliyuncs.com/wl4g"
    external_images = {
        "RUST_BUILDER_IMAGE": (
            "docker.io/library/rust:1.99.1-slim",
            "rust:1.99.1-slim",
        ),
        "RUNTIME_IMAGE": ("docker.io/library/debian:trixie-slim", "debian:trixie-slim"),
        "WEB_NODE_IMAGE": ("docker.io/library/node:22-alpine", "node:22-alpine"),
        "WEB_RUNTIME_IMAGE": (
            "docker.io/nginxinc/nginx-unprivileged:1.27-alpine",
            "nginxinc_nginx-unprivileged:1.27-alpine",
        ),
        "PYTHON_IMAGE": ("docker.io/library/python:3.12-slim", "python:3.12-slim"),
        "POSTGRES_IMAGE": (
            "docker.io/timescale/timescaledb-ha@sha256:98f88217cb7be56655f842d9920f90d0b4893b67b3463f93084548b433ed5c93",
            "timescale_timescaledb-ha@sha256:98f88217cb7be56655f842d9920f90d0b4893b67b3463f93084548b433ed5c93",
        ),
        "REDIS_IMAGE": (
            "docker.io/bitnami/redis-cluster:8.10",
            "bitnami_redis-cluster:8.10",
        ),
        "PGVECTOR_IMAGE": ("docker.io/pgvector/pgvector:pg18", "pgvector_pgvector:pg18"),
        "EMQX_IMAGE": ("docker.io/emqx/emqx:6.3.2", "emqx_emqx:6.3.2"),
        "MINIO_IMAGE": (
            "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z",
            "minio_minio:RELEASE.2025-04-22T22-12-26Z",
        ),
        "KAFKA_IMAGE": ("docker.io/apache/kafka:4.1.1", "apache_kafka:4.1.1"),
        "AUTHGUARD_IMAGE": ("ghcr.io/wl4g/authguard:1.0.2", "authguard:1.0.2"),
        "AUTHGUARD_WEB_IMAGE": ("ghcr.io/wl4g/authguard-web:1.0.2", "authguard-web:1.0.2"),
        "ENVOY_GATEWAY_IMAGE": ("docker.io/envoyproxy/gateway:v1.9.0", "envoyproxy_gateway:v1.9.0"),
        "ENVOY_PROXY_IMAGE": (
            "docker.io/envoyproxy/envoy:distroless-v1.36.4",
            "envoyproxy_envoy:distroless-v1.36.4",
        ),
        "ENVOY_RATELIMIT_IMAGE": (
            "docker.io/envoyproxy/ratelimit:17b1956c",
            "envoyproxy_ratelimit:17b1956c",
        ),
        "AUTHGUARD_REDIS_IMAGE": (
            "docker.io/bitnami/redis-cluster:7.0.14",
            "bitnami_redis-cluster:7.0.14",
        ),
        "AUTHGUARD_POSTGRES_IMAGE": (
            "docker.io/bitnami/postgresql:18.3",
            "bitnami_postgresql:18.3",
        ),
    }

    def __init__(self, source, version="1.0.0", overrides=None):
        if source not in {"aliyun", "global"}:
            raise ValueError("IMAGE_SOURCE must be aliyun or global")
        if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in version):
            raise ValueError("invalid image version")
        self.source, self.version = source, version
        self.overrides = os.environ if overrides is None else overrides

    def environment(self):
        index = 0 if self.source == "global" else 1
        images = {key: pair[index] for key, pair in self.external_images.items()}
        if self.source == "aliyun":
            images = {key: f"{self.mirror_registry}/{name}" for key, name in images.items()}
        registry = self.mirror_registry if self.source == "aliyun" else "ghcr.io/wl4g"
        images.update(
            SIGBOT_IMAGE=f"{registry}/sigbot:{self.version}",
            SIGBOT_WEB_IMAGE=f"{registry}/sigbot-web:{self.version}",
            MARKETPULSE_CARRIER_REPOSITORY=f"{registry}/sigbot-marketpulse-carrier",
        )
        for service in ("TWITTER", "TRUTHSOCIAL", "OPENAI"):
            images[f"MARKETPULSE_{service}_IMAGE"] = f"{registry}/sigbot-mock-{service.lower()}:{self.version}"
        images.update(
            MARKETPULSE_POSTGRES_IMAGE=images["POSTGRES_IMAGE"],
            MARKETPULSE_EMQX_IMAGE=images["EMQX_IMAGE"],
            MARKETPULSE_MINIO_IMAGE=images["MINIO_IMAGE"],
            MARKETPULSE_KAFKA_IMAGE=images["KAFKA_IMAGE"],
        )
        for key in list(images):
            images[key] = self.overrides.get(key, images[key])
            if self.source == "aliyun" and not images[key].split("/", 1)[0].endswith(".aliyuncs.com"):
                raise ValueError(f"{key} must use an aliyuncs.com registry when IMAGE_SOURCE=aliyun")
        images["IMAGE_SOURCE"] = self.source
        return images

    @staticmethod
    def image_fields(reference):
        name, _, digest = reference.partition("@")
        registry, _, repository = name.rpartition("/")
        repository, separator, tag = repository.partition(":")
        return {"registry": registry, "repository": repository, "tag": tag if separator else "", "digest": digest}

    def helm_values(self):
        images = self.environment()

        def image(key):
            return self.image_fields(images[key])

        return {
            "global": {"imageSource": self.source},
            "sigbot": {
                "image": image("SIGBOT_IMAGE"),
                "controller": {
                    "image": image("SIGBOT_IMAGE"),
                    "middleware": {
                        "redis": {"image": images["REDIS_IMAGE"]},
                        "pgvector": {"image": images["PGVECTOR_IMAGE"]},
                        "timescaledb": {"image": images["POSTGRES_IMAGE"]},
                        "emqx": {"image": images["EMQX_IMAGE"]},
                    },
                },
                "web": {"image": image("SIGBOT_WEB_IMAGE")},
            },
            "middleware": {
                "postgres": {"image": image("POSTGRES_IMAGE")},
                "redis": {"image": image("REDIS_IMAGE")},
                "authguard": {
                    "authguard": {
                        "authn": {"image": self.full_repository(image("AUTHGUARD_IMAGE"))},
                        "authz": {"image": self.full_repository(image("AUTHGUARD_IMAGE"))},
                        "web": {"image": self.full_repository(image("AUTHGUARD_WEB_IMAGE"))},
                    },
                    "envoy_gateway": {"global": {"images": {
                        "envoyGateway": {"image": images["ENVOY_GATEWAY_IMAGE"]},
                        "envoyProxy": {"image": images["ENVOY_PROXY_IMAGE"]},
                        "ratelimit": {"image": images["ENVOY_RATELIMIT_IMAGE"]},
                    }}},
                    "redis_cluster": {"image": image("AUTHGUARD_REDIS_IMAGE")},
                    "postgresql": {"image": image("AUTHGUARD_POSTGRES_IMAGE")},
                },
            },
        }

    @staticmethod
    def full_repository(image):
        return {"repository": image["registry"] + "/" + image["repository"], "tag": image["tag"]}

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--source", choices=["aliyun", "global"], required=True)
        parser.add_argument("--version", default=os.environ.get("VERSION", "1.0.0"))
        parser.add_argument("--format", choices=["shell", "env", "names", "helm"], default="shell")
        parser.add_argument("--get")
        args = parser.parse_args()
        catalog = cls(args.source, args.version)
        images = catalog.environment()
        if args.get:
            print(images[args.get])
        elif args.format == "names":
            print(" ".join(images))
        elif args.format == "helm":
            print(json.dumps(catalog.helm_values(), indent=2))
        else:
            separator = "\n" if args.format == "shell" else " "
            prefix = "export " if args.format == "shell" else ""
            print(separator.join(f"{prefix}{key}={shlex.quote(value)}" for key, value in images.items()))


if __name__ == "__main__":
    ImageCatalog.main()
