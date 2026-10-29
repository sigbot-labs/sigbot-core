"""External S3 infrastructure bootstrap; product logservice remains the only log writer."""
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class ArchiveStorage:
    def __init__(self, context):
        config = context.config["log_archive"]
        self.bucket = config["bucket"]
        access = os.environ.get(config["access_key_env"])
        secret = os.environ.get(config["secret_key_env"])
        if not access or not secret:
            raise RuntimeError("log archive access/secret credential environment variables are required")
        self.client = boto3.client("s3", endpoint_url=config["endpoint"], region_name=config["region"],
                                   aws_access_key_id=access, aws_secret_access_key=secret,
                                   config=Config(connect_timeout=10, read_timeout=10, retries={"max_attempts": 2},
                                                 s3={"addressing_style": "path"}))

    def bootstrap(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def read(self, key):
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        if response["ContentLength"] > 262_144:
            raise ValueError("log object exceeds the product record bound")
        with response["Body"] as body:
            return body.read(262_145)
