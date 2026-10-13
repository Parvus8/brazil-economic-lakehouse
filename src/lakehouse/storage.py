from typing import BinaryIO

import boto3
from botocore.exceptions import ClientError

from lakehouse.config import Settings


class ObjectStore:
    def __init__(self, settings: Settings):
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",
        )

    def ensure_bucket(self, bucket: str) -> None:
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                raise
            self._client.create_bucket(Bucket=bucket)

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def put_bytes(self, bucket: str, key: str, data: bytes) -> str:
        self.ensure_bucket(bucket)
        self._client.put_object(Bucket=bucket, Key=key, Body=data)
        return f"s3://{bucket}/{key}"

    def put_fileobj(self, bucket: str, key: str, fileobj: BinaryIO) -> str:
        self.ensure_bucket(bucket)
        self._client.upload_fileobj(fileobj, bucket, key)
        return f"s3://{bucket}/{key}"
