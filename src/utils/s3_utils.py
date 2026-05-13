from __future__ import annotations

import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from src.config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_DEFAULT_REGION,
    AWS_SECRET_ACCESS_KEY,
    ENABLE_S3_MIRROR,
    PROJECT_ROOT,
    S3_BUCKET_NAME,
    S3_ENDPOINT_URL,
)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def ensure_data_lake_bucket(bucket_name: str = S3_BUCKET_NAME) -> str:
    if not ENABLE_S3_MIRROR:
        return bucket_name
    client = get_s3_client()
    existing_buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in existing_buckets:
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": AWS_DEFAULT_REGION},
        )
    return bucket_name


def _relative_key(path: Path) -> str:
    return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()


def upload_json_to_s3(path: Path, payload: dict, bucket_name: str = S3_BUCKET_NAME) -> str | None:
    if not ENABLE_S3_MIRROR:
        return None
    ensure_data_lake_bucket(bucket_name)
    get_s3_client().put_object(
        Bucket=bucket_name,
        Key=_relative_key(path),
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket_name}/{_relative_key(path)}"


def upload_file_to_s3(path: Path, bucket_name: str = S3_BUCKET_NAME) -> str | None:
    if not ENABLE_S3_MIRROR:
        return None
    ensure_data_lake_bucket(bucket_name)
    key = _relative_key(path)
    get_s3_client().upload_file(str(path), bucket_name, key)
    return f"s3://{bucket_name}/{key}"


def bucket_exists(bucket_name: str = S3_BUCKET_NAME) -> bool:
    try:
        get_s3_client().head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False
