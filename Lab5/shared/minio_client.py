from __future__ import annotations

from io import BytesIO
from typing import Optional

from minio import Minio


def create_minio_client(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool,
) -> Minio:
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_bytes(
    *,
    client: Minio,
    bucket: str,
    object_key: str,
    data: bytes,
    content_type: str,
) -> None:
    ensure_bucket(client, bucket)
    client.put_object(
        bucket,
        object_key,
        BytesIO(data),
        length=len(data),
        content_type=content_type,
    )


def download_bytes(*, client: Minio, bucket: str, object_key: str) -> bytes:
    response = client.get_object(bucket, object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def object_exists(*, client: Minio, bucket: str, object_key: str) -> bool:
    try:
        client.stat_object(bucket, object_key)
        return True
    except Exception:
        return False


def guess_content_type(object_key: str, default: Optional[str] = None) -> str:
    if object_key.endswith(".jpg") or object_key.endswith(".jpeg"):
        return "image/jpeg"
    if object_key.endswith(".json"):
        return "application/json"
    return default or "application/octet-stream"

