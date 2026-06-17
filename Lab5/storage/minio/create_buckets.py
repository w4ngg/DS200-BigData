from __future__ import annotations

import logging

from shared.config import get_settings
from shared.logging import setup_logging
from shared.minio_client import create_minio_client, ensure_bucket


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    client = create_minio_client(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    ensure_bucket(client, settings.minio_bucket)
    logging.info("MinIO bucket is ready: %s", settings.minio_bucket)


if __name__ == "__main__":
    main()

