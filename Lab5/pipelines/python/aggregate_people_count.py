from __future__ import annotations

import json
import logging
from collections import defaultdict

from pymongo import MongoClient

from shared.config import get_settings
from shared.logging import setup_logging
from shared.minio_client import create_minio_client, upload_bytes


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    if not settings.mongodb_uri:
        raise RuntimeError("MONGODB_URI is required")

    mongo = MongoClient(settings.mongodb_uri)
    db = mongo[settings.mongodb_database]
    minio = create_minio_client(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )

    grouped: dict[str, list[dict]] = defaultdict(list)
    for doc in db.detections.find({}, {"_id": 0}).sort([("run_id", 1), ("timestamp_ms", 1)]):
        minute = int(doc.get("timestamp_ms", 0) // 60000)
        grouped[doc["run_id"]].append(
            {
                "minute": minute,
                "frame_id": doc["frame_id"],
                "timestamp_ms": doc["timestamp_ms"],
                "person_count": doc["person_count"],
            }
        )

    for run_id, rows in grouped.items():
        by_minute: dict[int, list[int]] = defaultdict(list)
        for row in rows:
            by_minute[row["minute"]].append(row["person_count"])

        output = [
            {
                "run_id": run_id,
                "minute": minute,
                "max_people_count": max(values),
                "avg_people_count": sum(values) / len(values),
                "samples": len(values),
            }
            for minute, values in sorted(by_minute.items())
        ]
        payload = json.dumps(output, ensure_ascii=False, indent=2).encode("utf-8")
        object_key = f"gold/people_count_by_minute/run_id={run_id}/part-00000.json"
        upload_bytes(
            client=minio,
            bucket=settings.minio_bucket,
            object_key=object_key,
            data=payload,
            content_type="application/json",
        )
        logging.info("Wrote %s", object_key)


if __name__ == "__main__":
    main()
