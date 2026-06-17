from __future__ import annotations

import logging

from pymongo import ASCENDING, MongoClient

from shared.config import get_settings
from shared.logging import setup_logging


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    if not settings.mongodb_uri:
        raise RuntimeError("MONGODB_URI is required for MongoDB Atlas")

    client = MongoClient(settings.mongodb_uri)
    db = client[settings.mongodb_database]

    db.runs.create_index([("run_id", ASCENDING)], unique=True)
    db.runs.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    db.detections.create_index([("run_id", ASCENDING), ("frame_id", ASCENDING)], unique=True)
    db.detections.create_index([("run_id", ASCENDING), ("timestamp_ms", ASCENDING)])

    logging.info("MongoDB indexes are ready in database: %s", settings.mongodb_database)


if __name__ == "__main__":
    main()

