from __future__ import annotations

from shared.kafka_client import create_json_consumer


def create_frame_consumer(bootstrap_servers: str, topic: str):
    return create_json_consumer(
        bootstrap_servers=bootstrap_servers,
        topics=[topic],
        group_id="detection-server",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

