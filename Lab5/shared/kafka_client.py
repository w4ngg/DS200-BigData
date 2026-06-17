from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from kafka import KafkaConsumer, KafkaProducer


def create_json_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )


def create_json_consumer(
    *,
    bootstrap_servers: str,
    topics: Iterable[str],
    group_id: str,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    consumer_timeout_ms: Optional[int] = None,
) -> KafkaConsumer:
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "group_id": group_id,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "key_deserializer": lambda key: key.decode("utf-8") if key else None,
        "value_deserializer": lambda value: json.loads(value.decode("utf-8")),
        "max_poll_records": 1,
    }
    if consumer_timeout_ms is not None:
        kwargs["consumer_timeout_ms"] = consumer_timeout_ms
    return KafkaConsumer(*topics, **kwargs)

