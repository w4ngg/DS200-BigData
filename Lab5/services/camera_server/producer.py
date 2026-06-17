from __future__ import annotations

from typing import Any, Dict

from shared.events import event_key
from shared.kafka_client import create_json_producer


class FrameEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = create_json_producer(bootstrap_servers)

    def send(self, event: Dict[str, Any]) -> None:
        key = event_key(event["run_id"], event["frame_id"])
        future = self.producer.send(self.topic, key=key, value=event)
        future.get(timeout=30)

    def close(self) -> None:
        self.producer.flush(timeout=30)
        self.producer.close(timeout=30)

