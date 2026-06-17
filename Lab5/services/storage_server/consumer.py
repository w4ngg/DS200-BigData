from __future__ import annotations

import logging
import threading

from shared.config import Settings
from shared.kafka_client import create_json_consumer
from services.storage_server.repository import MongoRepository


class DetectionConsumerThread(threading.Thread):
    def __init__(self, *, settings: Settings, repository: MongoRepository) -> None:
        super().__init__(name="detection-consumer", daemon=True)
        self.settings = settings
        self.repository = repository
        self._stop_event = threading.Event()

    def run(self) -> None:
        consumer = create_json_consumer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            topics=[self.settings.detection_topic],
            group_id="storage-server",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        logging.info("Storage consumer started on topic %s", self.settings.detection_topic)
        try:
            for message in consumer:
                if self._stop_event.is_set():
                    break
                try:
                    self.repository.upsert_detection(message.value)
                    consumer.commit()
                    logging.info(
                        "Stored detection run_id=%s frame_id=%s",
                        message.value.get("run_id"),
                        message.value.get("frame_id"),
                    )
                except Exception:
                    logging.exception("Failed to store detection event: %s", message.value)
        finally:
            consumer.close()

    def stop(self) -> None:
        self._stop_event.set()

