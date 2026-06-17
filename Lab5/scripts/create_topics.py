from __future__ import annotations

import logging
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from shared.config import get_settings
from shared.logging import setup_logging


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logging.info("Connecting to Kafka at %s", settings.kafka_bootstrap_servers)

    admin = None
    for attempt in range(1, 11):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id="people-counting-topic-init",
            )
            break
        except Exception as exc:
            logging.warning("Kafka is not ready yet, attempt %s/10: %s", attempt, exc)
            time.sleep(3)

    if admin is None:
        raise RuntimeError("Cannot connect to Kafka")

    topics = [
        NewTopic(settings.frame_topic, num_partitions=1, replication_factor=1),
        NewTopic(settings.detection_topic, num_partitions=1, replication_factor=1),
    ]

    try:
        admin.create_topics(topics)
        logging.info("Kafka topics created")
    except TopicAlreadyExistsError:
        logging.info("Kafka topics already exist")
    finally:
        admin.close()


if __name__ == "__main__":
    main()

