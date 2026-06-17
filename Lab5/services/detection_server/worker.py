from __future__ import annotations

import json
import logging

import cv2
import numpy as np

from shared.config import get_settings
from shared.events import (
    annotated_object_key,
    build_detection_event,
    detection_object_key,
)
from shared.logging import setup_logging
from shared.minio_client import create_minio_client, download_bytes, upload_bytes
from services.detection_server.consumer import create_frame_consumer
from services.detection_server.detector import PersonDetector
from services.detection_server.producer import DetectionEventProducer


def _decode_image(image_bytes: bytes):
    array = np.frombuffer(image_bytes, dtype=np.uint8)
    frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
    if frame is None:
        raise RuntimeError("Cannot decode frame image")
    return frame


def _encode_jpeg(frame) -> bytes:
    ok, buffer = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
    if not ok:
        raise RuntimeError("Cannot encode annotated frame")
    return buffer.tobytes()


def process_frame_event(event, *, detector, minio_client, producer, settings) -> None:
    run_id = event["run_id"]
    camera_id = event["camera_id"]
    frame_id = event["frame_id"]
    input_object_key = event["object_key"]

    frame_bytes = download_bytes(
        client=minio_client,
        bucket=event.get("bucket", settings.minio_bucket),
        object_key=input_object_key,
    )
    frame = _decode_image(frame_bytes)
    boxes, annotated = detector.detect(frame)

    detection_key = detection_object_key(run_id, camera_id, frame_id)
    annotated_key = annotated_object_key(run_id, camera_id, frame_id)

    detection_event = build_detection_event(
        run_id=run_id,
        camera_id=camera_id,
        frame_id=frame_id,
        timestamp_ms=event["timestamp_ms"],
        person_count=len(boxes),
        boxes=boxes,
        model_name=settings.model_name,
        input_object_key=input_object_key,
        detection_object_key_value=detection_key,
        annotated_object_key_value=annotated_key if settings.save_annotated_frames else "",
    )

    upload_bytes(
        client=minio_client,
        bucket=settings.minio_bucket,
        object_key=detection_key,
        data=json.dumps(detection_event, ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
    )

    if settings.save_annotated_frames:
        upload_bytes(
            client=minio_client,
            bucket=settings.minio_bucket,
            object_key=annotated_key,
            data=_encode_jpeg(annotated),
            content_type="image/jpeg",
        )

    producer.send(detection_event)
    logging.info(
        "Processed frame run_id=%s frame_id=%s people=%s",
        run_id,
        frame_id,
        len(boxes),
    )


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logging.info("Starting Detection Worker")

    minio_client = create_minio_client(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    detector = PersonDetector(settings.model_name, settings.model_device, settings.model_image_size)
    consumer = create_frame_consumer(settings.kafka_bootstrap_servers, settings.frame_topic)
    producer = DetectionEventProducer(settings.kafka_bootstrap_servers, settings.detection_topic)

    try:
        for message in consumer:
            try:
                process_frame_event(
                    message.value,
                    detector=detector,
                    minio_client=minio_client,
                    producer=producer,
                    settings=settings,
                )
                consumer.commit()
            except Exception:
                logging.exception("Failed to process frame event: %s", message.value)
                consumer.commit()
    finally:
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
