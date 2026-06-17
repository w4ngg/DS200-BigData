from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _get_optional_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return int(value)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    frame_topic: str
    detection_topic: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool

    mongodb_uri: str
    mongodb_database: str

    model_name: str
    model_device: str
    model_image_size: int

    sample_fps: float
    resize_width: int
    save_annotated_frames: bool
    max_frames: Optional[int]

    camera_server_url: str
    storage_server_url: str
    upload_dir: str
    log_level: str


def get_settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        frame_topic=os.getenv("FRAME_TOPIC", "camera.frames.raw"),
        detection_topic=os.getenv("DETECTION_TOPIC", "camera.detections.raw"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.getenv("MINIO_BUCKET", "people-counting"),
        minio_secure=_get_bool("MINIO_SECURE", False),
        mongodb_uri=os.getenv("MONGODB_URI", ""),
        mongodb_database=os.getenv("MONGODB_DATABASE", "people_counting"),
        model_name=os.getenv("MODEL_NAME", "yolov8n.pt"),
        model_device=os.getenv("MODEL_DEVICE", "cpu"),
        model_image_size=_get_int("MODEL_IMAGE_SIZE", 640),
        sample_fps=_get_float("SAMPLE_FPS", 1.0),
        resize_width=_get_int("RESIZE_WIDTH", 640),
        save_annotated_frames=_get_bool("SAVE_ANNOTATED_FRAMES", True),
        max_frames=_get_optional_int("MAX_FRAMES"),
        camera_server_url=os.getenv("CAMERA_SERVER_URL", "http://localhost:8001"),
        storage_server_url=os.getenv("STORAGE_SERVER_URL", "http://localhost:8003"),
        upload_dir=os.getenv("UPLOAD_DIR", "/tmp/people-counting/uploads"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )

