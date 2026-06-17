from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, Optional

import cv2

from shared.config import Settings
from shared.events import build_frame_event
from shared.minio_client import create_minio_client
from services.camera_server.frame_store import FrameStore
from services.camera_server.producer import FrameEventProducer

ProgressCallback = Callable[[Dict], None]


def _resize_frame(frame, resize_width: int):
    height, width = frame.shape[:2]
    if resize_width <= 0 or width <= resize_width:
        return frame
    resize_height = int(height * resize_width / width)
    return cv2.resize(frame, (resize_width, resize_height))


def ingest_video(
    *,
    video_path: Path,
    run_id: str,
    camera_id: str,
    source_name: str,
    sample_fps: float,
    resize_width: int,
    max_frames: Optional[int],
    settings: Settings,
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict:
    minio_client = create_minio_client(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    frame_store = FrameStore(minio_client, settings.minio_bucket)
    producer = FrameEventProducer(settings.kafka_bootstrap_servers, settings.frame_topic)

    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    video_fps = capture.get(cv2.CAP_PROP_FPS) or sample_fps or 1.0
    total_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    frame_interval = max(1, int(round(video_fps / sample_fps))) if sample_fps > 0 else 1

    published_frames = 0
    frame_id = 0

    logging.info(
        "Start ingesting run_id=%s source=%s fps=%s sample_fps=%s interval=%s",
        run_id,
        source_name,
        video_fps,
        sample_fps,
        frame_interval,
    )

    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                break

            if frame_id % frame_interval != 0:
                frame_id += 1
                continue

            sampled = _resize_frame(frame, resize_width)
            height, width = sampled.shape[:2]
            encoded, buffer = cv2.imencode(".jpg", sampled, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            if not encoded:
                logging.warning("Cannot encode frame run_id=%s frame_id=%s", run_id, frame_id)
                frame_id += 1
                continue

            object_key = frame_store.save_frame(
                run_id=run_id,
                camera_id=camera_id,
                frame_id=frame_id,
                jpeg_bytes=buffer.tobytes(),
            )
            timestamp_ms = capture.get(cv2.CAP_PROP_POS_MSEC)
            if timestamp_ms <= 0:
                timestamp_ms = (frame_id / video_fps) * 1000

            event = build_frame_event(
                run_id=run_id,
                camera_id=camera_id,
                frame_id=frame_id,
                source_type="upload",
                source_name=source_name,
                timestamp_ms=timestamp_ms,
                width=width,
                height=height,
                bucket=settings.minio_bucket,
                object_key=object_key,
            )
            producer.send(event)
            published_frames += 1

            if progress_callback:
                progress_callback(
                    {
                        "published_frames": published_frames,
                        "last_frame_id": frame_id,
                        "total_frames": total_frames,
                    }
                )

            if max_frames and published_frames >= max_frames:
                break

            frame_id += 1
    finally:
        capture.release()
        try:
            producer.close()
        except Exception:
            logging.exception("Kafka producer close failed after publishing %s frames", published_frames)

    if published_frames == 0:
        raise RuntimeError(
            "No frames were extracted from the video. "
            "The file may use an unsupported codec or OpenCV could not decode it."
        )

    result = {
        "run_id": run_id,
        "camera_id": camera_id,
        "source_name": source_name,
        "video_fps": video_fps,
        "sample_fps": sample_fps,
        "resize_width": resize_width,
        "total_frames": total_frames,
        "sampled_frames": published_frames,
    }
    logging.info("Completed ingestion: %s", result)
    return result
