from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_event_id() -> str:
    return str(uuid4())


def event_key(run_id: str, frame_id: int) -> str:
    return f"{run_id}:{frame_id}"


def frame_object_key(run_id: str, camera_id: str, frame_id: int) -> str:
    return f"bronze/frames/run_id={run_id}/camera_id={camera_id}/frame_{frame_id:06d}.jpg"


def detection_object_key(run_id: str, camera_id: str, frame_id: int) -> str:
    return f"silver/detections/run_id={run_id}/camera_id={camera_id}/frame_{frame_id:06d}.json"


def annotated_object_key(run_id: str, camera_id: str, frame_id: int) -> str:
    return f"silver/annotated_frames/run_id={run_id}/camera_id={camera_id}/frame_{frame_id:06d}.jpg"


def build_frame_event(
    *,
    run_id: str,
    camera_id: str,
    frame_id: int,
    source_type: str,
    source_name: str,
    timestamp_ms: float,
    width: int,
    height: int,
    bucket: str,
    object_key: str,
) -> Dict[str, Any]:
    return {
        "event_id": new_event_id(),
        "run_id": run_id,
        "camera_id": camera_id,
        "frame_id": frame_id,
        "source_type": source_type,
        "source_name": source_name,
        "timestamp_ms": timestamp_ms,
        "width": width,
        "height": height,
        "bucket": bucket,
        "object_key": object_key,
        "created_at": utc_now_iso(),
    }


def build_detection_event(
    *,
    run_id: str,
    camera_id: str,
    frame_id: int,
    timestamp_ms: float,
    person_count: int,
    boxes: list[dict[str, Any]],
    model_name: str,
    input_object_key: str,
    detection_object_key_value: str,
    annotated_object_key_value: str,
) -> Dict[str, Any]:
    return {
        "event_id": new_event_id(),
        "run_id": run_id,
        "camera_id": camera_id,
        "frame_id": frame_id,
        "timestamp_ms": timestamp_ms,
        "person_count": person_count,
        "boxes": boxes,
        "model_name": model_name,
        "input_object_key": input_object_key,
        "detection_object_key": detection_object_key_value,
        "annotated_object_key": annotated_object_key_value,
        "processed_at": utc_now_iso(),
    }

