from __future__ import annotations

from shared.events import frame_object_key
from shared.minio_client import upload_bytes


class FrameStore:
    def __init__(self, client, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def save_frame(self, *, run_id: str, camera_id: str, frame_id: int, jpeg_bytes: bytes) -> str:
        object_key = frame_object_key(run_id, camera_id, frame_id)
        upload_bytes(
            client=self.client,
            bucket=self.bucket,
            object_key=object_key,
            data=jpeg_bytes,
            content_type="image/jpeg",
        )
        return object_key

