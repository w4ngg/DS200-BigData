from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests


class ApiClient:
    def __init__(self) -> None:
        self.camera_url = os.getenv("CAMERA_SERVER_URL", "http://localhost:8001").rstrip("/")
        self.storage_url = os.getenv("STORAGE_SERVER_URL", "http://localhost:8003").rstrip("/")

    def start_run(
        self,
        *,
        filename: str,
        file_bytes: bytes,
        camera_id: str,
        sample_fps: float,
        resize_width: int,
        save_annotated_frames: bool,
        max_frames: Optional[int],
    ) -> Dict[str, Any]:
        files = {"video": (filename, file_bytes, "application/octet-stream")}
        data: Dict[str, Any] = {
            "camera_id": camera_id,
            "sample_fps": str(sample_fps),
            "resize_width": str(resize_width),
            "save_annotated_frames": str(save_annotated_frames).lower(),
        }
        if max_frames:
            data["max_frames"] = str(max_frames)

        response = requests.post(f"{self.camera_url}/runs", files=files, data=data, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_camera_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        response = requests.get(f"{self.camera_url}/runs/{run_id}", timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_storage_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        response = requests.get(f"{self.storage_url}/runs/{run_id}", timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_stats(self, run_id: str, bucket: str = "frame") -> Optional[Dict[str, Any]]:
        response = requests.get(f"{self.storage_url}/runs/{run_id}/stats", params={"bucket": bucket}, timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_detections(self, run_id: str, limit: int = 50) -> Optional[Dict[str, Any]]:
        response = requests.get(
            f"{self.storage_url}/runs/{run_id}/detections",
            params={"limit": limit, "offset": 0},
            timeout=10,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_annotated_frame(self, run_id: str, frame_id: int) -> Optional[bytes]:
        response = requests.get(
            f"{self.storage_url}/runs/{run_id}/frames/{frame_id}/annotated",
            timeout=20,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.content

