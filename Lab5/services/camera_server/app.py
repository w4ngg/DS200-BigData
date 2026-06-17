from __future__ import annotations

import logging
import shutil
from pathlib import Path
from threading import Lock
from typing import Dict, Optional
from uuid import uuid4

import requests
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile

from shared.config import get_settings
from shared.events import utc_now_iso
from shared.logging import setup_logging
from services.camera_server.ingestion import ingest_video

settings = get_settings()
setup_logging(settings.log_level)

app = FastAPI(title="Camera Ingestion Server", version="0.1.0")

RUNS: Dict[str, Dict] = {}
RUN_LOCK = Lock()
ACTIVE_STATUSES = {"queued", "ingesting"}


def _active_run_exists() -> bool:
    return any(run.get("status") in ACTIVE_STATUSES for run in RUNS.values())


def _notify_storage(path: str, payload: Dict) -> None:
    url = f"{settings.storage_server_url.rstrip('/')}{path}"
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code >= 400:
            logging.warning("Storage notification failed %s: %s", url, response.text)
    except Exception as exc:
        logging.warning("Storage notification error %s: %s", url, exc)


def _update_run(run_id: str, **fields) -> None:
    with RUN_LOCK:
        if run_id in RUNS:
            RUNS[run_id].update(fields)
            RUNS[run_id]["updated_at"] = utc_now_iso()


def _run_ingestion_job(
    *,
    run_id: str,
    video_path: Path,
    camera_id: str,
    source_name: str,
    sample_fps: float,
    resize_width: int,
    max_frames: Optional[int],
) -> None:
    _update_run(run_id, status="ingesting")
    _notify_storage(
        "/internal/runs",
        {
            "run_id": run_id,
            "camera_id": camera_id,
            "source_type": "upload",
            "source_name": source_name,
            "status": "ingesting",
            "sample_fps": sample_fps,
            "resize_width": resize_width,
            "created_at": RUNS[run_id]["created_at"],
        },
    )

    def progress(fields: Dict) -> None:
        _update_run(run_id, **fields)

    try:
        result = ingest_video(
            video_path=video_path,
            run_id=run_id,
            camera_id=camera_id,
            source_name=source_name,
            sample_fps=sample_fps,
            resize_width=resize_width,
            max_frames=max_frames,
            settings=settings,
            progress_callback=progress,
        )
        result_for_update = dict(result)
        result_for_update.pop("run_id", None)
        _update_run(run_id, status="ingestion_completed", **result_for_update)
        _notify_storage(
            f"/internal/runs/{run_id}/status",
            {
                "status": "ingestion_completed",
                "sampled_frames": result["sampled_frames"],
                "total_frames": result["total_frames"],
                "video_fps": result["video_fps"],
                "updated_at": utc_now_iso(),
            },
        )
    except Exception as exc:
        logging.exception("Ingestion failed for run_id=%s", run_id)
        _update_run(run_id, status="failed", error=str(exc))
        _notify_storage(
            f"/internal/runs/{run_id}/status",
            {
                "status": "failed",
                "error": str(exc),
                "updated_at": utc_now_iso(),
            },
        )


@app.get("/health")
def health() -> Dict[str, str]:
    return {"service": "camera-server", "status": "ok"}


@app.post("/runs", status_code=201)
async def create_run(
    background_tasks: BackgroundTasks,
    video: UploadFile = File(...),
    camera_id: str = Form("camera_001"),
    sample_fps: Optional[float] = Form(None),
    resize_width: Optional[int] = Form(None),
    save_annotated_frames: Optional[bool] = Form(None),
    max_frames: Optional[int] = Form(None),
) -> Dict:
    del save_annotated_frames

    with RUN_LOCK:
        if _active_run_exists():
            raise HTTPException(status_code=409, detail="Another run is already ingesting")

    run_id = str(uuid4())
    created_at = utc_now_iso()
    upload_dir = Path(settings.upload_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(video.filename or "uploaded_video.mp4").name
    video_path = upload_dir / f"{run_id}_{safe_name}"

    with video_path.open("wb") as output:
        shutil.copyfileobj(video.file, output)

    resolved_sample_fps = sample_fps or settings.sample_fps
    resolved_resize_width = resize_width or settings.resize_width
    resolved_max_frames = max_frames if max_frames is not None else settings.max_frames

    run = {
        "run_id": run_id,
        "camera_id": camera_id,
        "source_type": "upload",
        "source_name": safe_name,
        "status": "queued",
        "sample_fps": resolved_sample_fps,
        "resize_width": resolved_resize_width,
        "max_frames": resolved_max_frames,
        "created_at": created_at,
        "updated_at": created_at,
    }
    with RUN_LOCK:
        RUNS[run_id] = run

    background_tasks.add_task(
        _run_ingestion_job,
        run_id=run_id,
        video_path=video_path,
        camera_id=camera_id,
        source_name=safe_name,
        sample_fps=resolved_sample_fps,
        resize_width=resolved_resize_width,
        max_frames=resolved_max_frames,
    )
    return run


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> Dict:
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run
