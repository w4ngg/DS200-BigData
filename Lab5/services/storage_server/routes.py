from __future__ import annotations

from io import BytesIO
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from shared.config import get_settings
from shared.minio_client import create_minio_client, download_bytes
from services.storage_server.repository import MongoRepository

router = APIRouter()


def _repo(request: Request) -> MongoRepository:
    return request.app.state.repository


@router.get("/runs")
def list_runs(request: Request, limit: int = 20, offset: int = 0):
    return _repo(request).list_runs(limit=limit, offset=offset)


@router.get("/runs/{run_id}")
def get_run(request: Request, run_id: str):
    run = _repo(request).get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@router.get("/runs/{run_id}/detections")
def list_detections(
    request: Request,
    run_id: str,
    limit: int = 50,
    offset: int = 0,
    from_frame: Optional[int] = Query(None),
    to_frame: Optional[int] = Query(None),
    min_people_count: Optional[int] = Query(None),
):
    return _repo(request).list_detections(
        run_id=run_id,
        limit=limit,
        offset=offset,
        from_frame=from_frame,
        to_frame=to_frame,
        min_people_count=min_people_count,
    )


@router.get("/runs/{run_id}/detections/{frame_id}")
def get_detection(request: Request, run_id: str, frame_id: int):
    detection = _repo(request).get_detection(run_id, frame_id)
    if not detection:
        raise HTTPException(status_code=404, detail="Detection not found")
    return detection


@router.get("/runs/{run_id}/stats")
def get_stats(request: Request, run_id: str, bucket: str = "frame"):
    if bucket not in {"frame", "second", "minute"}:
        raise HTTPException(status_code=400, detail="bucket must be frame, second, or minute")
    if not _repo(request).get_run(run_id):
        raise HTTPException(status_code=404, detail="Run not found")
    return _repo(request).stats(run_id, bucket)


@router.get("/runs/{run_id}/frames/{frame_id}/annotated")
def get_annotated_frame(request: Request, run_id: str, frame_id: int):
    detection = _repo(request).get_detection(run_id, frame_id)
    if not detection or not detection.get("annotated_object_key"):
        raise HTTPException(status_code=404, detail="Annotated frame not found")

    settings = get_settings()
    minio_client = create_minio_client(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    image = download_bytes(
        client=minio_client,
        bucket=settings.minio_bucket,
        object_key=detection["annotated_object_key"],
    )
    return StreamingResponse(BytesIO(image), media_type="image/jpeg")


@router.post("/internal/runs")
def upsert_internal_run(request: Request, run: dict):
    return _repo(request).upsert_run(run)


@router.post("/internal/runs/{run_id}/status")
def update_internal_run_status(request: Request, run_id: str, payload: dict):
    run = _repo(request).update_run_status(run_id, payload)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run

