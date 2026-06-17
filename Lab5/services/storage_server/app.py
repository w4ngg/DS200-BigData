from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException

from shared.config import get_settings
from shared.logging import setup_logging
from services.storage_server.consumer import DetectionConsumerThread
from services.storage_server.repository import MongoRepository
from services.storage_server.routes import router

settings = get_settings()
setup_logging(settings.log_level)

app = FastAPI(title="Storage Server", version="0.1.0")
app.include_router(router)


@app.on_event("startup")
def startup() -> None:
    repository = MongoRepository(settings.mongodb_uri, settings.mongodb_database)
    repository.ping()
    repository.ensure_indexes()
    app.state.repository = repository

    consumer = DetectionConsumerThread(settings=settings, repository=repository)
    consumer.start()
    app.state.consumer = consumer
    logging.info("Storage Server started")


@app.on_event("shutdown")
def shutdown() -> None:
    consumer = getattr(app.state, "consumer", None)
    if consumer:
        consumer.stop()


@app.get("/health")
def health():
    repository = getattr(app.state, "repository", None)
    if repository is None:
        raise HTTPException(status_code=503, detail="Repository is not ready")
    try:
        repository.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB Atlas error: {exc}") from exc
    return {"service": "storage-server", "status": "ok", "mongodb_atlas": "ok"}

