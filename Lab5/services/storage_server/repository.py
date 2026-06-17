from __future__ import annotations

from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from shared.events import utc_now_iso


def _strip_id(document: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if document is None:
        return None
    document = dict(document)
    document.pop("_id", None)
    return document


class MongoRepository:
    def __init__(self, uri: str, database: str) -> None:
        if not uri:
            raise RuntimeError("MONGODB_URI is required for MongoDB Atlas")
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.runs: Collection = self.db.runs
        self.detections: Collection = self.db.detections

    def ping(self) -> None:
        self.client.admin.command("ping")

    def ensure_indexes(self) -> None:
        self.runs.create_index([("run_id", ASCENDING)], unique=True)
        self.runs.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
        self.detections.create_index([("run_id", ASCENDING), ("frame_id", ASCENDING)], unique=True)
        self.detections.create_index([("run_id", ASCENDING), ("timestamp_ms", ASCENDING)])

    def upsert_run(self, run: Dict[str, Any]) -> Dict[str, Any]:
        now = utc_now_iso()
        payload = dict(run)
        payload.setdefault("created_at", now)
        payload["updated_at"] = now
        set_payload = dict(payload)
        created_at = set_payload.pop("created_at")
        self.runs.update_one(
            {"run_id": payload["run_id"]},
            {"$setOnInsert": {"created_at": created_at}, "$set": set_payload},
            upsert=True,
        )
        return self.get_run(payload["run_id"]) or payload

    def update_run_status(self, run_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = dict(fields)
        payload["run_id"] = run_id
        payload["updated_at"] = payload.get("updated_at") or utc_now_iso()
        self.runs.update_one({"run_id": run_id}, {"$set": payload}, upsert=True)
        self._mark_completed_if_ready(run_id)
        return self.get_run(run_id)

    def upsert_detection(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(event)
        payload["updated_at"] = utc_now_iso()
        self.detections.update_one(
            {"run_id": payload["run_id"], "frame_id": payload["frame_id"]},
            {"$set": payload},
            upsert=True,
        )

        processed_frames = self.detections.count_documents({"run_id": payload["run_id"]})
        run_set = {
            "run_id": payload["run_id"],
            "camera_id": payload["camera_id"],
            "status": "processing",
            "processed_frames": processed_frames,
            "updated_at": utc_now_iso(),
        }
        self.runs.update_one(
            {"run_id": payload["run_id"]},
            {
                "$setOnInsert": {"created_at": payload["processed_at"]},
                "$set": run_set,
                "$max": {"max_people_count": payload["person_count"]},
            },
            upsert=True,
        )
        self._mark_completed_if_ready(payload["run_id"])
        return payload

    def list_runs(self, *, limit: int, offset: int) -> Dict[str, Any]:
        cursor = (
            self.runs.find({}, {"_id": 0})
            .sort("created_at", -1)
            .skip(max(0, offset))
            .limit(max(1, min(limit, 100)))
        )
        return {
            "items": list(cursor),
            "limit": limit,
            "offset": offset,
            "total": self.runs.count_documents({}),
        }

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        run = _strip_id(self.runs.find_one({"run_id": run_id}))
        if run is None:
            return None
        summary = self._summary(run_id)
        run.update(summary)
        return run

    def list_detections(
        self,
        *,
        run_id: str,
        limit: int,
        offset: int,
        from_frame: Optional[int],
        to_frame: Optional[int],
        min_people_count: Optional[int],
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"run_id": run_id}
        if from_frame is not None or to_frame is not None:
            query["frame_id"] = {}
            if from_frame is not None:
                query["frame_id"]["$gte"] = from_frame
            if to_frame is not None:
                query["frame_id"]["$lte"] = to_frame
        if min_people_count is not None:
            query["person_count"] = {"$gte": min_people_count}

        safe_limit = max(1, min(limit, 200))
        cursor = (
            self.detections.find(query, {"_id": 0})
            .sort("frame_id", 1)
            .skip(max(0, offset))
            .limit(safe_limit)
        )
        items = [self._with_annotated_url(item) for item in cursor]
        return {
            "items": items,
            "limit": safe_limit,
            "offset": offset,
            "total": self.detections.count_documents(query),
        }

    def get_detection(self, run_id: str, frame_id: int) -> Optional[Dict[str, Any]]:
        detection = _strip_id(self.detections.find_one({"run_id": run_id, "frame_id": frame_id}))
        if detection is None:
            return None
        return self._with_annotated_url(detection)

    def stats(self, run_id: str, bucket: str) -> Dict[str, Any]:
        detections = list(
            self.detections.find({"run_id": run_id}, {"_id": 0}).sort("timestamp_ms", 1)
        )
        if bucket == "frame":
            items = [
                {
                    "frame_id": item["frame_id"],
                    "timestamp_ms": item["timestamp_ms"],
                    "person_count": item["person_count"],
                }
                for item in detections
            ]
        else:
            divisor = 1000 if bucket == "second" else 60000
            grouped: Dict[int, List[int]] = {}
            for item in detections:
                key = int(item.get("timestamp_ms", 0) // divisor)
                grouped.setdefault(key, []).append(item["person_count"])
            items = [
                {
                    bucket: key,
                    "max_people_count": max(values),
                    "avg_people_count": sum(values) / len(values),
                    "samples": len(values),
                }
                for key, values in sorted(grouped.items())
            ]

        return {"run_id": run_id, "bucket": bucket, "items": items, "summary": self._summary(run_id)}

    def _summary(self, run_id: str) -> Dict[str, Any]:
        docs = list(self.detections.find({"run_id": run_id}, {"person_count": 1, "_id": 0}))
        if not docs:
            return {"processed_frames": 0, "max_people_count": 0, "avg_people_count": 0}
        counts = [doc["person_count"] for doc in docs]
        return {
            "processed_frames": len(counts),
            "max_people_count": max(counts),
            "avg_people_count": sum(counts) / len(counts),
        }

    def _mark_completed_if_ready(self, run_id: str) -> None:
        run = self.runs.find_one({"run_id": run_id}, {"sampled_frames": 1, "processed_frames": 1})
        if not run:
            return
        sampled = run.get("sampled_frames")
        processed = self.detections.count_documents({"run_id": run_id})
        if sampled and processed >= sampled:
            self.runs.update_one(
                {"run_id": run_id},
                {"$set": {"status": "completed", "processed_frames": processed, "updated_at": utc_now_iso()}},
            )
        else:
            self.runs.update_one(
                {"run_id": run_id},
                {"$set": {"processed_frames": processed, "updated_at": utc_now_iso()}},
            )

    @staticmethod
    def _with_annotated_url(detection: Dict[str, Any]) -> Dict[str, Any]:
        detection = dict(detection)
        if detection.get("annotated_object_key"):
            detection["annotated_frame_url"] = (
                f"/runs/{detection['run_id']}/frames/{detection['frame_id']}/annotated"
            )
        return detection
