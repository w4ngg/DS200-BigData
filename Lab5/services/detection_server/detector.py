from __future__ import annotations

from typing import Any, Dict, List, Tuple

import cv2
import numpy as np
from ultralytics import YOLO


class PersonDetector:
    def __init__(self, model_name: str, device: str, image_size: int) -> None:
        self.model_name = model_name
        self.device = device
        self.image_size = image_size
        self.model = YOLO(model_name)

    def detect(self, frame: np.ndarray) -> Tuple[List[Dict[str, Any]], np.ndarray]:
        results = self.model(
            frame,
            imgsz=self.image_size,
            device=self.device,
            verbose=False,
        )
        boxes: List[Dict[str, Any]] = []
        annotated = frame.copy()

        if not results:
            return boxes, annotated

        result = results[0]
        names = result.names
        if result.boxes is None:
            return boxes, annotated

        for box in result.boxes:
            class_id = int(box.cls[0].item())
            class_name = names.get(class_id, str(class_id))
            if class_name != "person":
                continue

            confidence = float(box.conf[0].item())
            x1, y1, x2, y2 = [float(value) for value in box.xyxy[0].tolist()]
            person_box = {
                "class_name": "person",
                "confidence": confidence,
                "x1": x1,
                "y1": y1,
                "x2": x2,
                "y2": y2,
            }
            boxes.append(person_box)
            self._draw_box(annotated, person_box)

        return boxes, annotated

    @staticmethod
    def _draw_box(frame: np.ndarray, box: Dict[str, Any]) -> None:
        x1, y1, x2, y2 = (int(box["x1"]), int(box["y1"]), int(box["x2"]), int(box["y2"]))
        label = f"person {box['confidence']:.2f}"
        cv2.rectangle(frame, (x1, y1), (x2, y2), (40, 220, 40), 2)
        cv2.putText(
            frame,
            label,
            (x1, max(20, y1 - 8)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (40, 220, 40),
            2,
            cv2.LINE_AA,
        )

