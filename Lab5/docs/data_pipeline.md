# Data pipeline

Pipeline xử lý theo mô hình Bronze/Silver/Gold:

```text
Upload video
  -> Camera Server
  -> MinIO Bronze raw frames
  -> Kafka camera.frames.raw
  -> Detection Worker
  -> MinIO Silver detection JSON + annotated frames
  -> Kafka camera.detections.raw
  -> Storage Server
  -> MongoDB Atlas serving collections
  -> Dashboard
```

## Bronze

Bronze lưu frame JPEG đã được sample từ video:

```text
bronze/frames/run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.jpg
```

Kafka topic `camera.frames.raw` chỉ chứa metadata và `object_key`, không chứa ảnh trực tiếp.

## Silver

Silver lưu kết quả sau nhận diện:

```text
silver/detections/run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.json
silver/annotated_frames/run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.jpg
```

Kafka topic `camera.detections.raw` chứa số người, bounding boxes và đường dẫn object trên MinIO.

## Gold

Gold dành cho dữ liệu tổng hợp:

```text
gold/people_count_by_minute/run_id=<run_id>/part-00000.json
```

Phase đầu dùng `pipelines/python/aggregate_people_count.py`. Spark để optional vì máy local chỉ có 4 cores và 8 GB RAM.

