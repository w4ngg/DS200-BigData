# Data quality

## Kiểm tra đầu vào

- Video upload phải đọc được bằng OpenCV.
- `sample_fps` phải lớn hơn 0.
- `resize_width` phải lớn hơn 0.
- `max_frames` chỉ dùng để giới hạn demo, không dùng để đánh giá toàn bộ video dài.

## Kiểm tra frame event

Frame event cần có:

- `event_id`
- `run_id`
- `camera_id`
- `frame_id`
- `timestamp_ms`
- `bucket`
- `object_key`

`frame_id` và `timestamp_ms` không được âm.

## Kiểm tra detection event

Detection event cần có:

- `run_id`
- `frame_id`
- `person_count`
- `boxes`
- `model_name`
- `input_object_key`
- `detection_object_key`
- `annotated_object_key`

Quy tắc:

- `person_count == len(boxes)`
- `confidence` nằm trong khoảng 0 đến 1
- `x1 < x2`
- `y1 < y2`
- ghi MongoDB Atlas theo `(run_id, frame_id)` bằng upsert để tránh duplicate

## Chất lượng vận hành

- Nếu lỗi một frame, log lỗi theo `run_id` và `frame_id`.
- Nếu Kafka hoặc MinIO lỗi, service nên fail để Docker restart.
- Nếu MongoDB Atlas lỗi, kiểm tra internet, credential và Network Access/IP allowlist.
- Với video dài, giảm `sample_fps` hoặc dùng `max_frames` để tránh đầy MinIO và quá tải CPU.

