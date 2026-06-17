# Tài liệu API và event contract

## 1. Tổng quan

Hệ thống có hai nhóm API chính:

- Camera Ingestion API: nhận video upload và bắt đầu một lần xử lý.
- Storage API: cung cấp dữ liệu run, detections, stats và annotated frame cho dashboard.

Các service xử lý bất đồng bộ qua Kafka:

- `camera.frames.raw`: Camera Server publish frame metadata.
- `camera.detections.raw`: Detection Server publish detection metadata.

Base URL đề xuất khi chạy local:

```text
Camera Server:  http://localhost:8001
Storage Server: http://localhost:8003
Dashboard:      http://localhost:8501
MinIO Console:  http://localhost:9001
```

## 2. Quy ước chung

### 2.1 Content type

- API JSON dùng `application/json`.
- Upload video dùng `multipart/form-data`.

### 2.2 Time format

- API trả timestamp theo ISO 8601 UTC, ví dụ `2026-06-17T10:00:00Z`.
- Frame timestamp trong video dùng `timestamp_ms`.

### 2.3 Error response

```json
{
  "error": {
    "code": "VIDEO_READ_FAILED",
    "message": "Cannot read uploaded video",
    "details": {
      "run_id": "uuid"
    }
  }
}
```

HTTP status đề xuất:

- `200 OK`: request thành công.
- `201 Created`: tạo run thành công.
- `400 Bad Request`: input thiếu hoặc sai.
- `404 Not Found`: không tìm thấy run/frame.
- `409 Conflict`: run đang chạy, không cho chạy thêm nếu phase đầu chỉ xử lý một video tại một thời điểm.
- `500 Internal Server Error`: lỗi không mong đợi.
- `503 Service Unavailable`: Kafka/MinIO chưa sẵn sàng hoặc Storage Server chưa kết nối được MongoDB Atlas.

## 3. Camera Ingestion API

### 3.1 Health check

```http
GET /health
```

Response:

```json
{
  "service": "camera-server",
  "status": "ok"
}
```

### 3.2 Tạo run từ video upload

```http
POST /runs
Content-Type: multipart/form-data
```

Form fields:

| Field | Type | Required | Default | Mô tả |
| --- | --- | --- | --- | --- |
| `video` | file | yes | | File video upload |
| `camera_id` | string | no | `camera_001` | ID camera/video source |
| `sample_fps` | number | no | `1` | Số frame lấy mẫu mỗi giây |
| `resize_width` | integer | no | `640` | Chiều rộng frame sau resize |
| `save_annotated_frames` | boolean | no | `true` | Lưu annotated frame cho frame đã xử lý |
| `max_frames` | integer | no | null | Giới hạn số frame để demo nhanh |

Ví dụ request bằng `curl`:

```bash
curl -X POST http://localhost:8001/runs \
  -F "video=@data/sample_videos/demo.mp4" \
  -F "camera_id=camera_001" \
  -F "sample_fps=1" \
  -F "resize_width=640" \
  -F "save_annotated_frames=true"
```

Response `201 Created`:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "source_type": "upload",
  "source_name": "demo.mp4",
  "status": "ingesting",
  "sample_fps": 1,
  "resize_width": 640,
  "save_annotated_frames": true,
  "created_at": "2026-06-17T10:00:00Z"
}
```

Ghi chú:

- Phase đầu chỉ xử lý một video tại một thời điểm. Nếu có run đang chạy, API có thể trả `409 Conflict`.
- Camera Server có thể trả response ngay sau khi tạo run, còn ingestion chạy nền.

### 3.3 Xem trạng thái ingestion của run

```http
GET /runs/{run_id}
```

Response:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "source_name": "demo.mp4",
  "status": "ingesting",
  "video_fps": 30,
  "sample_fps": 1,
  "total_frames": 1800,
  "published_frames": 42,
  "created_at": "2026-06-17T10:00:00Z",
  "updated_at": "2026-06-17T10:00:30Z"
}
```

## 4. Storage API

### 4.1 Health check

```http
GET /health
```

Response:

```json
{
  "service": "storage-server",
  "status": "ok",
  "mongodb_atlas": "ok"
}
```

Ghi chú:

- Storage Server kết nối MongoDB Atlas qua `MONGODB_URI` trong `.env`.
- Nếu health check trả lỗi database, cần kiểm tra Atlas username/password, Network Access/IP allowlist và kết nối internet.

### 4.2 Danh sách runs

```http
GET /runs?limit=20&offset=0
```

Response:

```json
{
  "items": [
    {
      "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
      "camera_id": "camera_001",
      "source_name": "demo.mp4",
      "status": "processing",
      "sample_fps": 1,
      "processed_frames": 80,
      "created_at": "2026-06-17T10:00:00Z",
      "updated_at": "2026-06-17T10:01:00Z"
    }
  ],
  "limit": 20,
  "offset": 0,
  "total": 1
}
```

### 4.3 Chi tiết một run

```http
GET /runs/{run_id}
```

Response:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "source_type": "upload",
  "source_name": "demo.mp4",
  "status": "processing",
  "sample_fps": 1,
  "resize_width": 640,
  "total_frames": 1800,
  "sampled_frames": 60,
  "processed_frames": 42,
  "max_people_count": 5,
  "avg_people_count": 2.4,
  "created_at": "2026-06-17T10:00:00Z",
  "updated_at": "2026-06-17T10:01:00Z"
}
```

### 4.4 Danh sách detections của run

```http
GET /runs/{run_id}/detections?limit=50&offset=0
```

Query params:

| Param | Type | Required | Mô tả |
| --- | --- | --- | --- |
| `limit` | integer | no | Số record trả về |
| `offset` | integer | no | Vị trí bắt đầu |
| `from_frame` | integer | no | Lọc từ frame_id |
| `to_frame` | integer | no | Lọc đến frame_id |
| `min_people_count` | integer | no | Lọc frame có số người tối thiểu |

Response:

```json
{
  "items": [
    {
      "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
      "camera_id": "camera_001",
      "frame_id": 120,
      "timestamp_ms": 4000,
      "person_count": 2,
      "boxes": [
        {
          "class_name": "person",
          "confidence": 0.91,
          "x1": 120,
          "y1": 80,
          "x2": 300,
          "y2": 620
        }
      ],
      "annotated_frame_url": "http://localhost:8003/runs/7c4f5e34-1111-4444-8888-73fb5e93f001/frames/120/annotated",
      "processed_at": "2026-06-17T10:00:01Z"
    }
  ],
  "limit": 50,
  "offset": 0,
  "total": 42
}
```

### 4.5 Chi tiết detection theo frame

```http
GET /runs/{run_id}/detections/{frame_id}
```

Response:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "frame_id": 120,
  "timestamp_ms": 4000,
  "person_count": 2,
  "boxes": [
    {
      "class_name": "person",
      "confidence": 0.91,
      "x1": 120,
      "y1": 80,
      "x2": 300,
      "y2": 620
    }
  ],
  "model_name": "yolov8n",
  "input_object_key": "bronze/frames/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.jpg",
  "detection_object_key": "silver/detections/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.json",
  "annotated_object_key": "silver/annotated_frames/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.jpg",
  "processed_at": "2026-06-17T10:00:01Z"
}
```

### 4.6 Lấy annotated frame

```http
GET /runs/{run_id}/frames/{frame_id}/annotated
```

Response:

- `200 OK`
- `Content-Type: image/jpeg`
- Body là ảnh JPEG đã vẽ bounding boxes.

Ghi chú:

- Endpoint này có thể stream ảnh từ MinIO qua Storage Server.
- Một lựa chọn khác là trả pre-signed URL của MinIO, nhưng với phase đầu stream qua Storage Server dễ kiểm soát hơn.

### 4.7 Thống kê số người theo thời gian

```http
GET /runs/{run_id}/stats?bucket=frame
```

Query params:

| Param | Type | Default | Mô tả |
| --- | --- | --- | --- |
| `bucket` | string | `frame` | `frame`, `second`, `minute` |

Response:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "bucket": "frame",
  "items": [
    {
      "frame_id": 30,
      "timestamp_ms": 1000,
      "person_count": 1
    },
    {
      "frame_id": 60,
      "timestamp_ms": 2000,
      "person_count": 3
    }
  ],
  "summary": {
    "processed_frames": 42,
    "max_people_count": 5,
    "avg_people_count": 2.4
  }
}
```

## 5. Kafka event contracts

### 5.1 Topic `camera.frames.raw`

Producer:

- Camera Ingestion Server.

Consumer:

- Detection Server.

Key:

```text
run_id:frame_id
```

Value:

```json
{
  "event_id": "a6d21d0b-2222-4444-8888-e3b81ea1f001",
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "frame_id": 120,
  "source_type": "upload",
  "source_name": "demo.mp4",
  "timestamp_ms": 4000,
  "width": 640,
  "height": 360,
  "bucket": "people-counting",
  "object_key": "bronze/frames/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.jpg",
  "created_at": "2026-06-17T10:00:00Z"
}
```

Validation rules:

- `event_id`, `run_id`, `camera_id`, `frame_id`, `bucket`, `object_key` required.
- `frame_id >= 0`.
- `timestamp_ms >= 0`.
- `width > 0`, `height > 0`.

### 5.2 Topic `camera.detections.raw`

Producer:

- Detection Server.

Consumer:

- Storage Server.

Key:

```text
run_id:frame_id
```

Value:

```json
{
  "event_id": "b0f151cf-3333-4444-8888-1abdb1f2f001",
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "frame_id": 120,
  "timestamp_ms": 4000,
  "person_count": 2,
  "boxes": [
    {
      "class_name": "person",
      "confidence": 0.91,
      "x1": 120,
      "y1": 80,
      "x2": 300,
      "y2": 620
    }
  ],
  "model_name": "yolov8n",
  "input_object_key": "bronze/frames/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.jpg",
  "detection_object_key": "silver/detections/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.json",
  "annotated_object_key": "silver/annotated_frames/run_id=7c4f5e34-1111-4444-8888-73fb5e93f001/camera_id=camera_001/frame_000120.jpg",
  "processed_at": "2026-06-17T10:00:01Z"
}
```

Validation rules:

- `person_count >= 0`.
- `person_count == len(boxes)`.
- Mỗi box có `0 <= confidence <= 1`.
- `x1 < x2`, `y1 < y2`.
- `run_id + frame_id` phải idempotent khi ghi vào MongoDB Atlas.

## 6. Dashboard integration flow

Dashboard nên gọi API theo trình tự:

1. User upload video.
2. Dashboard gọi `POST /runs` tới Camera Server.
3. Dashboard nhận `run_id`.
4. Dashboard poll `GET /runs/{run_id}` từ Storage Server mỗi 1-3 giây.
5. Dashboard gọi `GET /runs/{run_id}/stats` để vẽ chart.
6. Dashboard gọi `GET /runs/{run_id}/detections` để hiển thị bảng.
7. Khi user chọn frame, Dashboard gọi `GET /runs/{run_id}/frames/{frame_id}/annotated`.

Trường hợp run mới tạo nhưng Storage Server chưa nhận detection event nào:

- Storage API có thể trả `404` nếu chưa có run document.
- Dashboard nên hiển thị trạng thái chờ và tiếp tục poll.
- Một cách tốt hơn trong implementation là Camera Server tạo run metadata vào Storage Server ngay khi start run.

## 7. Internal run metadata option

Để dashboard không bị `404` lúc run vừa tạo, có thể thêm endpoint nội bộ ở Storage Server:

```http
POST /internal/runs
```

Request:

```json
{
  "run_id": "7c4f5e34-1111-4444-8888-73fb5e93f001",
  "camera_id": "camera_001",
  "source_type": "upload",
  "source_name": "demo.mp4",
  "status": "ingesting",
  "sample_fps": 1,
  "resize_width": 640,
  "created_at": "2026-06-17T10:00:00Z"
}
```

Endpoint này chỉ dùng giữa Camera Server và Storage Server trong mạng Docker Compose. Storage Server sau đó ghi metadata lên MongoDB Atlas. Phase đầu có thể implement nếu muốn dashboard mượt hơn.

## 8. API cần ưu tiên implement

Thứ tự tối thiểu để có demo:

1. `POST /runs` ở Camera Server.
2. Publish event `camera.frames.raw`.
3. Consume và publish event `camera.detections.raw`.
4. Storage Server consume detection và ghi MongoDB Atlas.
5. `GET /runs/{run_id}/detections`.
6. `GET /runs/{run_id}/stats`.
7. Streamlit upload video và hiển thị chart/table.
8. `GET /runs/{run_id}/frames/{frame_id}/annotated`.

Các endpoint còn lại có thể bổ sung sau để hoàn thiện UX và tài liệu báo cáo.
