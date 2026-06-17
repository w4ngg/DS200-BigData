# Tài liệu kiến trúc hệ thống

## 1. Bối cảnh và mục tiêu

Hệ thống cần đếm số lượng người hiện diện trong một video/camera. Bản triển khai ban đầu ưu tiên video file upload từ dashboard, chạy trên một máy local CPU-only với 4 cores và 8 GB RAM.

Mục tiêu kiến trúc:

- Tách hệ thống thành nhiều service độc lập.
- Các service giao tiếp qua network và message broker.
- Có pipeline dữ liệu theo hướng Data Engineering: ingest, streaming event, object storage, serving database, dashboard.
- Chạy được local để demo và báo cáo.
- Có khả năng mở rộng sang nhiều camera, nhiều worker detection hoặc batch analytics sau này.

Ngoài phạm vi bản đầu:

- Không tracking định danh từng người qua nhiều frame.
- Không chạy Spark/Airflow thật trong demo mặc định.
- Không xử lý nhiều video/camera đồng thời trong phase đầu.
- Không bắt buộc dùng socket TCP trực tiếp; `tcp_example.py` chỉ là ví dụ tham khảo client-server.

## 2. Quyết định kỹ thuật chính

| Nhóm | Công nghệ | Lý do |
| --- | --- | --- |
| Ngôn ngữ | Python | Phù hợp OpenCV, YOLO, FastAPI, Streamlit |
| Video processing | OpenCV | Đọc video, tách frame, resize, encode JPEG |
| API service | FastAPI | Nhẹ, dễ tạo REST API, dễ chạy Docker |
| Message broker | Apache Kafka | Thử trước theo scope đã chốt, thể hiện rõ streaming data pipeline |
| Object storage | MinIO | Mô phỏng data lake local, lưu raw/annotated frame và JSON |
| Detection model | YOLO nano model | Chạy được CPU-only tốt hơn model lớn |
| Serving database | MongoDB Atlas | Lưu kết quả detection dạng document linh hoạt, giảm RAM local |
| Dashboard | Streamlit | Upload video và xem kết quả nhanh |
| Packaging | Docker Compose | Chạy nhiều service độc lập trên một máy |

Ghi chú về Kafka:

- Bản đầu thử Apache Kafka.
- Nếu máy 8 GB RAM chạy quá chậm, có thể thay Apache Kafka bằng Redpanda vì Redpanda tương thích Kafka API. Khi đó phần lớn code producer/consumer vẫn giữ nguyên.

## 3. Kiến trúc tổng quan

```text
User
 |
 | upload video / start run
 v
Streamlit Dashboard
 |
 | REST API
 v
Camera Ingestion Server
 |
 | 1. read video, sample frame, resize
 | 2. save raw frame
 v
MinIO Bronze
 |
 | publish metadata event
 v
Kafka topic: camera.frames.raw
 |
 | consume frame event
 v
Detection Server
 |
 | 1. load raw frame from MinIO
 | 2. run YOLO person detection
 | 3. save detection json + annotated frame
 v
MinIO Silver
 |
 | publish detection event
 v
Kafka topic: camera.detections.raw
 |
 | consume detection event
 v
Storage Server
 |
 | save serving documents
 v
MongoDB Atlas
 |
 | REST API
 v
Streamlit Dashboard
```

## 4. Service boundaries

### 4.1 Streamlit Dashboard

Vai trò:

- Cho phép upload video trực tiếp.
- Gửi video sang Camera Ingestion Server để tạo run.
- Poll Storage API để xem trạng thái và kết quả.
- Hiển thị:
  - tổng số frame đã xử lý;
  - số người theo frame/thời gian;
  - bảng bounding box;
  - annotated frame.

Dashboard không trực tiếp chạy YOLO và không ghi MongoDB Atlas. Nó chỉ là giao diện điều khiển và hiển thị.

### 4.2 Camera Ingestion Server

Vai trò:

- Nhận video upload từ dashboard.
- Tạo `run_id`.
- Đọc video bằng OpenCV.
- Lấy mẫu frame theo cấu hình, ví dụ 1 FPS hoặc 2 FPS.
- Resize frame về chiều rộng mặc định 640 px để giảm tải CPU và dung lượng lưu.
- Lưu raw frame vào MinIO Bronze.
- Publish metadata vào Kafka topic `camera.frames.raw`.

Camera Server không detect người và không lưu kết quả cuối vào MongoDB Atlas.

### 4.3 Detection Server

Vai trò:

- Consume event từ topic `camera.frames.raw`.
- Tải raw frame từ MinIO.
- Chạy YOLO model, lọc class `person`.
- Tính `person_count`.
- Vẽ bounding box lên ảnh để tạo annotated frame.
- Lưu detection JSON và annotated frame vào MinIO Silver.
- Publish detection event vào Kafka topic `camera.detections.raw`.

Vì máy CPU-only, Detection Server chạy một worker trước. Batch size mặc định là 1.

### 4.4 Storage Server

Vai trò:

- Consume event từ topic `camera.detections.raw`.
- Upsert thông tin run và detection vào MongoDB Atlas.
- Cung cấp REST API cho dashboard đọc dữ liệu.
- Tính thống kê đơn giản theo frame hoặc theo mốc thời gian.

Storage Server không chạy YOLO và không xử lý ảnh nặng.

### 4.5 Kafka

Vai trò:

- Tách rời tốc độ ingest video và tốc độ detection.
- Cho phép Detection Server xử lý chậm hơn Camera Server mà không mất event.
- Là thành phần Big Data/streaming chính trong bản demo.

Topics:

- `camera.frames.raw`: frame metadata sau khi raw frame được lưu vào MinIO.
- `camera.detections.raw`: detection metadata sau khi YOLO xử lý frame.

### 4.6 MinIO

Vai trò:

- Lưu file lớn và bán cấu trúc.
- Tránh gửi ảnh trực tiếp qua Kafka.
- Tổ chức dữ liệu theo Bronze/Silver/Gold.

Buckets/prefixes:

```text
people-counting/
├── bronze/frames/
├── silver/detections/
├── silver/annotated_frames/
└── gold/people_count_by_minute/
```

Trong phase đầu, annotated frame được lưu cho mọi frame đã được sample và xử lý. Nếu video dài, nên giảm `sample_fps` để tránh đầy ổ đĩa.

### 4.7 MongoDB Atlas

Vai trò:

- Lưu dữ liệu phục vụ truy vấn dashboard.
- Lưu document linh hoạt cho detection result và bounding boxes.
- Giảm tải RAM local vì database chạy trên managed cloud service.

Collections đề xuất:

- `runs`: thông tin mỗi lần xử lý video.
- `detections`: kết quả từng frame.
- `run_stats`: thống kê tổng hợp, có thể tạo bằng worker Python sau.

## 5. Luồng xử lý chi tiết

### 5.1 Start run

1. User upload video trên Streamlit.
2. Dashboard gọi `POST /runs` của Camera Server.
3. Camera Server tạo `run_id`.
4. Camera Server lưu video tạm thời trong container hoặc volume local.
5. Camera Server bắt đầu đọc video và publish frame event.
6. Dashboard nhận `run_id` và bắt đầu poll Storage API.

### 5.2 Ingest frame

1. Camera Server đọc video metadata: FPS, width, height, duration.
2. Camera Server quyết định frame nào cần lấy theo `sample_fps`.
3. Mỗi frame được resize và encode thành JPEG.
4. JPEG được lưu vào MinIO Bronze.
5. Camera Server publish event vào `camera.frames.raw`.

### 5.3 Detection

1. Detection Server consume event từ `camera.frames.raw`.
2. Detection Server tải frame từ MinIO.
3. YOLO detect object.
4. Chỉ giữ object có class `person`.
5. Detection Server tạo:
   - `person_count`;
   - `boxes`;
   - `model_name`;
   - `input_object_key`;
   - `detection_object_key`;
   - `annotated_object_key`.
6. Detection JSON và annotated frame được lưu vào MinIO Silver.
7. Detection Server publish event vào `camera.detections.raw`.

### 5.4 Store and serve

1. Storage Server consume event từ `camera.detections.raw`.
2. Storage Server lưu document vào MongoDB Atlas.
3. Dashboard gọi Storage API để lấy detections/stats.
4. Dashboard hiển thị biểu đồ, bảng và annotated frame.

## 6. Data model

### 6.1 `runs`

```json
{
  "run_id": "uuid",
  "camera_id": "camera_001",
  "source_type": "upload",
  "source_name": "demo.mp4",
  "status": "running",
  "sample_fps": 1,
  "resize_width": 640,
  "total_frames": 1250,
  "sampled_frames": 95,
  "processed_frames": 80,
  "created_at": "2026-06-17T10:00:00Z",
  "updated_at": "2026-06-17T10:01:00Z"
}
```

Status đề xuất:

- `queued`
- `ingesting`
- `processing`
- `completed`
- `failed`

### 6.2 `detections`

```json
{
  "run_id": "uuid",
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
  "input_object_key": "bronze/frames/run_id=.../frame_000120.jpg",
  "detection_object_key": "silver/detections/run_id=.../frame_000120.json",
  "annotated_object_key": "silver/annotated_frames/run_id=.../frame_000120.jpg",
  "processed_at": "2026-06-17T10:00:01Z"
}
```

Indexes đề xuất:

- `runs.run_id` unique.
- `detections.run_id + detections.frame_id` unique.
- `detections.run_id + detections.timestamp_ms`.

## 7. MongoDB Atlas và fallback local

### MongoDB Atlas

Ưu điểm:

- Không tốn RAM local cho database.
- Dễ trình bày hướng cloud database trong báo cáo.
- Có UI quản lý dữ liệu tiện hơn.
- Phù hợp hơn với cách triển khai service độc lập kết nối qua network.

Nhược điểm:

- Cần internet ổn định khi demo.
- Cần tạo account, cluster, database user/password.
- Phải cấu hình Network Access/IP allowlist.
- Cần bảo vệ connection string, không commit credential vào git.
- Nếu trường/mạng chặn kết nối cloud thì demo có thể lỗi.

Khuyến nghị:

- Dùng MongoDB Atlas làm mặc định.
- Đặt connection string trong `.env`.
- Tạo indexes bằng script `storage/mongo/create_indexes.py` khi khởi tạo project.

### Local MongoDB bằng Docker

Ưu điểm:

- Dễ demo offline.
- Không cần tài khoản cloud.
- Không cần mở IP allowlist.
- Dữ liệu nằm trên máy local, dễ reset.

Nhược điểm:

- Máy 8 GB RAM phải gánh thêm một container.
- Nếu xóa volume Docker nhầm thì mất dữ liệu.
- Không thể hiện rõ hướng cloud/managed database như Atlas.

Khuyến nghị:

- Chỉ dùng local MongoDB làm fallback khi không có internet hoặc Atlas gặp lỗi lúc demo.
- Nếu cần fallback, chỉ đổi `MONGODB_URI` sang URI local và bật thêm MongoDB container trong Docker Compose profile riêng.

## 8. Deployment local

Docker Compose nên chia thành các nhóm:

Core infrastructure:

- Kafka
- MinIO

Application services:

- camera-server
- detection-server
- storage-server
- dashboard

Optional:

- MongoDB local fallback
- Spark batch
- Airflow

Với máy 4 cores/8 GB RAM:

- Không bật Spark/Airflow mặc định.
- Chỉ chạy một Detection Worker.
- Dùng video ngắn hoặc giảm `sample_fps`.
- Lưu annotated frame cho frame đã sample, không nhất thiết mọi frame gốc của video.
- Nếu Kafka quá nặng, chuyển sang Redpanda.

## 9. Failure handling

Các lỗi cần xử lý trong code:

- Video upload lỗi hoặc không đọc được bằng OpenCV.
- MinIO không sẵn sàng.
- Kafka chưa tạo topic.
- Detection Server không tải được model YOLO.
- Frame object không tồn tại trong MinIO.
- MongoDB Atlas mất kết nối, sai credential hoặc IP chưa được allowlist.
- Dashboard poll khi run chưa có detection nào.

Chiến lược đơn giản:

- Log lỗi rõ ràng theo `run_id` và `frame_id`.
- Với lỗi từng frame, ghi trạng thái frame là failed và tiếp tục frame tiếp theo.
- Với lỗi hệ thống như Kafka/MinIO down, service fail fast để Docker restart.
- Với lỗi MongoDB Atlas tạm thời, Storage Server nên retry có giới hạn và log rõ lỗi connection string/IP allowlist.
- Consumer nên dùng idempotent write: upsert theo `(run_id, frame_id)` để tránh duplicate.

## 10. Bảo mật và cấu hình

Trong bản lab local:

- Không cần auth phức tạp giữa các service.
- Tất cả credentials đặt trong `.env`.
- Không commit `.env`.
- Commit `.env.example` để người khác biết biến môi trường cần có.
- MongoDB Atlas connection string phải đặt trong `.env`.
- Không commit username, password hoặc full Atlas URI vào git.

Biến môi trường chính:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=people-counting
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster-url>/?retryWrites=true&w=majority
MONGODB_DATABASE=people_counting
MODEL_NAME=yolov8n.pt
SAMPLE_FPS=1
RESIZE_WIDTH=640
SAVE_ANNOTATED_FRAMES=true
```

## 11. Extension sau phase đầu

Các hướng mở rộng khi core pipeline đã chạy:

- Hỗ trợ RTSP/webcam.
- Chạy nhiều Detection Worker cùng group consumer.
- Thêm Spark batch job đọc Silver và ghi Gold.
- Thêm Airflow DAG điều phối job hằng ngày.
- Thêm tracking để giảm đếm trùng theo thời gian.
- Thêm alert khi số người vượt ngưỡng.
- Thêm authentication cho dashboard/API.
