# Kế hoạch triển khai hệ thống đếm người trong camera/video

## 1. Mục tiêu

Xây dựng hệ thống đếm số lượng người hiện diện trong một camera hoặc video đầu vào. Hệ thống phải được tách thành các server/service độc lập, giao tiếp qua network, và có sử dụng công nghệ dữ liệu lớn trong ngữ cảnh streaming data pipeline.

Yêu cầu chính:

- Camera Ingestion Server nhận video/camera, tách frame và gửi frame event vào pipeline.
- Detection Server nhận frame, chạy nhận diện người, trả về bounding box và số lượng người trong từng frame.
- Storage Server lưu kết quả nhận diện.
- Dashboard cho phép chọn hoặc upload video bất kỳ để chạy thử hệ thống và xem kết quả.
- Hệ thống chạy được trên máy hiện tại: CPU 4 cores, RAM 8 GB.

## 2. Scope đã chốt

- Chạy trên 1 máy local, không cần tách thành 3 máy vật lý.
- Các server vẫn phải tách biệt thành service riêng và giao tiếp qua network/message broker.
- Input ưu tiên giai đoạn đầu: video file bất kỳ.
- Mỗi frame chỉ cần đếm số người hiện diện, chưa cần tracking định danh người qua nhiều frame.
- Kết quả cần lưu:
  - số lượng người trong frame;
  - danh sách bounding box của người trong frame;
  - metadata như `run_id`, `camera_id`, `frame_id`, timestamp, model version.
- Không bắt buộc dùng Hadoop/Spark/Kafka cụ thể, nhưng project nên thể hiện pipeline chuẩn Data Engineer.

## 3. Quyết định công nghệ đề xuất

Do máy chỉ có 4 cores và 8 GB RAM, không nên bật toàn bộ Kafka + Spark + Airflow + nhiều database cùng lúc trong bản demo mặc định. Kiến trúc nên chia thành 2 mức:

### Core pipeline bắt buộc

- Python: ngôn ngữ chính.
- OpenCV: đọc video, trích frame.
- FastAPI: tạo API cho Camera Server và Storage Server.
- Apache Kafka: message broker streaming chính, thử trước theo scope đã chốt.
- MinIO: object storage để lưu raw frames, annotated frames và file kết quả dạng JSON.
- YOLOv8n hoặc YOLO11n: model nhận diện người, dùng bản nano để chạy được trên CPU.
- MongoDB Atlas: lưu metadata detection và kết quả truy vấn nhanh cho dashboard. MongoDB local chỉ là fallback khi cần demo offline.
- Streamlit: dashboard upload/chọn video, trigger job, xem kết quả.
- Docker Compose: chạy các service độc lập trên cùng một máy.

Ghi chú: nếu Apache Kafka quá nặng trên máy 8 GB RAM, có thể chuyển sang Redpanda vì Redpanda tương thích Kafka API và ít ảnh hưởng đến code producer/consumer.

### Optional pipeline để mở rộng báo cáo Big Data

- Spark batch job: aggregate số người theo phút/giờ/ngày từ dữ liệu Silver sang Gold.
- Airflow: điều phối batch pipeline nếu cần trình bày orchestration.

Trong bản chạy mặc định, Spark và Airflow nên để optional bằng Docker Compose profile hoặc chỉ mô tả trong tài liệu. Nếu bật cả Spark/Airflow trên máy 8 GB RAM, hệ thống dễ chậm hoặc thiếu RAM.

## 4. Kiến trúc dữ liệu

Không nên gửi trực tiếp ảnh/frame lớn qua Kafka. Cách tốt hơn là:

1. Camera Server đọc frame từ video.
2. Camera Server lưu frame JPEG vào MinIO Bronze.
3. Camera Server gửi metadata event vào topic `camera.frames.raw`.
4. Detection Server consume event, lấy frame từ MinIO, chạy YOLO.
5. Detection Server lưu annotated frame và detection JSON vào MinIO Silver.
6. Detection Server gửi detection event vào topic `camera.detections.raw`.
7. Storage Server consume detection event, lưu kết quả vào MongoDB Atlas.
8. Aggregation Worker hoặc Spark job tạo thống kê theo thời gian và lưu Gold dataset.
9. Streamlit Dashboard đọc dữ liệu từ Storage API để hiển thị.

Luồng tổng quan:

```text
Streamlit Dashboard
        |
        | upload/select video, start run
        v
Camera Ingestion Server
        |
        | save raw frame
        v
MinIO Bronze
        |
        | publish frame metadata
        v
Kafka topic: camera.frames.raw
        |
        v
Detection Server YOLO
        |
        | save detection json + annotated frame
        v
MinIO Silver
        |
        | publish detection metadata
        v
Kafka topic: camera.detections.raw
        |
        v
Storage Server
        |
        v
MongoDB Atlas
        |
        v
Storage API / Streamlit Dashboard
```

## 5. Data lake layout

MinIO buckets hoặc prefixes nên chia theo Bronze/Silver/Gold:

```text
people-counting/
├── bronze/
│   └── frames/
│       └── run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.jpg
├── silver/
│   ├── detections/
│   │   └── run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.json
│   └── annotated_frames/
│       └── run_id=<run_id>/camera_id=<camera_id>/frame_<frame_id>.jpg
└── gold/
    └── people_count_by_minute/
        └── run_id=<run_id>/part-*.json
```

Ý nghĩa:

- Bronze: dữ liệu thô, gần với input nhất.
- Silver: dữ liệu đã xử lý bằng model detection.
- Gold: dữ liệu tổng hợp phục vụ phân tích/dashboard/report.

## 6. Event schema đề xuất

### Topic `camera.frames.raw`

```json
{
  "event_id": "uuid",
  "run_id": "uuid",
  "camera_id": "camera_001",
  "frame_id": 120,
  "source_type": "video_file",
  "source_name": "demo.mp4",
  "timestamp_ms": 4000,
  "width": 1280,
  "height": 720,
  "bucket": "people-counting",
  "object_key": "bronze/frames/run_id=.../camera_id=camera_001/frame_000120.jpg",
  "created_at": "2026-06-17T10:00:00Z"
}
```

### Topic `camera.detections.raw`

```json
{
  "event_id": "uuid",
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

## 7. Cấu trúc repo cần sửa lại

Repo hiện tại mới có tài liệu và `tcp_example.py`. Nên tái cấu trúc theo hướng sau:

```text
Lab5/
├── docker-compose.yml
├── README.md
├── .env.example
├── .gitignore
│
├── configs/
│   └── app.yaml
│
├── services/
│   ├── camera_server/
│   │   ├── app.py
│   │   ├── ingestion.py
│   │   ├── frame_store.py
│   │   ├── producer.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── detection_server/
│   │   ├── worker.py
│   │   ├── detector.py
│   │   ├── consumer.py
│   │   ├── producer.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── storage_server/
│   │   ├── app.py
│   │   ├── consumer.py
│   │   ├── repository.py
│   │   ├── routes.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   └── dashboard/
│       ├── app.py
│       ├── api_client.py
│       ├── requirements.txt
│       └── Dockerfile
│
├── shared/
│   ├── __init__.py
│   ├── config.py
│   ├── events.py
│   ├── kafka_client.py
│   ├── minio_client.py
│   └── logging.py
│
├── schemas/
│   ├── frame_event_schema.json
│   └── detection_event_schema.json
│
├── storage/
│   ├── minio/
│   │   └── create_buckets.py
│   └── mongo/
│       └── create_indexes.py
│
├── pipelines/
│   ├── python/
│   │   └── aggregate_people_count.py
│   └── spark/
│       └── aggregate_people_count.py
│
├── scripts/
│   ├── create_topics.py
│   ├── run_local.ps1
│   └── smoke_test.py
│
├── data/
│   └── sample_videos/
│       └── .gitkeep
│
├── results/
│   ├── sample_detection.json
│   ├── sample_stats.json
│   └── screenshots/
│
├── docs/
│   ├── yeucau.md
│   ├── implementation_plan.md
│   ├── architecture.md
│   ├── data_pipeline.md
│   ├── data_quality.md
│   └── api.md
│
└── examples/
    └── tcp_example.py
```

Ghi chú:

- `tcp_example.py` nên chuyển vào `examples/` vì đây là ví dụ socket TCP, không phải core pipeline.
- `services/` chứa các server độc lập.
- `shared/` chứa code dùng chung: config, schema/event model, Kafka client, MinIO client.
- `pipelines/python/` là aggregation nhẹ chạy được trên máy local.
- `pipelines/spark/` để optional nếu cần chứng minh thêm năng lực Big Data.
- `data/sample_videos/` chỉ lưu video nhỏ dùng demo. Video lớn không nên commit vào git.

## 8. Thiết kế từng service

### Camera Server

Nhiệm vụ:

- Nhận request start run từ Streamlit hoặc API.
- Nhận video upload hoặc đường dẫn video local.
- Đọc video bằng OpenCV.
- Sample frame theo cấu hình, ví dụ 1-3 FPS.
- Resize frame về chiều rộng khoảng 640 px để giảm tải CPU.
- Lưu frame vào MinIO Bronze.
- Publish frame metadata vào topic `camera.frames.raw`.

API đề xuất:

- `POST /runs`: tạo run mới từ video upload hoặc video path.
- `GET /runs/{run_id}`: xem trạng thái ingestion.

### Detection Server

Nhiệm vụ:

- Consume topic `camera.frames.raw`.
- Tải frame từ MinIO.
- Chạy YOLO và lọc class `person`.
- Tạo danh sách bounding box.
- Lưu detection JSON và annotated frame vào MinIO Silver.
- Publish event vào topic `camera.detections.raw`.

Cấu hình mặc định cho máy 4 cores/8 GB RAM:

- model: `yolov8n.pt` hoặc model nano tương đương;
- inference device: CPU nếu không có NVIDIA GPU;
- batch size: 1;
- frame sample rate: 1-3 FPS;
- image size: 640.

### Storage Server

Nhiệm vụ:

- Consume topic `camera.detections.raw`.
- Lưu metadata vào MongoDB Atlas.
- Cung cấp API cho dashboard.

API đề xuất:

- `GET /runs`: danh sách run.
- `GET /runs/{run_id}`: thông tin run.
- `GET /runs/{run_id}/detections`: danh sách detection theo frame.
- `GET /runs/{run_id}/stats`: thống kê số người theo thời gian.

### Streamlit Dashboard

Nhiệm vụ:

- Upload/chọn video.
- Gọi Camera Server để bắt đầu xử lý.
- Hiển thị trạng thái pipeline.
- Hiển thị bảng detection theo frame.
- Hiển thị biểu đồ số người theo thời gian.
- Hiển thị frame đã annotate nếu có lưu annotated frame.

## 9. Lộ trình triển khai

### Phase 1: Skeleton và infrastructure

- Tạo lại cấu trúc repo.
- Tạo `.env.example`.
- Tạo Docker Compose cho Apache Kafka, MinIO và các service app. MongoDB dùng Atlas nên chỉ cần cấu hình `MONGODB_URI`.
- Tạo script tạo topic và bucket.

### Phase 2: Camera ingestion

- Implement Camera Server đọc video.
- Lưu frame vào MinIO.
- Publish frame metadata vào Kafka.
- Test bằng video ngắn.

### Phase 3: Detection

- Implement Detection Server consume frame event.
- Chạy YOLO lọc người.
- Lưu detection JSON và annotated frame.
- Publish detection event.

### Phase 4: Storage API

- Implement Storage Server consume detection event.
- Lưu MongoDB Atlas.
- Tạo API đọc run/detection/stats.

### Phase 5: Dashboard

- Implement Streamlit upload video.
- Trigger run.
- Hiển thị kết quả.

### Phase 6: Data Engineering extension

- Implement Python aggregation tạo Gold dataset.
- Optional: Spark batch job đọc Silver và ghi Gold.
- Viết tài liệu kiến trúc, pipeline, data quality, API.

## 10. Yêu cầu phần cứng và cấu hình chạy

Máy hiện tại: CPU 4 cores, RAM 8 GB.

Cấu hình chạy khuyến nghị:

- Chạy local bằng Docker Compose cho Kafka/MinIO/app services, kết nối MongoDB Atlas qua internet.
- Không bật Spark/Airflow trong demo mặc định.
- Thử Apache Kafka trước; chỉ chuyển sang Redpanda nếu máy local quá chậm hoặc thiếu RAM.
- Chỉ xử lý video ở 1-3 FPS.
- Resize frame trước khi lưu và detect.
- Dùng YOLO nano model.
- Lưu annotated frame cho mọi frame đã được sample và xử lý. Nếu video dài, giảm `sample_fps` hoặc dùng `max_frames` để tránh đầy ổ đĩa.

Ước lượng RAM:

```text
Apache Kafka:     1.5 - 3.0 GB
MinIO:            0.3 - 0.5 GB
Camera Server:    0.3 - 0.7 GB
Detection Server: 1.5 - 3.0 GB
Storage Server:   0.3 - 0.7 GB
Streamlit:        0.3 - 0.7 GB
```

Tổng có thể nằm quanh 5.5-8 GB tùy video và model nếu dùng Apache Kafka. MongoDB Atlas giúp giảm RAM local vì không cần chạy container MongoDB. Vì vậy vẫn cần giới hạn FPS, độ phân giải và số service optional. Nếu máy thiếu RAM, chuyển Kafka sang Redpanda là phương án giảm tải hợp lý.

Nếu có GPU NVIDIA, Detection Server có thể chạy nhanh hơn đáng kể. Nếu chỉ có CPU, hệ thống vẫn chạy được nhưng không nên kỳ vọng realtime với video độ phân giải cao.

## 11. Quyết định đã chốt và câu hỏi còn mở

Quyết định đã chốt:

1. Message broker: thử Apache Kafka trước. Nếu máy 8 GB RAM không chịu nổi, chuyển sang Redpanda.
2. Dashboard: hỗ trợ upload video trực tiếp.
3. Môi trường chạy: CPU-only.
4. Annotated frame: lưu cho mọi frame đã được sample và xử lý.
5. Spark/Airflow: không chạy thật trong demo mặc định, chỉ để optional/tài liệu hóa.
6. Số lượng input: một video tại một thời điểm.
7. `tcp_example.py`: chỉ dùng làm ví dụ tham khảo socket TCP, không phải core pipeline.
8. Bước tiếp theo: viết tài liệu kiến trúc và API trước khi tạo skeleton repo.

