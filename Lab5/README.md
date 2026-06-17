# Lab5 - People Counting Streaming Pipeline

Hệ thống đếm số người trong video theo kiến trúc tách rời 3 service:

```text
Streamlit Dashboard -> Camera Server -> Kafka -> Detection Server -> Kafka -> Storage Server -> MongoDB Atlas
                                      \-> MinIO Bronze/Silver/Gold data lake
```

## Công nghệ

- Python, FastAPI, Streamlit
- OpenCV để đọc và lấy mẫu frame
- Apache Kafka để truyền metadata event
- MinIO để lưu raw frame, detection JSON và annotated frame
- YOLO nano model để detect người
- MongoDB Atlas để lưu metadata phục vụ dashboard
- Chi tiết về công nghệ nằm trong [file kiến trúc này](docs/architecture.md)
## Data lake Bronze/Silver/Gold

Pipeline realtime mặc định ghi Bronze và Silver:

- Bronze: raw frame JPEG được sample từ video.
- Silver: detection JSON và annotated frame sau khi YOLO xử lý.
- Gold là tầng tổng hợp phục vụ báo cáo/thống kê, có thể thay đổi tùy mục tiêu. Hiện tại Gold là people count theo phút/giờ hoặc các thống kê đã aggregate.
- Có thể tạo Gold sau khi run hoàn tất bằng Docker job:

```powershell
docker compose --profile jobs run --rm gold-aggregator
```

Kết quả Gold được ghi vào MinIO:

```text
gold/people_count_by_minute/run_id=<run_id>/part-00000.json
```

Nếu muốn chạy script trực tiếp từ host thay vì Docker, cần cài dependencies Python và dùng endpoint MinIO qua host:

```powershell
$env:MINIO_ENDPOINT="localhost:9000"
python pipelines\python\aggregate_people_count.py
```

## Chạy local

1. Tạo file `.env` từ `.env.example`.
2. Điền `MONGODB_URI` MongoDB Atlas.
3. Mở IP hiện tại trong MongoDB Atlas Network Access.
4. Chạy hệ thống:

```powershell
docker compose up --build
```

5. Mở dashboard:

```text
http://localhost:8501
```

## Khởi tạo thủ công

Các service tự tạo bucket MinIO khi cần. Nếu muốn tạo trước:

```powershell
python storage\minio\create_buckets.py
python scripts\create_topics.py
python storage\mongo\create_indexes.py
```

Khi chạy script từ host, đặt:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=localhost:9000
```

## Cấu hình khuyến nghị cho máy 4 cores/8 GB RAM

- `SAMPLE_FPS=1`
- `RESIZE_WIDTH=640`
- `MODEL_DEVICE=cpu`
- Dùng video ngắn khi demo
- Nếu Kafka quá nặng, thay bằng Redpanda tương thích Kafka API
