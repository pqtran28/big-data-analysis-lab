# Assignment 5 – Hệ thống đếm số lượng người trong camera với xử lý phân tán

## Mô tả

Hệ thống đếm số lượng người hiện diện trong camera theo thời gian thực, xây dựng trên nền tảng **Apache Kafka**

## Kiến trúc hệ thống

```
┌─────────────────────┐
│   Video / Webcam    │
└────────┬────────────┘
         │ frame (base64 JPEG)
         ▼
┌─────────────────────┐        Kafka Topic        ┌──────────────────────┐
│  camera_server.py   │ ──── "raw_frames" ──────► │ detection_server.py  │
│  (Kafka Producer)   │      3 partitions          │ (Kafka Consumer      │
└─────────────────────┘                            │  + Producer)         │
                                                   │  HOG                 │
                                                   └──────────┬───────────┘
                                                              │ bounding boxes
                                                              │ Kafka Topic
                                                              │ "detection_results"
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │  storage_server.py   │
                                                   │  (Kafka Consumer     │
                                                   │   + Flask REST API)  │
                                                   │  SQLite Database     │
                                                   └──────────┬───────────┘
                                                              │ REST API :5001
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │    dashboard.py      │
                                                   │  (Flask Web UI :5000)│
                                                   └──────────────────────┘
```

## Các thành phần

### 1. Camera Server (`servers/camera_server.py`)
- Đọc frame từ webcam hoặc video file
- Encode frame sang base64 JPEG
- Publish message JSON lên Kafka topic `raw_frames`

### 2. Detection Server (`servers/detection_server.py`)
- Subscribe Kafka topic `raw_frames`
- Decode frame từ base64 -> numpy array
- Chạy nhận diện người bằng **HOG + SVM** (mặc định) hoặc **YOLOv8** (hiện tại hệ thống chưa dùng mô hình này)
- Trả về tập hợp bounding boxes `[{x1, y1, x2, y2, confidence}]`
- Publish kết quả lên Kafka topic `detection_results`

### 3. Storage Server (`servers/storage_server.py`)
- Subscribe Kafka topic `detection_results`
- Lưu kết quả vào SQLite database (`results/people_count.db`)
- Expose REST API tại port 5001

### 4. Dashboard (`dashboard.py`)
- Web UI hiển thị live feed, biểu đồ theo thời gian, bảng kết quả
- Poll REST API mỗi 1 giây
- Hiện tại hệ thống chưa hoàn thiện tính năng xem dashboard này

## Công nghệ Big Data

| Công nghệ | Vai trò |
|-----------|---------|
| Apache Kafka | Distributed event streaming – nhận/gửi frame giữa các server |
| Kafka Topics | `raw_frames` (3 partitions), `detection_results` (3 partitions) |
| Consumer Groups | Scale detection_server theo chiều ngang |
| ZooKeeper | Quản lý Kafka cluster |

Kafka đóng vai trò **message broker phân tán** - tách rời hoàn toàn 3 server, cho phép mỗi server chạy độc lập theo tốc độ của nó. Camera có thể gửi frame nhanh hơn detection xử lý mà không mất dữ liệu.

## Yêu cầu

- Python 3.10+
- Java JDK 17
- Apache Kafka 3.7.1

```bash
pip install kafka-python-ng opencv-python numpy flask
```

## Cách chạy

### Bước 1: Khởi động Kafka

**Lưu ý**: Cần khai báo biến môi trường phù hợp và cần chỉnh sửa đường dẫn phù hợp để chạy trong file `kafka-run-class.bat`

```bash
# ZooKeeper
# Do kafka hiện tại được cài nằm trong ổ D
D:\kafka\bin\windows\zookeeper-server-start.bat D:\kafka\config\zookeeper.properties

# Kafka Broker
D:\kafka\bin\windows\kafka-server-start.bat D:\kafka\config\server.properties
```

### Bước 2: Tạo Kafka topics

```bash
python utils/setup_topics.py
```

### Bước 3: Chạy 3 server

```bash
# storage
python servers/storage_server.py

# detection
python servers/detection_server.py

# camera frame
python servers/camera_server.py --source video.mp4
```

### Bước 4: Xem dashboard (hiện tính năng chưa hoàn thiện)

```bash
python dashboard.py
# Mở trình duyệt: http://localhost:5000
```

## REST API

| Endpoint | Mô tả |
|----------|-------|
| `GET /api/results` | 100 kết quả gần nhất |
| `GET /api/stats` | Thống kê tổng hợp |
| `GET /api/latest` | Frame mới nhất |
| `GET /api/export/csv` | Tải toàn bộ dữ liệu CSV |

## Kết quả

Dữ liệu lưu trong `results/people_count.db` (SQLite) và `results/export.csv`.

Mỗi record gồm: `camera_id`, `frame_id`, `person_count`, `bounding_boxes`, `latency_ms`, `inference_ms`, `timestamp`.

## Cấu trúc thư mục

```
assignment-5/
├── servers/
│   ├── camera_server.py      # Kafka Producer
│   ├── detection_server.py   # Kafka Consumer + Producer
│   └── storage_server.py     # Kafka Consumer + REST API
├── utils/
│   └── setup_topics.py       # Tạo Kafka topics
├── results/
│   ├── people_count.db       # SQLite database
│   └── export.csv            # CSV export
├── dashboard.py              # Web UI
├── requirements.txt
└── README.md
```
