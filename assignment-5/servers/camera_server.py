"""
servers/camera_server.py  –  Camera Server (Kafka Producer)
============================================================
Chức năng:
  - Đọc frame từ webcam hoặc video file
  - Encode frame sang base64 JPEG
  - Publish message JSON lên Kafka topic "raw_frames"
"""

import argparse
import base64
import json
import time
import uuid
import sys
import os

import cv2
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP   = "localhost:9092"
TOPIC_RAW_FRAMES  = "raw_frames"
CAMERA_ID         = f"cam-{uuid.uuid4().hex[:6]}"
TARGET_FPS        = 5  
JPEG_QUALITY      = 60   
MAX_WIDTH         = 640   


def connect_producer(retries: int = 10) -> KafkaProducer:
    """Kết nối Kafka producer với retry."""
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_request_size=10_485_760,   # 10 MB – đủ chứa 1 frame JPEG
                compression_type="gzip",
                linger_ms=50,
            )
            print(f"[Camera] ✅ Kết nối Kafka thành công")
            return producer
        except NoBrokersAvailable:
            print(f"[Camera] ⏳ Retry {i+1}/{retries} – Kafka chưa sẵn sàng...")
            time.sleep(3)
    print("[Camera] ❌ Không thể kết nối Kafka. Hãy khởi động Kafka trước.")
    sys.exit(1)


def resize_frame(frame: np.ndarray, max_width: int) -> np.ndarray:
    """Resize frame giữ tỉ lệ khung hình nếu rộng hơn max_width."""
    h, w = frame.shape[:2]
    if w > max_width:
        scale = max_width / w
        frame = cv2.resize(frame, (max_width, int(h * scale)))
    return frame


def encode_frame(frame: np.ndarray, quality: int = 60) -> str:
    """Encode numpy array → base64 JPEG string."""
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, quality]
    _, buffer = cv2.imencode(".jpg", frame, encode_params)
    return base64.b64encode(buffer).decode("utf-8")


def generate_demo_frame(frame_idx: int) -> np.ndarray:
    """
    Tạo frame giả để demo khi không có webcam/video.
    Vẽ hình người đơn giản với số lượng thay đổi theo thời gian.
    """
    frame = np.zeros((480, 640, 3), dtype=np.uint8)
    frame[:] = (30, 30, 30)

    # Số người ngẫu nhiên theo cycle
    n_people = (frame_idx // 20) % 5 + 1

    # Vẽ hình người (rectangle đại diện)
    colors = [(0, 200, 0), (200, 0, 0), (0, 0, 200), (200, 200, 0), (0, 200, 200)]
    for i in range(n_people):
        x = 80 + i * 110
        y = 150
        w, h = 60, 120
        cv2.rectangle(frame, (x, y), (x+w, y+h), colors[i % len(colors)], -1)
        # Vẽ đầu
        cx, cy = x + w//2, y - 20
        cv2.circle(frame, (cx, cy), 18, colors[i % len(colors)], -1)

    # Timestamp text
    ts = time.strftime("%H:%M:%S")
    cv2.putText(frame, f"DEMO MODE | {ts} | People: {n_people}",
                (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
    cv2.putText(frame, f"Frame #{frame_idx}",
                (10, 460), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (150, 150, 150), 1)
    return frame


def run_camera(source: str):
    producer = connect_producer()
    frame_idx = 0
    interval = 1.0 / TARGET_FPS

    if source == "demo":
        cap = None
        print(f"[Camera] Chế độ DEMO – tự tạo frame.")
    else:
        src = int(source) if source.isdigit() else source
        cap = cv2.VideoCapture(src)
        if not cap.isOpened():
            print(f"[Camera] ❌ Không mở được nguồn: {source}")
            sys.exit(1)
        print(f"[Camera] 🎥 Đang đọc từ: {source}")

    print(f"[Camera] 📤 Publish lên topic '{TOPIC_RAW_FRAMES}' @ {TARGET_FPS} FPS")
    print(f"[Camera] ID: {CAMERA_ID}  |  Nhấn Ctrl+C để dừng\n")

    sent_count = 0
    try:
        while True:
            t_start = time.time()

            # Lấy frame
            if cap is None:
                frame = generate_demo_frame(frame_idx)
                ret = True
            else:
                ret, frame = cap.read()
                if not ret:
                    if isinstance(src, str): 
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        continue
                    print("[Camera] Webcam ngừng trả frame, dừng lại.")
                    break

            frame = resize_frame(frame, MAX_WIDTH)
            h, w = frame.shape[:2]

            # Encode & publish
            b64_frame = encode_frame(frame, JPEG_QUALITY)
            message = {
                "camera_id":   CAMERA_ID,
                "frame_id":    frame_idx,
                "timestamp":   time.time(),
                "width":       w,
                "height":      h,
                "frame_b64":   b64_frame,
            }

            future = producer.send(TOPIC_RAW_FRAMES, value=message)
            sent_count += 1
            frame_idx  += 1

            if sent_count % 10 == 0:
                print(f"[Camera] Đã gửi {sent_count} frames  (frame #{frame_idx})")

            # Giữ đúng FPS
            elapsed = time.time() - t_start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n[Camera] Dừng. Tổng frames đã gửi: {sent_count}")
    finally:
        producer.flush()
        producer.close()
        if cap:
            cap.release()
        print("[Camera] Producer đã đóng.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Camera Server – Kafka Producer")
    parser.add_argument(
        "--source", default="demo",
        help="Nguồn video: '0' (webcam), đường dẫn video, hoặc 'demo'"
    )
    args = parser.parse_args()
    run_camera(args.source)
