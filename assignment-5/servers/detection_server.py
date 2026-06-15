"""
servers/detection_server.py  –  Detection Server (Kafka Consumer + Producer)
=============================================================================
Chức năng:
  - Subscribe Kafka topic "raw_frames"
  - Decode frame từ base64
  - Publish kết quả bounding boxes lên Kafka topic "detection_results"
"""

import argparse
import base64
import json
import sys
import time

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP       = "localhost:9092"
TOPIC_RAW_FRAMES      = "raw_frames"
TOPIC_DETECTION       = "detection_results"
GROUP_ID              = "detection-group"
CONF_THRESHOLD        = 0.4   # yolo threshold
NMS_THRESHOLD         = 0.3  


class HOGDetector:
    """
    HOG + SVM người (built-in OpenCV).
    """
    def __init__(self):
        self.hog = cv2.HOGDescriptor()
        self.hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
        print("[Detection] 🔍 Sử dụng HOG + SVM (OpenCV built-in)")

    def detect(self, frame: np.ndarray) -> list[dict]:
        """
        Trả về list các bounding box:
          [{"x1": int, "y1": int, "x2": int, "y2": int, "confidence": float, "label": "person"}, ...]
        """
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        rects, weights = self.hog.detectMultiScale(
            gray,
            winStride=(8, 8),
            padding=(4, 4),
            scale=1.05,
        )

        boxes = []
        if len(rects) == 0:
            return boxes

        rects_nms = np.array([[x, y, x+w, y+h] for (x, y, w, h) in rects])
        weights_flat = weights.flatten()

        idxs = cv2.dnn.NMSBoxes(
            [[int(r[0]), int(r[1]), int(r[2]-r[0]), int(r[3]-r[1])] for r in rects_nms],
            weights_flat.tolist(),
            score_threshold=0.1,
            nms_threshold=NMS_THRESHOLD,
        )

        if len(idxs) > 0:
            for i in idxs.flatten():
                x1, y1, x2, y2 = rects_nms[i]
                boxes.append({
                    "x1": int(x1), "y1": int(y1),
                    "x2": int(x2), "y2": int(y2),
                    "confidence": float(weights_flat[i]),
                    "label": "person",
                })
        return boxes


class YOLODetector:
    """
    phiên bản yolov8
    """
    def __init__(self):
        try:
            from ultralytics import YOLO
            self.model = YOLO("yolov8n.pt") 
            print("[Detection] 🚀 Sử dụng YOLOv8n")
        except ImportError:
            print("[Detection] ❌ ultralytics chưa cài. Chạy: pip install ultralytics")
            sys.exit(1)

    def detect(self, frame: np.ndarray) -> list[dict]:
        results = self.model(frame, conf=CONF_THRESHOLD, classes=[0], verbose=False)
        boxes = []
        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                conf = float(box.conf[0])
                boxes.append({
                    "x1": int(x1), "y1": int(y1),
                    "x2": int(x2), "y2": int(y2),
                    "confidence": round(conf, 3),
                    "label": "person",
                })
        return boxes


# ══════════════════════════════════════════════════════════════════════════════
#  Kafka helpers
# ══════════════════════════════════════════════════════════════════════════════

def connect_consumer(retries: int = 10) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_RAW_FRAMES,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                max_partition_fetch_bytes=15_728_640,   # 15 MB
                fetch_max_bytes=52_428_800,             # 50 MB
            )
            print("[Detection] ✅ Consumer kết nối Kafka thành công")
            return consumer
        except NoBrokersAvailable:
            print(f"[Detection] ⏳ Retry {i+1}/{retries}...")
            time.sleep(3)
    print("[Detection] ❌ Không thể kết nối Kafka.")
    sys.exit(1)


def connect_producer(retries: int = 10) -> KafkaProducer:
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type="gzip",
            )
            print("[Detection] ✅ Producer kết nối Kafka thành công")
            return producer
        except NoBrokersAvailable:
            print(f"[Detection] ⏳ Retry {i+1}/{retries}...")
            time.sleep(3)
    print("[Detection] ❌ Không thể kết nối Kafka.")
    sys.exit(1)


def decode_frame(b64_str: str) -> np.ndarray:
    """Giải mã base64 → numpy array BGR."""
    raw = base64.b64decode(b64_str)
    arr = np.frombuffer(raw, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


# ══════════════════════════════════════════════════════════════════════════════
#  Main loop
# ══════════════════════════════════════════════════════════════════════════════

def run_detection(detector_name: str):
    detector = YOLODetector() if detector_name == "yolo" else HOGDetector()
    consumer = connect_consumer()
    producer = connect_producer()

    print(f"[Detection] 🔄 Đang lắng nghe topic '{TOPIC_RAW_FRAMES}' ...\n")

    processed = 0
    total_latency = 0.0

    try:
        for msg in consumer:
            data = msg.value
            camera_id  = data.get("camera_id", "unknown")
            frame_id   = data.get("frame_id", 0)
            cam_ts     = data.get("timestamp", time.time())
            width      = data.get("width", 0)
            height     = data.get("height", 0)
            b64_frame  = data.get("frame_b64", "")

            if not b64_frame:
                continue

            # Decode frame
            frame = decode_frame(b64_frame)
            if frame is None:
                print(f"[Detection] ⚠️  Không decode được frame #{frame_id}")
                continue

            # ── Nhận diện ─────────────────────────────────────────────────
            t0 = time.time()
            boxes = detector.detect(frame)
            inference_ms = round((time.time() - t0) * 1000, 1)

            person_count = len(boxes)
            processed   += 1
            latency_ms   = round((time.time() - cam_ts) * 1000, 1)
            total_latency += latency_ms

            vis_frame = frame.copy()
            for b in boxes:
                cv2.rectangle(vis_frame,
                              (b["x1"], b["y1"]), (b["x2"], b["y2"]),
                              (0, 255, 0), 2)
                label_txt = f"person {b['confidence']:.2f}"
                cv2.putText(vis_frame, label_txt,
                            (b["x1"], b["y1"] - 5),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)

            cv2.putText(vis_frame,
                        f"Count: {person_count} | {time.strftime('%H:%M:%S')}",
                        (10, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)

            _, vis_buf = cv2.imencode(".jpg", vis_frame,
                                      [cv2.IMWRITE_JPEG_QUALITY, 60])
            vis_b64 = base64.b64encode(vis_buf).decode("utf-8")

            result = {
                "camera_id":       camera_id,
                "frame_id":        frame_id,
                "capture_ts":      cam_ts,
                "process_ts":      time.time(),
                "latency_ms":      latency_ms,
                "inference_ms":    inference_ms,
                "width":           width,
                "height":          height,
                "person_count":    person_count,
                "bounding_boxes":  boxes,
                "annotated_b64":   vis_b64,
            }
            producer.send(TOPIC_DETECTION, value=result)

            # Log
            avg_lat = round(total_latency / processed, 1)
            print(
                f"[Detection] Frame #{frame_id:05d} | "
                f"👥 {person_count} người | "
                f"⏱ infer {inference_ms}ms | "
                f"📡 latency {latency_ms}ms (avg {avg_lat}ms)"
            )

    except KeyboardInterrupt:
        print(f"\n[Detection] Dừng. Đã xử lý {processed} frames.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("[Detection] Đã đóng kết nối.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detection Server – Kafka Consumer/Producer")
    parser.add_argument(
        "--detector", choices=["hog", "yolo"], default="hog",
        help="Chọn detector: 'hog' hoặc 'yolo'"
    )
    args = parser.parse_args()
    run_detection(args.detector)
