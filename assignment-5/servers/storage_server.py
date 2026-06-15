"""
servers/storage_server.py  –  Storage Server (Kafka Consumer + SQLite + REST API)
==================================================================================
Chức năng:
  - Subscribe Kafka topic "detection_results"
  - Lưu kết quả vào SQLite database
  - Export CSV
"""

import csv
import io
import json
import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime

from flask import Flask, jsonify, Response
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP    = "localhost:9092"
TOPIC_DETECTION    = "detection_results"
GROUP_ID           = "storage-group"
DB_PATH            = os.path.join(os.path.dirname(__file__), "..", "results", "people_count.db")
API_PORT           = 5001

app = Flask(__name__)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def init_db():
    """Tạo schema nếu chưa tồn tại."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS detections (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                camera_id     TEXT    NOT NULL,
                frame_id      INTEGER NOT NULL,
                capture_ts    REAL    NOT NULL,
                process_ts    REAL    NOT NULL,
                latency_ms    REAL,
                inference_ms  REAL,
                width         INTEGER,
                height        INTEGER,
                person_count  INTEGER NOT NULL DEFAULT 0,
                bounding_boxes TEXT,          -- JSON string
                annotated_b64  TEXT,          -- base64 JPEG
                created_at    TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_camera_ts
            ON detections (camera_id, capture_ts DESC)
        """)
        conn.commit()
    print(f"[Storage] 💾 Database: {os.path.abspath(DB_PATH)}")


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def save_detection(record: dict):
    """Lưu 1 kết quả phát hiện vào database."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO detections
              (camera_id, frame_id, capture_ts, process_ts,
               latency_ms, inference_ms, width, height,
               person_count, bounding_boxes, annotated_b64)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record.get("camera_id", "unknown"),
            record.get("frame_id", 0),
            record.get("capture_ts", time.time()),
            record.get("process_ts", time.time()),
            record.get("latency_ms"),
            record.get("inference_ms"),
            record.get("width"),
            record.get("height"),
            record.get("person_count", 0),
            json.dumps(record.get("bounding_boxes", [])),
            record.get("annotated_b64", ""),
        ))
        conn.commit()


def row_to_dict(row) -> dict:
    d = dict(row)
    # Parse JSON fields
    try:
        d["bounding_boxes"] = json.loads(d.get("bounding_boxes") or "[]")
    except Exception:
        d["bounding_boxes"] = []
    # Không trả ảnh base64 trong list results (nặng)
    d.pop("annotated_b64", None)
    # Format timestamp
    if d.get("capture_ts"):
        d["capture_time"] = datetime.fromtimestamp(d["capture_ts"]).strftime("%H:%M:%S")
    return d


@app.route("/api/results")
def api_results():
    """100 kết quả phát hiện gần nhất."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM detections
            ORDER BY id DESC LIMIT 100
        """).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route("/api/stats")
def api_stats():
    """Thống kê tổng hợp."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
        if total == 0:
            return jsonify({"total_frames": 0, "message": "Chưa có dữ liệu"})

        row = conn.execute("""
            SELECT
                COUNT(*)                   AS total_frames,
                AVG(person_count)          AS avg_people,
                MAX(person_count)          AS max_people,
                MIN(person_count)          AS min_people,
                AVG(latency_ms)            AS avg_latency_ms,
                AVG(inference_ms)          AS avg_inference_ms,
                COUNT(DISTINCT camera_id)  AS camera_count
            FROM detections
        """).fetchone()

        # Phân phối số người
        dist_rows = conn.execute("""
            SELECT person_count, COUNT(*) AS cnt
            FROM detections
            GROUP BY person_count
            ORDER BY person_count
        """).fetchall()

        # 10 phút gần nhất (theo phút)
        timeline = conn.execute("""
            SELECT
                strftime('%H:%M', datetime(capture_ts,'unixepoch','localtime')) AS minute,
                ROUND(AVG(person_count),1) AS avg_count,
                MAX(person_count) AS max_count
            FROM detections
            WHERE capture_ts >= strftime('%s','now') - 600
            GROUP BY minute
            ORDER BY minute
        """).fetchall()

        latest = conn.execute("""
            SELECT person_count, created_at FROM detections ORDER BY id DESC LIMIT 1
        """).fetchone()

    return jsonify({
        "total_frames":      row["total_frames"],
        "avg_people":        round(row["avg_people"] or 0, 2),
        "max_people":        row["max_people"] or 0,
        "min_people":        row["min_people"] or 0,
        "avg_latency_ms":    round(row["avg_latency_ms"] or 0, 1),
        "avg_inference_ms":  round(row["avg_inference_ms"] or 0, 1),
        "camera_count":      row["camera_count"],
        "distribution":      {str(r["person_count"]): r["cnt"] for r in dist_rows},
        "timeline_10min":    [dict(r) for r in timeline],
        "latest_count":      latest["person_count"] if latest else 0,
        "latest_time":       latest["created_at"] if latest else None,
    })


@app.route("/api/latest")
def api_latest():
    """Frame mới nhất kèm ảnh annotated."""
    with get_db() as conn:
        row = conn.execute("""
            SELECT * FROM detections ORDER BY id DESC LIMIT 1
        """).fetchone()
    if not row:
        return jsonify({"message": "Chưa có dữ liệu"}), 404

    d = dict(row)
    try:
        d["bounding_boxes"] = json.loads(d.get("bounding_boxes") or "[]")
    except Exception:
        d["bounding_boxes"] = []

    if d.get("capture_ts"):
        d["capture_time"] = datetime.fromtimestamp(d["capture_ts"]).strftime("%H:%M:%S")
    return jsonify(d)


@app.route("/api/export/csv")
def api_export_csv():
    """Export toàn bộ dữ liệu thành CSV."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, camera_id, frame_id,
                   datetime(capture_ts,'unixepoch','localtime') AS capture_time,
                   datetime(process_ts,'unixepoch','localtime') AS process_time,
                   latency_ms, inference_ms, width, height, person_count, created_at
            FROM detections ORDER BY id
        """).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id","camera_id","frame_id","capture_time","process_time",
        "latency_ms","inference_ms","width","height","person_count","created_at"
    ])
    for row in rows:
        writer.writerow(list(row))

    csv_path = os.path.join(os.path.dirname(DB_PATH), "export.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write(output.getvalue())

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=people_count_export.csv"}
    )


@app.route("/api/health")
def api_health():
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    return jsonify({"status": "ok", "records": count, "timestamp": time.time()})

# consumer

def connect_consumer(retries: int = 10) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_DETECTION,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                max_partition_fetch_bytes=15_728_640,
            )
            print("[Storage] ✅ Consumer kết nối Kafka thành công")
            return consumer
        except NoBrokersAvailable:
            print(f"[Storage] ⏳ Retry {i+1}/{retries}...")
            time.sleep(3)
    print("[Storage] ❌ Không thể kết nối Kafka.")
    sys.exit(1)


def kafka_consumer_thread():
    """Chạy trong background thread – lắng nghe và lưu dữ liệu."""
    consumer = connect_consumer()
    saved = 0
    print(f"[Storage] 🔄 Lắng nghe topic '{TOPIC_DETECTION}' ...\n")
    try:
        for msg in consumer:
            record = msg.value
            save_detection(record)
            saved += 1
            print(
                f"[Storage] 💾 Saved #{saved} | "
                f"cam={record.get('camera_id','?')} | "
                f"frame={record.get('frame_id','?')} | "
                f"👥 {record.get('person_count',0)} người"
            )
    except Exception as e:
        print(f"[Storage] ❌ Consumer lỗi: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    init_db()

    # Khởi Kafka consumer trong background thread
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    print(f"\n[Storage] 🌐 REST API tại http://localhost:{API_PORT}")
    print(f"[Storage]   GET /api/results       – kết quả gần nhất")
    print(f"[Storage]   GET /api/stats          – thống kê")
    print(f"[Storage]   GET /api/latest         – frame mới nhất + ảnh")
    print(f"[Storage]   GET /api/export/csv     – tải CSV\n")

    app.run(host="0.0.0.0", port=API_PORT, debug=False, use_reloader=False)
