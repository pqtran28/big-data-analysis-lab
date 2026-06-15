"""
utils/setup_topics.py
Tạo các Kafka topics cần thiết cho hệ thống.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = [
    NewTopic(
        name="raw_frames",
        num_partitions=3,     
        replication_factor=1,
    ),
    NewTopic(
        name="detection_results",
        num_partitions=3,
        replication_factor=1,
    ),
]


def create_topics():
    print("[Setup] Đang kết nối Kafka tại", KAFKA_BOOTSTRAP)
    retries = 5
    for i in range(retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="setup-client",
            )
            break
        except Exception as e:
            print(f"[Setup] Retry {i+1}/{retries} – {e}")
            time.sleep(3)
    else:
        print("[Setup] FAIL: Không kết nối được Kafka. Hãy chắc chắn Kafka đang chạy.")
        return

    created, skipped = [], []
    for topic in TOPICS:
        try:
            admin.create_topics(new_topics=[topic], validate_only=False)
            created.append(topic.name)
        except TopicAlreadyExistsError:
            skipped.append(topic.name)
        except Exception as e:
            print(f"[Setup] Lỗi tạo topic '{topic.name}': {e}")

    admin.close()
    if created:
        print(f"[Setup] ✅ Đã tạo topics: {created}")
    if skipped:
        print(f"[Setup] ⚠️  Topics đã tồn tại (bỏ qua): {skipped}")
    print("[Setup] Hoàn tất.")


if __name__ == "__main__":
    create_topics()
