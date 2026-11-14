"""
settings.py
Pengaturan bersama producer dan consumer: alamat Kafka, nama topic, consumer group.
dan pembuatan topic bila belum ada.
Jalankan dari root repo untuk mengecek koneksi ke Kafka dan membuat topic
    python -m streaming.settings
"""
import os

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from ingestion.config import ENV_FILE, ROOT_DIR

load_dotenv(ENV_FILE)

# Dari laptop/PC Kafka dicapai di localhost:9092 (port yang dibuka docker-compose.yml)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "olist.order_events"
# 3 partisi: pesan dengan key (order_id) yang sama selalu masuk partisi yang sama,
# jadi urutan event satu order tetap terjaga, sementara order berbeda bisa diproses paralel
PARTITIONS = 3
CONSUMER_GROUP = "olist-warehouse-loader"
EVENTS_DDL = ROOT_DIR / "warehouse" / "init" / "02_create_order_events.sql"

# Konfigurasi dasar semua client. enable.metrics.push=false: jangan minta langganan
# metrik ke broker (KIP-714), supaya log tidak berisi pesan telemetri.
CLIENT_CONFIG = {"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.metrics.push":False}

def ensure_topic() -> bool:
    """Buat topic jika belum ada. Kembalikan True jika baru dibuat."""
    admin = AdminClient(CLIENT_CONFIG)
    existing = admin.list_topics(timeout=10).topics
    if TOPIC in existing:
        return False
    # replication_factor=1 karena broker hanya satu (laptop/PC dan CI)
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])
    futures[TOPIC].result()
    return True

if __name__ == "__main__":
    created = ensure_topic()
    status = "created" if created else "already exists"
    print(f"Kafka at {BOOTSTRAP_SERVERS}: topic {TOPIC} {status} ({PARTITIONS} partitions)")
