"""
consumer.py
Membaca event order dari Kafka dan menulisnya ke raw.order_events, per batch kecil.
Urutannya: tulis ke warehouse dan commit transaksi dulu, baru commit offset Kafka.
Bila consumer mati di antara keduanya, batch itu dibaca ulang setelah restart, dan
ON CONFLICT (event_id) DO NOTHING membuang baris yang sudah ada. Hasilnya: tidak ada event
yang hilang dan tidak ada yang tercatat dua kali.
Jalankan dari root repo:
    python -m streaming.consumer                    # jalan terus sampai Ctrl+C
    python -m streaming.consumer --idle-timeout 15  # berhenti bila 15 detik tanpa pesan
"""
import argparse
import json
import os
import time

from confluent_kafka import Consumer
from psycopg2.extras import execute_values

from ingestion.config import get_engine
from streaming.settings import CLIENT_CONFIG, CONSUMER_GROUP, EVENTS_DDL, TOPIC

COLUMNS = ["event_id", "event_type", "order_id", "customer_id", "event_ts",
           "estimated_delivery_ts", "produced_at", "_topic", "_partition", "_offset"]
BATCH_SIZE = 500
REPORT_EVERY_SECONDS = 5

#--------------------------------------------------------------------------Tahap A
def to_row(message) -> tuple:
    """Pesan Kafka -> satu baris raw.order_events, termasuk asal pesan (topic, partisi, offset)."""
    event = json.loads(message.value())
    return (event["event_id"], event["event_type"], event["order_id"], event["customer_id"],
            event["event_ts"], event["estimated_delivery_ts"], event["produced_at"],
            message.topic(), message.partition(), message.offset())

def write_batch(conn, rows: list) -> int:
    """Tulis satu batch dalam satu transaksi; kembalikan jumlah baris yang benar-benar baru."""
    with conn.cursor() as cur:
        # page_size = jumlah baris: satu statemnet, jadi rowcount = baris yang masuk
        execute_values(cur, f"INSERT INTO raw.order_events ({', '.join(COLUMNS)}) VALUES %s "
                       "ON CONFLICT (event_id) DO NOTHING", rows, page_size=len(rows))
        inserted = cur.rowcount
    conn.commit()
    return inserted

#--------------------------------------------------------------------------Tahap B
def run(idle_timeout: float | None, crash_after: int | None) -> dict:
    """Konsumsi pesan sampai Ctrl+C atau idle_timeout detik tanpa pesan."""
    conn = get_engine().raw_connection()
    with conn.cursor() as cur:
        cur.execute(EVENTS_DDL.read_text(encoding="utf-8"))
    conn.commit()

    consumer = Consumer({
        **CLIENT_CONFIG,
        "group.id": CONSUMER_GROUP,
        # Consumer group baru mulai dari pesan paling awal di topic
        "auto.offset.reset": "earliest",
        # Offset di-commit manual, hanya setelah data aman di warehouse
        "enable.auto.commit": False,
        # Consumer yang mati dikeluarkan dari group setelah 10 detik (bawaan 45 detik),
        # jadi consumer pengganti cepat mendapat partisinya
        "session.timeout.ms": 10000,
    })
    stats = {"messages":0, "inserted": 0}
    # Hitungan idle baru mulai setelah partisi dibagikan ke consumer ini (rebalance selesai)
    clock = {"last_message": None}

    def on_assign(consumer, partitions) -> None:
        print(f"Assigned partitions: {sorted(p.partition for p in partitions)}")
        clock["last_message"] = time.monotonic()

    consumer.subscribe([TOPIC], on_assign=on_assign)
    last_report = time.monotonic()
    try:
        while True:
            if idle_timeout and clock["last_message"] and time.monotonic() - clock["last_message"] > idle_timeout:
                break
            messages = [m for m in consumer.consume(num_messages=BATCH_SIZE, timeout=1.0) if not m.error()]
            if not messages:
                continue
            clock["last_message"] = time.monotonic()
            stats["inserted"] += write_batch(conn, [to_row(m) for m in messages])
            stats["messages"] += len(messages)
            if crash_after and stats["messages"] >= crash_after:
                # Uji: mati mendadak setelah commit warehouse, SEBELUM commit offset
                print(f"Simulated crash after {stats['messages']:,} messages (offsets not committed)")
                os._exit(1)
            consumer.commit(asynchronous=False)
            if time.monotonic() - last_report >= REPORT_EVERY_SECONDS:
                print(f"{stats['messages']:>8,} messages read, {stats['inserted']:>8,} new events written")
                last_report = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()
    return stats

def parse_args() -> argparse.Namespace:
    """Baca argumne baris perintah."""
    parser = argparse.ArgumentParser(description="Load Olist order events from Kafka into raw.order_events.")
    parser.add_argument("--idle-timeout", type=float, help="stop after this many seconds without messages")
    parser.add_argument("--crash-after", type=int, help="test only: exit abruptly after this many messages")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    stats = run(args.idle_timeout, args.crash_after)
    skipped = stats["messages"] - stats["inserted"]
    print(f"Done: {stats['messages']:,} messages, {stats['inserted']:,} new events, "
          f"{skipped:,} duplicates skipped")
