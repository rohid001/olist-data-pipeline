"""
producer.py
Memutar ulang riwayat order Olist sebagai event ke Kafka. Setiap timestamp di tabel orders
menjadi satu event (dibuat, disetujui, dikirim, diterima), dikirim sesuai urutan waktunya.
Sebagian kecil event sengaja dikirim terlambat atau dikirim dua kali, untuk meniru masalah
nyata di sistem streaming.
Jalankan dari root repo:
    python -m streaming.producer --month 2017-11
    python -m streaming.producer --month 2017-11 --speed 86400      # 1 hari data per detik
    python -m streaming.producer --all
"""
import argparse
import json
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import pandas as pd
from confluent_kafka import Producer

from ingestion.data_contract import CONTRACT
from ingestion.partition import SOURCE_DIR
from streaming.settings import CLIENT_CONFIG, TOPIC, ensure_topic

# Kolom timestamp di tabel orders -> jenis event, sesuai urutan tahap order
EVENT_COLUMNS = {
    "order_created": "order_purchase_timestamp",
    "order_approved": "order_approved_at",
    "order_shipped": "order_delivered_carrier_date",
    "order_delivered": "order_delivered_customer_date",
}
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Nomor tahap, untuk memecah seri bila dua tahap punya timestamp yang sama persis
# (di data asli, 1.295 order Maret 2017 disetujui tepat pada detik order dibuat)
STAGE = {event_type: number for number, event_type in enumerate(EVENT_COLUMNS, start=1)}

#--------------------------------------------------------------------------------------Tahap A
def build_events(orders: pd.DataFrame) -> list:
    """Ubah tabel orders menjadi daftar event, diurutkan menurut waktu kejadian."""
    events = []
    for event_type, column in EVENT_COLUMNS.items():
        # Timestamp kosong = tahap itu belum (atau tidak pernah) terjadi, jadi tidak ada event
        rows = orders[orders[column].notna()]
        for row in rows.itertuples(index=False):
            events.append({
                "event_id": f"{row.order_id}-{event_type}",
                "event_type": event_type,
                "order_id": row.order_id,
                "customer_id": row.customer_id,
                "event_ts": getattr(row, column),
                # Perkiraan tanggal tiba hanya diketahui saat order dibuat
                "estimated_delivery_ts": (row.order_estimated_delivery_date
                                          if event_type == "order_created" else None),
            })
    # Format "YYY-MM-DD HH:MM:SS" bisa diurutkan sebagai teks
    return sorted(events, key=lambda e: (e["event_ts"], STAGE[e["event_type"]], e["order_id"]))

def load_events(month: str | None) -> list:
    """Baca CSV orders dari data/source, lalu ambil event bulan 'month' (None = semua)."""
    path = SOURCE_DIR / CONTRACT["orders"]["file"]
    orders = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    events = build_events(orders)
    if month:
        events = [e for e in events if e["event_ts"][:7] == month]
    return events

#--------------------------------------------------------------------------------------Tahap B
def plan_publication(events: list, late_ratio: float, duplicate_ratio: float, seed: int) -> list:
    """Tentukan kapan setiap event dikirim. Kembalikan daftar (waktu kirim, jenis event), terurut.
        Normalnya event dikirim tepat pada waktu kejadiannya ("on_time"). Sebagian (late_ratio)
        tertunda 1-72 jam ("late"), sehingga bisa tiba setelah tahap berikutnya dari order yang sama.
        Sebagian lagi (duplicate_ratio) dikirim ulang 0-6 jam kemudian ("duplicate"), seperti producer
        yang retry setelah timeout.
    """
    rng = random.Random(seed)   # seed tetap: hasil simulasi sama setiap kali dijalankan
    plan = []
    for event in events:
        sent_at = datetime.strptime(event["event_ts"], TIMESTAMP_FORMAT)
        kind = "on_time"
        if rng.random() < late_ratio:
            sent_at += timedelta(hours=rng.uniform(1, 72))
            kind = "late"
        plan.append((sent_at, kind, event))
        if rng.random() < duplicate_ratio:
            plan.append((sent_at + timedelta(hours=rng.uniform(0, 6)), "duplicate", event))
    return sorted(plan, key=lambda item: (item[0], STAGE[item[2]["event_type"]], item[2]["order_id"]))

#--------------------------------------------------------------------------------------Tahap C
def publish(plan: list, speed: float) -> dict:
    """Kirim event ke Kafka sesuai rencana. speed = detik simulasi per detik nyata (0 = tanpa jeda)."""
    # enable.idempotence: broker membuang pesan kembar akibat retry jaringan producer.
    # Duplikat yang sengaja dibuat di plan_publication tetap terkirim, karena itu pesan baru.
    producer = Producer({**CLIENT_CONFIG, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    stats = {"produced": 0, "errors": 0}

    def on_delivery(error, message) -> None:
        stats["errors" if error else "produced"] += 1

    previous = None
    for sent_at, _, event in plan:
        if speed and previous:
            time.sleep((sent_at - previous).total_seconds() / speed)
        previous = sent_at
        message = {**event, "produced_at": datetime.now(timezone.utc).isoformat()}
        while True:
            try:
                # Key = order_id: semua event satu order masuk ke partisi yang sama
                producer.produce(TOPIC, key=event["order_id"], value=json.dumps(message),
                                 on_delivery=on_delivery)
                break
            except BufferError:
                # Antrean lokal penuh: tunggu sebagian pesan terkirim dulu
                producer.poll(0.5)
        producer.poll(0)
    producer.flush(30)
    return stats

def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah."""
    parser = argparse.ArgumentParser(description="Replay Olist order history as Kafka events.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="events whose event time falls in this month, e.g. 2017-11")
    group.add_argument("--all", action="store_true", help="all events")
    parser.add_argument("--speed", type=float, default=0,
                        help="simulated seconds per real second (0 = as fast as possible)")
    parser.add_argument("--late-ratio", type=float, default=0.05, help="share of events sent late")
    parser.add_argument("--duplicate-ratio", type=float, default=0.02, help="share of events sent twice")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    events = load_events(args.month)
    plan = plan_publication(events, args.late_ratio, args.duplicate_ratio, args.seed)
    print(f"Events:         {len(events):>8,}   ({len({e['order_id'] for e in events}):,} orders)")
    kinds = Counter(kind for _, kind, _ in plan)
    print(f"Sent late:      {kinds['late']:>8,}")
    print(f"Duplicates:     {kinds['duplicate']:>8,}")

    ensure_topic()
    stats = publish(plan, args.speed)
    print(f"Message sent:   {stats['produced']:>8,} to  {TOPIC}, {stats['errors']} delivery errors")
