"""
stream_report.py
Ringkasan terukur hasil streaming: pesan di Kafka vs baris di warehouse (duplikat yang
dibuang), event yang tiba tidak berurutan, latency producer -> warehouse, dan consumer
lag.
Jalankan dari root repo setelah consumer selesai membaca:
    python -m streaming.stream_report
"""
from confluent_kafka import Consumer, KafkaException, TopicPartition
from sqlalchemy import text

from ingestion.config import get_engine
from streaming.settings import CLIENT_CONFIG, CONSUMER_GROUP, PARTITIONS, TOPIC

# Urutan tahap order, untuk mendeteksi event yang tiba setelah tahap berikutnya
STEP_SQL = """case event_type when 'order_created' then 1 when 'order_approved' then 2
when 'order_shipped' then 3 else 4 end"""

WAREHOUSE_SQL = f"""
with events as (
    select
        *,
        {STEP_SQL} as step,
        -- Event satu order selalu di partisi yang sama, jadi urutan offset = urutan tiba
        max({STEP_SQL}) over (
            partition by order_id order by _offset
            rows between unbounded preceding and 1 preceding
        ) as max_step_before
    from raw.order_events
)
select
    count(*) as events,
    count(distinct order_id) as orders,
    count(*) filter (where step < max_step_before) as out_of_order,
    percentile_cont(0.5) within group (
        order by extract(epoch from _ingested_at - produced_at::timestamptz)) as p50_seconds,
    percentile_cont(0.95) within group (
        order by extract(epoch from _ingested_at - produced_at::timestamptz)) as p95_seconds
from events
"""

#----------------------------------------------------------------------------Tahap A
def kafka_offsets() -> tuple:
    """(jumlah pesan di topic, total consumer lag) dari watermark dan offset yang di commit."""
    consumer = Consumer({**CLIENT_CONFIG, "group.id": CONSUMER_GROUP})
    partitions = [TopicPartition(TOPIC, p) for p in range(PARTITIONS)]
    try:
        committed = consumer.committed(partitions, timeout=10)
    except KafkaException:
        # Group belum ada (consumer belum pernah commit): anggap semua pesan belum dibaca
        committed = [TopicPartition(TOPIC, p, -1) for p in range(PARTITIONS)]
    messages = lag = 0
    for tp in committed:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)
        messages += high - low
        # Offset negatif = group belum pernah commit di partisi ini
        lag += high - (tp.offset if tp.offset >= 0 else low)
    consumer.close()
    return messages, lag

def warehouse_stats(engine) -> dict:
    """Jumlah event, order, event tidak berurutan, dan latency dari raw.order_events."""
    with engine.connect() as conn:
        return dict(conn.execute(text(WAREHOUSE_SQL)).mappings().one())

if __name__ == "__main__":
    messages, lag = kafka_offsets()
    stats = warehouse_stats(get_engine())
    print(f"Kafka messages in topic:    {messages:9,}")
    print(f"Events in warehouse:        {stats['events']:9,}    ({stats['orders']:,} orders)")
    # Selisih pesan dari baris hanya berarti dupliat bila consumer sudah membaca semuanya
    duplicates = f"{messages - stats['events']:>9,}" if lag == 0 else "         n/a (consumer lag > 0)"
    print(f"Duplicates dropped:         {duplicates}")
    print(f"Arrived out of order:       {stats['out_of_order']:>9,}")
    if stats["events"]:
        print(f"Latency p50 / p95:      {stats['p50_seconds']:>9.2f} s / {stats['p95_seconds']:.2f} s")
    print(f"Consumer lag:               {lag:>9,}")
