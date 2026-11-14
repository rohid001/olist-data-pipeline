-- Tabel tujuan event order dari Kafka (P2). Dibuar otomatis oleh image postgres saat volume
-- masih kosong, dan dijalankan juga oleh streaming/consumer.py setiap kali mulai, jadi aman
-- diajalankan berulang kali (IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS raw.order_events(
    event_id TEXT PRIMARY KEY,          -- <order_id>-<event_type>; kunci duplikasi
    event_type TEXT NOT NULL,           -- order_created, order_approved, order_shipped, order_delivered
    order_id TEXT NOT NULL,
    customer_id TEXT,
    event_ts TEXT NOT NULL,             -- watku kejadian (event time), dari CSV Olist
    estimated_delivery_ts TEXT,         -- hanya diisi di event order_created
    produced_at TEXT NOT NULL,          -- waktu producer mengirim pesan
    _topic TEXT NOT NULL,
    _partition INTEGER NOT NULL,
    _offset BIGINT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()      -- waktu tiba di warehouse
);

-- Model incremental dbt memfilter berdasarkan waktu tiba
CREATE INDEX IF NOT EXISTS ix_order_events_ingested_at ON raw.order_events (_ingested_at);
