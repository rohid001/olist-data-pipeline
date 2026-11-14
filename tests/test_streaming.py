"""
test_streaming.py
Unit test untuk logika producer dan consumer yang tidak butuh Kafka maupun database:
membentuk event dari tabel orders, rencana pengiriman (terlambat, duplikat), dan
mengubah pesan Kafka menjadi baris tabel.
Jalankan dari root repo: python -m pytest
"""
import json

import pandas as pd

from streaming.consumer import COLUMNS, to_row
from streaming.producer import build_events, plan_publication


def make_orders() -> pd.DataFrame:
    """2 order: o1 sudah diterima pelanggan, o2 baru dibuat (timestamp lain kosong)."""
    return pd.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c2"],
        "order_purchase_timestamp": ["2017-11-01 10:00:00", "2017-11-02 09:00:00"],
        "order_approved_at": ["2017-11-01 10:15:00", None],
        "order_delivered_carrier_date": ["2017-11-03 08:00:00", None],
        "order_delivered_customer_date": ["2017-11-10 17:00:00", None],
        "order_estimated_delivery_date": ["2017-11-20 00:00:00", "2017-11-25 00:00:00"],
    })

def test_build_events_skips_empty_timestamps():
    events = build_events(make_orders())
    # o1 punya 4 tahap, o2 hanya order_created
    assert [e["event_id"] for e in events] == [
        "o1-order_created", "o1-order_approved", "o2-order_created",
        "o1-order_shipped", "o1-order_delivered",
    ]

def test_same_timestamp_keeps_stage_order():
    # Disutujui pada detik yang sama dengan dibuat: order_created tetap dikirim lebih dulu
    orders = make_orders().assign(order_approved_at=["2017-11-01 10:00:00", None])
    events = [e["event_id"] for e in build_events(orders)]
    assert events.index("o1-order_created") < events.index("o1-order_approved")

def test_estimated_delivery_only_on_created_event():
    events = {e["event_id"]: e for e in build_events(make_orders())}
    assert events["o1-order_created"]["estimated_delivery_ts"] == "2017-11-20 00:00:00"
    assert events["o1-order_approved"]["estimated_delivery_ts"] is None

def test_plan_without_late_or_duplicates_keeps_event_order():
    events = build_events(make_orders())
    plan = plan_publication(events, late_ratio=0, duplicate_ratio=0, seed=1)
    assert [event for _, _, event in plan] == events
    assert {kind for _, kind, _ in plan} == {"on_time"}

def test_plan_with_duplicates_sends_every_event_twice():
    events = build_events(make_orders())
    plan = plan_publication(events, late_ratio=0, duplicate_ratio=1, seed=1)
    assert len(plan) == 2 * len(events)
    # Duplikat memakai event_id yang sama, jadi bisa dibuang di warehouse
    assert {e["event_id"] for _, _, e in plan} == {e["event_id"] for e in events}

def test_plan_is_reporducible_with_same_seed():
    events = build_events(make_orders())
    first = plan_publication(events, late_ratio=0.5, duplicate_ratio=0.5, seed=7)
    second = plan_publication(events, late_ratio=0.5, duplicate_ratio=0.5, seed=7)
    assert first == second

class FakeMessage:
    """Pengganti pesan confluent_kafka untuk test: hanya method yang dipakai to_row."""

    def __init__(self, value: dict, partition: int, offset: int):
        self._value = json.dumps(value).encode()
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def topic(self):
        return "olist.order_events"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

def test_to_row_matches_table_columns():
    event = build_events(make_orders())[0]
    message = FakeMessage({**event, "produced_at": "2026:11:09T10:00:00+00:00"},
                          partition=2, offset=41)
    row = to_row(message)
    assert len(row) == len(COLUMNS)
    assert dict(zip(COLUMNS, row))["event_id"] == "o1-order_created"
    assert row[-3:] == ("olist.order_events", 2, 41)
