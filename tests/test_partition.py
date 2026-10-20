"""
test_partition.py
Unit test untuk build_partition dan to_month dengan data kecil buatan sendiri,
jedi bisa jalan tanpa CSV Olist dan tanpa database.
Jalankan dari root repo: python -m pytest
"""
import pandas as pd
import pytest

from ingestion.partition import build_partition, to_month

def make_data() -> dict:
    """Data mini: 3 order (2 di januari, 1 di februari) beserta tabel turunannya."""
    orders = pd.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c3"],
        "order_purchase_timestamp": [
            "2017-01-05 10:00:00", "2017-01-20 08:30:00", "2017-02-01 00:00:00",
        ],
    })
    order_items = pd.DataFrame({
        "order_id": ["o1", "o1", "o2", "o3"],
        "order_item_id": ["1", "2", "1", "1"],
        "product_id": ["p1", "p1", "p2", "p1"],
        "seller_id": ["s1", "s1", "s1", "s2"],
    })
    order_reviews = pd.DataFrame({
        "review_id": ["r1", "r2"],
        "order_id": ["o1", "o2"],
        # r2 untuk order Januari, tapi baru dibuat Februari (late-arriving)
        "review_creation_date": ["2017-01-10 00:00:00", "2017-02-03 00:00:00"],
    })
    customers = pd.DataFrame({"customer_id": ["c1", "c2", "c3"]})
    products = pd.DataFrame({"product_id": ["p1", "p2", "p3"]})
    return {
        "orders": orders,
        "order_items": order_items,
        "order_reviews": order_reviews,
        "customers": customers,
        "products": products,
    }

def test_time_column_select_only_that_month():
    result = build_partition(make_data(), "orders", "2017-01")
    assert list(result["order_id"]) == ["o1", "o2"]
    
def test_child_rows_follow_parent_month():
    result = build_partition(make_data(), "order_items", "2017-01")
    assert list(result["order_id"]) == ["o1", "o1", "o2"]
    
def test_customers_follow_their_order():
    result = build_partition(make_data(), "customers", "2017-02")
    assert list(result["customer_id"]) == ["c3"]


def test_late_review_goes_to_the_month_it_was_created():
    result = build_partition(make_data(), "order_reviews", "2017-02")
    assert list(result["review_id"]) == ["r2"]


def test_product_sold_twice_appears_once_per_month():
    result = build_partition(make_data(), "products", "2017-01")
    assert list(result["product_id"]) == ["p1", "p2"]


def test_empty_month_keeps_columns():
    result = build_partition(make_data(), "order_items", "2016-11")
    assert len(result) == 0
    assert list(result.columns) == ["order_id", "order_item_id", "product_id", "seller_id"]


def test_unexpected_timestamp_format_is_rejected():
    with pytest.raises(ValueError):
        to_month(pd.Series(["15/01/2017 10:20"], name="order_purchase_timestamp"))