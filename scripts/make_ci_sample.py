"""
make_ci_sample.py
Membuat sampel kecil data Olist untuk CI di tests/fixtures/olist_sample/.
CSV asli (sekitar 120 MB) tidak di-commit, jadi GitHub Actions memakai sample ini.
Dari setiap bulan dipilih acak paling banyak ORDERS_PER_MONTH order, lalu semua baris
turunannya ikut diambil, supaya relasi antartabel tetap utuh dan test dbt tetap bermakna.
Jalankan dari root repo setelah data/source terisi (Fase 2):
    python -m scripts.make_ci_sample
"""
import pandas as pd

from ingestion.config import ROOT_DIR
from ingestion.data_contract import CONTRACT
from ingestion.partition import MONTHLY_TABLES, SOURCE_DIR, STATIC_TABLES, to_month

SAMPLE_DIR = ROOT_DIR / "tests" / "fixtures" / "olist_sample"
ORDERS_PER_MONTH = 20
GEO_ROWS_PER_ZIP = 3
RANDOM_SEED = 42 # seed tetap: menjalankan ulang script = sample yang sama persis

#---------------------------------------------------------------------------------Tahap A
def read_source(table: str) -> pd.DataFrame:
    """Baca satu CSV sumber, semua kolom sebagai teks (sama seperti partition.py)."""
    return pd.read_csv(SOURCE_DIR / CONTRACT[table]["file"], dtype=str, encoding="utf-8-sig")

def sample_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Ambil acak paling banyak ORDERS_PER_MONTH order dari setiap bulan."""
    month = to_month(orders["order_purchase_timestamp"])
    # Bulan yang ordernya lebih sedikit (misalnya 2016-12 hanya 1) diambil semuanya
    picked = orders.groupby(month, group_keys=False).apply(
        lambda group: group.sample(n=min(len(group), ORDERS_PER_MONTH), random_state=RANDOM_SEED)
    )
    return picked.sort_values("order_purchase_timestamp")

def build_sample() -> dict:
    """Kembalikan sampel semua tabel bulanan dan statis, mengikuti order yang terpilih"""
    orders = sample_orders(read_source("orders"))
    order_ids = orders["order_id"]
    
    items = read_source("order_items")
    items = items[items["order_id"].isin(order_ids)]
    customers = read_source("customers")
    customers = customers[customers["customer_id"].isin(orders["customer_id"])]
    sellers = read_source("sellers")
    sellers = sellers[sellers["seller_id"].isin(items["seller_id"])]
    
    # Geolocation tidak dipakai mart mana pun, jadi cukup bebearap baris per kode pos
    zips = pd.concat([customers["customer_zip_code_prefix"],
                      sellers["seller_zip_code_prefix"]])
    geolocation = read_source("geolocation")
    geolocation = (geolocation[geolocation["geolocation_zip_code_prefix"].isin(zips)].groupby("geolocation_zip_code_prefix").head(GEO_ROWS_PER_ZIP))
    
    sample = {
        "orders": orders,
        "order_items": items,
        "order_payments": read_source("order_payments").pipe(lambda df:df[df["order_id"].isin(order_ids)]),
        "order_reviews": read_source("order_reviews").pipe(lambda df: df[df["order_id"].isin(order_ids)]),
        "customers": customers,
        "products": read_source("products").pipe(lambda df: df[df["product_id"].isin(items["product_id"])]),
        "sellers": sellers,
        "geolocation": geolocation
    }
    assert set(sample) == set(MONTHLY_TABLES + STATIC_TABLES)
    return sample

#---------------------------------------------------------------------------------Tahap B
def write_sample(sample: dict) -> None:
    """Tulis sampel dengan nama file yang sama seperti CSV sumber."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for table, df in sample.items():
        # Komentar review asli memakai baris CRLF. Diubah ke LF karena .gitattributes
        # memakasa LF; tanpa ini git mengubah isi file saat commit dan memberi peringatan.
        df = df.apply(lambda column: column.str.replace("\r\n", "\n", regex=False))
        df.to_csv(SAMPLE_DIR / CONTRACT[table]["file"], index=False, encoding="utf-8",
                  lineterminator="\n")

if __name__ == "__main__":
    sample = build_sample()
    write_sample(sample)
    for table, df in sample.items():
        print(f"{table:<16} {len(df):>6,}")
    size_kb = sum(f.stat().st_size for f in SAMPLE_DIR.glob("*.csv")) / 1024
    print(f"Sample written to {SAMPLE_DIR.relative_to(ROOT_DIR).as_posix()} ({size_kb:,.0f} KB)")
    
    