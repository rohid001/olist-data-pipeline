"""
export_marts.py
Mengekspor tabel mart yang dipakai dashboard dari schema marts ke file Parquet di
app/data.
Dengan snapshot ini, dashbaord Streamlit bisa di-hosting gratis tanpa koneksi warehouse.
Jalankan dari root repo setelha dbt build.
    python -m scripts.export_marts.py
"""
import pandas as pd

from ingestion.config import ROOT_DIR, get_engine

EXPORT_DIR = ROOT_DIR / "app" / "data"

# sama dengan depends_on di dbt/olits/model/exposures.yml
MARTS = [
    "mart_overview_kpi",
    "mart_customer_rfm",
    "mart_buyer_type",
    "mart_revenue_monthly",
    "mart_seasonality",
    "mart_category_growth",
    "mart_customer_churn",
    "mart_review_churn",
    "mart_category_per_state",
    "mart_revenue_per_state",
    "mart_category_win_count",
]

#----------------------------------------------------------------Tahap A
def read_mart(engine, mart: str) -> pd.DataFrame:
    """Ambil seluruh isi satu mart, diurutkan supaya file hasil export selalu sama."""
    df = pd.read_sql(f"select * from marts.{mart}", engine)
    # Tanpa ORDER BY, urutan baris dari PostgreSQL bisa berubah antar-run. Diurutkan di sini
    # supaya export ulang data yang sama tidak membuat file Parquet berubah di git.
    return df.sort_values(list(df.columns), ignore_index=True)

#----------------------------------------------------------------Tahap B
def export_mart(engine, mart: str) -> int:
    """Tulis satu mart ke app/data/<mart>.parquet; kembalikan jumlah barisnya."""
    df = read_mart(engine, mart)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(EXPORT_DIR / f"{mart}.parquet", index=False)
    return len(df)

if __name__ == "__main__":
    engine = get_engine()
    for mart in MARTS:
        rows = export_mart(engine, mart)
        print(f"{mart:<26} {rows:>7,} rows")
    size_kb = sum(f.stat().st_size for f in EXPORT_DIR.glob("*.parquet")) / 1024
    folder = EXPORT_DIR.relative_to(ROOT_DIR).as_posix()
    print(f"{len(MARTS)} marts exported to {folder} ({size_kb:,.0f} KB)")