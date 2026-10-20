"""
profile_data.py
Memeriksa 9 CSV Olist terhadap data_contract.py sebelum pipeline dibangun.
    Tahap A: file ada, jumlah baris, kolom sama persis dengan kontrak
    Tahap B: key (grain) tiap tabel benar-benar unik
    Tahap C: sebaran per bulan, review terlambat, dimensi lintas bulan,
            lalu ringkasannya disimpan ke docs/data_profile.md
Jalankan dari root repo: python -m ingestion.profile_data
"""

import sys
import pandas as pd

from ingestion.config import ROOT_DIR
from ingestion.data_contract import CONTRACT, FIRST_MONTH, LAST_MONTH

SOURCE_DIR = ROOT_DIR/ "data" / "source"
REPORT_FILE = ROOT_DIR/ "docs" / "data_profile.md"

def read_table(table: str) -> pd.DataFrame:
    """Baca satu CSV dengan semua kolom sebagai teks (tipe data baru ditentukan di staging)."""
    path = SOURCE_DIR / CONTRACT[table]["file"]
    # utf-8-sig membuang karakter BOM di awal file (ada di file terjemahan kategori)
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")


#----------------------------------------------------------------- Tahap A
def check_files_and_columns() -> dict:
    """Baca semua tabel, pastikan file ada dan kolomnya sama persis dengan kontrak."""
    data = {}
    has_problem = False
    print(f"{'Table':<35} {'Rows':>10}  Columns")
    for table, spec in CONTRACT.items():
        if not (SOURCE_DIR / spec["file"]).exists():
            print(f"{table:<35} {'-':>10}   MISSING FILE: {spec['file']}")
            has_problem = True
            continue
        df = read_table(table)
        missing = [c for c in spec["columns"] if c not in df.columns]
        extra = [c for c in df.columns if c not in spec["columns"]]
        status = "OK" if not missing and not extra else f"missing {missing}, extra {extra}"
        has_problem = has_problem or status != "OK"
        print(f"{table:<35} {len(df):>10,}  {status}")
        data[table] = df
    if has_problem:
        print("Some files or columns do not match the contract. Fix them first.")
        sys.exit(1)
    return data

#----------------------------------------------------------------- Tahap B
def count_duplicates(df: pd.DataFrame, key: list) -> int:
    """Jumlah baris yang kuncinya sudah muncul di baris sebelumnya."""
    return int(df.duplicated(subset=key).sum())

def check_keys(data: dict) -> None:
    """Setiap kunci di kontrak harus unik; itu bukti grain tabelnya benar."""
    has_problem = False
    print(f"\n{'Table':<35} {'Key':<35} Duplicates")
    for table, spec in CONTRACT.items():
        if spec["key"] is None:
            full_dupes = int(data[table].duplicated().sum())
            print(f"{table:<35} {'(no key)':<35} {full_dupes:,} fully duplicated rows")
            continue
        dupes = count_duplicates(data[table], spec["key"])
        has_problem = has_problem or dupes > 0
        print(f"{table:<35} {' + '.join(spec['key']):<35} {dupes:,}")
    review_id_dupes = count_duplicates(data["order_reviews"], ["review_id"])
    print(f"Note: review_id alone has {review_id_dupes:,} duplicates, "
          "so the order_reviews key is review_id + order_id")
    if has_problem:
        print("Some keys are not unique. Fix the contract.")
        sys.exit(1)
        
#----------------------------------------------------------------- Tahap C
def to_month(column: pd.Series) -> pd.Series:
    """'20170-01-15 10:20:00' -> '2017-01'."""
    return pd.to_datetime(column).dt.strftime("%Y-%m")

def batch_months() -> list:
    """Semua bulan batch dari FIRST_MONTH sampai LAST_MONTH, termasuk yang kosong."""
    return [str(p) for p in pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")]

def count_per_month(data: dict) -> pd.DataFrame:
    """Jumlah order dan review per bulan batch; bulan tanpa data tetap muncul dengan 0."""
    orders = to_month(data["orders"]["order_purchase_timestamp"]).value_counts()
    reviews = to_month(data["order_reviews"]["review_creation_date"]).value_counts()
    result = pd.DataFrame(index=batch_months())
    result["orders"] = orders.reindex(result.index, fill_value=0)
    result["reviews"] = reviews.reindex(result.index, fill_value=0)
    return result

def compare_review_months(data: dict) -> dict:
    """Bandingkan bulan dibuatnya review dengan bulan order-nya."""
    orders = data["orders"][["order_id", "order_purchase_timestamp"]]
    reviews = data["order_reviews"][["order_id", "review_creation_date"]]
    merged = reviews.merge(orders, on="order_id", how="left")
    review_month = to_month(merged["review_creation_date"])
    order_month = to_month(merged["order_purchase_timestamp"])
    return {
        "total": len(merged),
        "same_month": int((review_month == order_month).sum()),
        "late": int((review_month > order_month).sum()),
        "early": int((review_month < order_month).sum()),
    }
    
def count_multi_month_dims(data: dict) -> dict:
    """Hitung dimensi yang muncul di lebih dari satu bulan (alasan memilih upsert)."""
    orders = data["orders"].assign(month=to_month(data["orders"]["order_purchase_timestamp"]))
    items = data["order_items"].merge(orders[["order_id", "month"]], on="order_id")
    result = {}
    for column in ["product_id", "seller_id"]:
        months_per_id = items.groupby(column)["month"].nunique()
        result[column] = {"total": len(months_per_id), "multi_month": int((months_per_id > 1).sum())}
    result["reused_customer_ids"] = int(data["orders"]["customer_id"].duplicated().sum())
    return result


def write_report(per_month: pd.DataFrame, reviews: dict, dims: dict) -> None:
    """Simpan ringkasan Tahap C ke docs/data_profile.md sebagai bukti keputusan desain."""
    late_pct = reviews["late"] / reviews["total"] * 100
    lines = [
        "# Olist data profile",
        "",
        "Generated by `python -m ingestion.profile_data`. Do not edit by hand.",
        "",
        "## Orders and reviews per batch month",
        "",
        "| Month | Orders | Reviews |",
        "| --- | ---: | ---: |",
    ]
    for month, row in per_month.iterrows():
        lines.append(f"| {month} | {row['orders']:,} | {row['reviews']:,} |")
    lines += [
        f"| **Total** | **{per_month['orders'].sum():,}** | **{per_month['reviews'].sum():,}** |",
        "",
        "## Review month vs. order month",
        "",
        f"- Same month: {reviews['same_month']:,}",
        f"- Late (a later month): {reviews['late']:,} ({late_pct:.1f}%)",
        f"- Earlier than the order (anomaly): {reviews['early']:,}",
        "",
        "## Dimensions across months",
        "",
        f"- Products sold in more than 1 month: {dims['product_id']['multi_month']:,} "
        f"of {dims['product_id']['total']:,}",
        f"- Sellers selling in more than 1 month: {dims['seller_id']['multi_month']:,} "
        f"of {dims['seller_id']['total']:,}",
        f"- customer_id values reused by more than 1 order: {dims['reused_customer_ids']:,}",
    ]
    REPORT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    

if __name__ == "__main__":
    # Uji Tahap A: 9 file ada dan kolomnya sesuai kontrak
    data = check_files_and_columns()
    # Uji Tahap B: kunci tiap tabel unik
    check_keys(data)
    # Uji Tahap C: sebaran per bulan dan bukti untuk load strategy
    per_month = count_per_month(data)
    empty_months = per_month.index[per_month["orders"] == 0].tolist()
    print(f"\n{len(per_month)} batch months: {per_month['orders'].sum():,} orders, "
          f"{per_month['reviews'].sum():,} reviews")
    print("Months without orders:", ", ".join(empty_months))

    reviews = compare_review_months(data)
    late_pct = reviews["late"] / reviews["total"] * 100
    print(f"Late reviews: {reviews['late']:,} of {reviews['total']:,} ({late_pct:.1f}%), "
          f"earlier than their order: {reviews['early']:,}")

    dims = count_multi_month_dims(data)
    print(f"Multi-month products: {dims['product_id']['multi_month']:,} of "
          f"{dims['product_id']['total']:,}; multi-month sellers: "
          f"{dims['seller_id']['multi_month']:,} of {dims['seller_id']['total']:,}")
    print(f"customer_id values reused by more than 1 order: {dims['reused_customer_ids']:,}")

    write_report(per_month, reviews, dims)
    print(f"Report saved: {REPORT_FILE.relative_to(ROOT_DIR).as_posix()}")
    
        