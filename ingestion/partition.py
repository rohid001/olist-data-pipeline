"""
partition.py
Memecah CSV di data/source/ menjadi partisi bulanan di data/landing/,
mengikuti aturan partisi di data_contract.py:
    data/landing/<table>/month=YYY-MM/<table>.csv   (tabel berpartisi)
    data/landing/<table>/static/<table>.csv         (tabel statis)
Jalankan dari root repo:
    python -m ingestion.partition --month 2017-01
    python -m ingestion.partition --static
    python -m ingestion.partition --all
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

from ingestion.config import ROOT_DIR
from ingestion.data_contract import CONTRACT, FIRST_MONTH, LAST_MONTH

SOURCE_DIR = ROOT_DIR / "data" / "source"
LANDING_DIR = ROOT_DIR / "data" / "landing"
TIMESTAMP_PATTERN  = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"

MONTHLY_TABLES = [t for t, s in CONTRACT.items() if s["partition_type"] in ("time_column", "parent_table")]
STATIC_TABLES = [t for t, s in CONTRACT.items() if s["partition_type"] == "static"]


#----------------------------------------------------------------------Tahap A
def load_sources() -> dict:
    """Baca semua table yang dipartisi atau statis, semua kolom sebagai teks."""
    data = {}
    for table in MONTHLY_TABLES + STATIC_TABLES:
        path = SOURCE_DIR / CONTRACT[table]["file"]
        data[table] = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    return data

def to_month(column: pd.Series) -> pd.Series:
    """'2017-01-15 10:20:00' -> '2017-01' (ambil 7 karakter pertama)."""
    # Jauh lebih cepat dari pd.to_datetime, tapi hanya benar bila formatnya pasti.
    # Karena itu format dicek dulu; kalau berubah, script berhenti, bukan diam-diam salah.
    if not column.str.fullmatch(TIMESTAMP_PATTERN).all():
        raise ValueError(f"Unexpected timestamp format in column {column.name}")
    return column.str[:7]

def build_partition(data: dict, table: str, month: str) -> pd.DataFrame:
    """Kembalikan baris 'table' yang termasuk batch 'month', sesuai aturan di kontak."""
    spec = CONTRACT[table]
    df = data[table]
    if spec["partition_type"] == "time_column":
        return df[to_month(df[spec["time_column"]]) == month]
    if spec["partition_type"] == "parent_table":
        # Ambil dulu partisi table induk di bulan yang sama, lalu ikuti lewat join_column
        parent = build_partition(data, spec["parent_table"], month)
        join_column = spec["join_column"]
        return df[df[join_column].isin(parent[join_column])]
    raise ValueError(f"{table} is not partitioned by month ({spec['parititon_type']})")

#----------------------------------------------------------------------Tahap B
def partition_path(table: str, month: str) -> Path:
    """data/landing/orders/month=2017-01/orders.csv"""
    return LANDING_DIR / table / f"month={month}" / f"{table}.csv"

def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Tulis CSV, menimpa file lama bila ada (menjalankan ulang = hasil sama)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # lineterminator="\n": akhir baris LF di semua OS; tanpa ini Windows menulis CRLF
    df.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")
    
def write_month(data: dict, month: str) -> dict:
    """Tulis partisi semua table bulanan untuk satu batch; kembalikan jumlah baris per table."""
    counts = {}
    for table in MONTHLY_TABLES:
        part = build_partition(data, table, month)
        write_csv(part, partition_path(table, month))
        counts[table] = len(part)
    return counts

def write_static(data: dict) -> dict:
    """Tulis tabel statis (tidak dipartisi) ke data/landing/<table>/static/."""
    counts = {}
    for table in STATIC_TABLES:
        write_csv(data[table], LANDING_DIR / table / "static" / f"{table}.csv")
        counts[table] = len(data[table])
    return counts

#----------------------------------------------------------------------Tahap C
def batch_months() -> list:
    """Semua bulan batch dari FIRST_MONTH sampai LAST_MONTH."""
    return [str(p) for p in pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")]

def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah: tepat satu dari --month, --static, --all"""
    parser = argparse.ArgumentParser(description="Split Olist source CSVs into landing partitions.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="one batch month e.g. 2017-01")
    group.add_argument("--static", action="store_true", help="static tables only")
    group.add_argument("--all", action="store_true", help="all batch months plus static tables")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.month and args.month not in batch_months():
        print(f"Month {args.month} is outside the batch range {FIRST_MONTH} to {LAST_MONTH}")
        sys.exit(1)
        
    data = load_sources()
    
    if args.month:
        counts = write_month(data, args.month)
        for table, rows in counts.items():
            print(f"{table:<16} {rows:>8,}")
        print(f"Batch {args.month} written to data/landing")
        
    if args.all:
        totals = {table: 0 for table in MONTHLY_TABLES}
        for month in batch_months():
            for table, rows in write_month(data, month).items():
                totals[table] += rows
        for table, rows in totals.items():
            print(f"{table:<16} {rows:>8,}")
        print(f"{len(batch_months())} batches written to data/landing")
        
    if args.static or args.all:
        for table, rows in write_static(data).items():
            print(f"{table:<16} {rows:>8,}  (static)")