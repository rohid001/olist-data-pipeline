"""
load_raw.py
Memuat partisi dari data/landing/ ke schema raw di warehouse.
Satu batch (satu bulan) dimuat dalam SATU transaksi: semua table berhasil,
atau tidak ada yang berubah. Cara memuat tiap tabel mengikuti load_strategy
di data_contract.py: delete_insert, upsert, atau full_replace.
Jalankan dari root repo:
    python -m ingestion.load_raw --month 2017-01
    python -m ingestion.laod_raw --static
    python -m ingestion.load_raw --full
"""

import argparse
import sys

from ingestion.config import ROOT_DIR, get_engine
from ingestion.data_contract import CONTRACT, FIRST_MONTH, LAST_MONTH
from ingestion.partition import LANDING_DIR, MONTHLY_TABLES, STATIC_TABLES, batch_months, partition_path

METADATA_COLUMNS = [
    "_batch_id TEXT NOT NULL",
    "_source_file TEXT NOT NULL",
    "_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()"
]

#----------------------------------------------------------------Tahap A
def create_raw_tables(cur) -> None:
    """Buat tabel raw dari kontrak: semua kolom bisnis TEXT + 3 kolom metadata."""
    for table in MONTHLY_TABLES + STATIC_TABLES:
        spec = CONTRACT[table]
        columns = [f"{c} TEXT" for c in spec["columns"]] + METADATA_COLUMNS
        if spec["load_strategy"] == "upsert":
            # ON CONFILIC di upsert butuh PRIMARY KEY pada kolom kunci
            columns.append(f"PRIMARY KEY ({', '.join(spec['key'])})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS raw.{table} ({', '.join(columns)})")
        if spec["load_strategy"] == "delete_insert":
            # Index supaya DELETE ... WHERE _batch_id = ... tidak membaca seluruh tabel
            cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_batch_id ON raw.{table}(_batch_id)")
            
def count_rows(cur) -> dict:
    """Jumlah baris tiap tabel raw, untuk membuktikan hasil load."""
    counts = {}
    for table in MONTHLY_TABLES + STATIC_TABLES:
        cur.execute(f"SELECT count(*) FROM raw.{table}")
        counts[table] = cur.fetchone()[0]
    return counts

#----------------------------------------------------------------Tahap B
def load_table(cur, table: str, batch_id: str, path) -> int:
    """Muat satu file CSV ke raw.<table> sesuai load_strategy; kembalikan jumlah baris yang ditulis."""
    spec = CONTRACT[table]
    columns = ", ".join(spec["columns"])
    temp_table = f"tmp_{table}"
    
    # 1. Salin file ke tabel sementara (hilang otomatis saat transaksi selesai)
    cur.execute(f"CREATE TEMP TABLE {temp_table} ({', '.join(f'{c} TEXT' for c in spec['columns'])}) "
                "ON COMMIT DROP")
    with open(path, encoding="utf-8") as file:
        # HEADER MATCH: PostgreSQL menolak file bila nama kolom di header tidak sama dengan tabel
        cur.copy_expert(f"COPY {temp_table} FROM STDIN WITH (FORMAT csv, HEADER MATCH)", file)
    
    # 2. Pindahkan ke tabel raw sesuai strategi
    strategy = spec["load_strategy"]
    if strategy == "delete_insert":
        cur.execute(f"DELETE FROM raw.{table} WHERE _batch_id = %s", (batch_id,))
    elif strategy == "full_replace":
        cur.execute(f"TRUNCATE raw.{table}")
    
    insert = (f"INSERT INTO raw.{table} ({columns}, _batch_id, _source_file) "
              f"SELECT {columns}, %s, %s FROM {temp_table}")
    if strategy == "upsert":
        updates = [f"{c} = EXCLUDED.{c}" for c in spec["columns"] if c not in spec["key"]]
        updates += ["_batch_id = EXCLUDED._batch_id", "_source_file = EXCLUDED._source_file",
                    "_loaded_at = EXCLUDED._loaded_at"]
        insert += f" ON CONFLICT ({', '.join(spec['key'])}) DO UPDATE SET {', '.join(updates)}"
    
    source_file = path.relative_to(ROOT_DIR).as_posix()
    cur.execute(insert, (batch_id, source_file))
    return cur.rowcount

def load_batch(engine, month: str) -> dict:
    """Muat 7 tabel bulanan untuk satu batch dalam satu transaksi."""
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        create_raw_tables(cur)
        written = {table: load_table(cur, table, month, partition_path(table, month)) for table in MONTHLY_TABLES}
        conn.commit()
        return written
    except Exception:
        # Satu tabel gagal = seluruh batch dibatalkan, termasuk tabel yang sudah dimuat
        conn.rollback()
        raise
    finally:
        conn.close()
    
#----------------------------------------------------------------Tahap C
def load_static(engine) -> dict:
    """Muat tabel statis (geolocation) dengan full_replace dalam satu transaksi."""
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        create_raw_tables(cur)
        written = {table: load_table(cur, table, "static", LANDING_DIR / table / "static" / f"{table}.csv") for table in STATIC_TABLES}
        conn.commit()
        return written
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        
def print_counts(engine) -> None:
    """Cetak jumlah baris akhir semua tabel raw."""
    conn = engine.raw_connection()
    try:
        for table, rows in count_rows(conn.cursor()).items():
            print(f"raw.{table:<16} {rows:>10,}")
    finally:
        conn.close()
        
def parse_args() -> argparse.Namespace:
    """Baca argumen baris perintah: tepat satu dari --month, --static, --all."""
    parser = argparse.ArgumentParser(description="Load landing partitions into the raw schema.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="one batch month, e.g. 2017-01")
    group.add_argument("--static", action="store_true", help="static tables only")
    group.add_argument("--all", action="store_true", help="all batch months plus static tables")
    return parser.parse_args()
    
if __name__ == "__main__":
    args = parse_args()
    if args.month and args.month not in batch_months():
        print(f"Month {args.month} is outside the batch range {FIRST_MONTH} to {LAST_MONTH}")
        sys.exit(1)
    
    engine = get_engine()
    months = [args.month] if args.month else (batch_months() if args.all else [])
    for month in months:
        written = load_batch(engine, month)
        print(f"Batch {month} loaded: {sum(written.values()):,} rows written")
    if args.static or args.all:
        for table, rows in load_static(engine).items():
            print(f"Static {table} loaded: {rows:,} rows written")
    
    print("Row counts in raw:")
    print_counts(engine)