"""
cek_conn.py
Menguji conn ke warehouse dan memastikan 4 schema layer sudah ada.
Jalankan dari root repo: python -m ingestion.cek_conn
"""
import sys

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from ingestion.config import get_engine

REQUIRED_SCHEMAS = ["raw", "staging", "intermediate", "marts"]


def get_server_version(engine) -> str:
    """Kembalikan name dan versi PostgreSQL, misalnya 'PostgreSQL 18.1'."""
    with engine.connect() as conn:
        full_version = conn.execute(text("SELECT version()")).scalar()
    return " ".join(full_version.split()[:2])

def list_schemas(engine) -> list[str]:
    """Kembalikan name schema buatan pengguna (tanpa schema sistem pg_*)."""
    query = text("""
                 SELECT schema_name
                 FROM information_schema.schemata
                 WHERE schema_name NOT LIKE 'pg\\_%'
                    AND schema_name <> 'information_schema'
                ORDER BY schema_name
                 """)
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(query)]
    
def find_missing_schemas(schemas: list[str]) -> list[str]:
    """Kembalikan schema wajib yang belum ada di warehouse."""
    return [name for name in REQUIRED_SCHEMAS if name not in schemas]

if __name__ == "__main__":
    # Uji Tahap A: bisa terhubung dan membaca versi server
    engine = get_engine()
    try:
        print("Versi  :", get_server_version(engine))
    except OperationalError:
        print("GAGAL terhubung ke warehouse.")
        print("Cek: container berjalan? port di .env = 5433? password sama?")
        sys.exit(1)
    
    # Uji Tahap B: 4 schema layer dari warehouse/init sudah dibuat
    schemas = list_schemas(engine)
    print("Schema :", ", ".join(schemas))

    missing = find_missing_schemas(schemas)
    if missing:
        print("Schema belum ada:", ", ".join(missing))
        sys.exit(1)
    print("conn OK, 4 schema layer siap")