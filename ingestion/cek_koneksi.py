"""
cek_koneksi.py
Menguji koneksi ke warehouse dan memastikan 4 schema layer sudah ada.
Jalankan dari root repo: python -m ingestion.cek_koneksi
"""
import sys

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from ingestion.konfigurasi import buat_engine

SCHEMA_WAJIB = ["raw", "staging", "intermediate", "marts"]


def ambil_versi(engine) -> str:
    """Kembalikan nama dan versi PostgreSQL, misalnya 'PostgreSQL 18.1'."""
    with engine.connect() as koneksi:
        versi_lengkap = koneksi.execute(text("SELECT version()")).scalar()
    return " ".join(versi_lengkap.split()[:2])

def ambil_daftar_schema(engine) -> list[str]:
    """Kembalikan nama chema buatan pengguna (tanpa schema sistem pg_*)."""
    query = text("""
                 SELECT schema_name
                 FROM information_schema.schemata
                 WHERE schema_name NOT LIKE 'pg\\_%'
                    AND schema_name <> 'information_schema'
                ORDER BY schema_name
                 """)
    with engine.connect() as koneksi:
        return [baris[0] for baris in koneksi.execute(query)]
    
def cari_schema_hilang(daftar_schema: list[str]) -> list[str]:
    """Kembalikan schema wajib yang belum ada di warehouse."""
    return [nama for nama in SCHEMA_WAJIB if nama not in daftar_schema]

if __name__ == "__main__":
    # Uji Tahap A: bisa terhubung dan membaca versi server
    engine = buat_engine()
    try:
        print("Versi  :", ambil_versi(engine))
    except OperationalError:
        print("GAGAL terhubung ke warehouse.")
        print("Cek: container berjalan? port di .env = 5433? password sama?")
        sys.exit(1)
    
    # Uji Tahap B: 4 schema layer dari warehouse/init sudah dibuat
    daftar_schema = ambil_daftar_schema(engine)
    print("Schema :", ", ".join(daftar_schema))

    schema_hilang = cari_schema_hilang(daftar_schema)
    if schema_hilang:
        print("Schema belum ada:", ", ".join(schema_hilang))
        sys.exit(1)
    print("Koneksi OK, 4 schema layer siap")