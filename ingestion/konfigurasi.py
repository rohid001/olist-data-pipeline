"""
konfigurasi.py
Membaca pengaturan warehouse dari file .env dan membuat koneksi database.
Semua modul ingestion memakai file ini, jadi password hanya tersimpan di .env.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, Engine, create_engine, text

# Root repo = satu folder di atas folder ingestion/
FOLDER_ROOT = Path(__file__).resolve().parent.parent
FILE_ENV = FOLDER_ROOT / ".env"

VARIABEL_WAJIB = [
    "WAREHOUSE_HOST",
    "WAREHOUSE_PORT",
    "WAREHOUSE_DB",
    "WAREHOUSE_USER",
    "WAREHOUSE_PASSWORD",
]

def ambil_konfigurasi() -> dict:
    """Baca .env, cek semua variabel wajib terisi, lalu kembalikan sebagai dict."""
    if not FILE_ENV.exists():
        raise FileNotFoundError(
            f"File .env tidak ditemukan di {FILE_ENV}. "
            "Salin .env.example menjadi env lalu isi password-nya."
        )
    load_dotenv(FILE_ENV)
    
    belum_diisi = [nama for nama in VARIABEL_WAJIB if not os.getenv(nama)]
    if belum_diisi:
        raise ValueError(f"Variabel ini belum diisi di .env {', '.join(belum_diisi)}")
    
    return {
        "host": os.getenv("WAREHOUSE_HOST"),
        "port": int(os.getenv("WAREHOUSE_PORT")),
        "database": os.getenv("WAREHOUSE_DB"),
        "user": os.getenv("WAREHOUSE_USER"),
        "password": os.getenv("WAREHOUSE_PASSWORD"),
    }
    
def buat_engine() -> Engine:
    """Buat engine SQLAlchemy ke warehouse.
    
    Pakia URL.create (bukan f-string) supaya passowrd berisi karakter
    khusus seperti @ atau # tidak merusak alamat koneksi
    """
    
    konfig = ambil_konfigurasi()
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=konfig["user"],
        password=konfig["password"],
        host=konfig["host"],
        port=konfig["port"],
        database=konfig["database"],
    )
    return create_engine(url)
    
if __name__ == "__main__":
    # Uji Tahap A: env terbaca, password tidak ikut ditampilkan
    konfig = ambil_konfigurasi()
    for kunci, nilai in konfig.items():
        tampil = "********" if kunci == "password" else nilai
        print(f"{kunci:<9}: {tampil}")
    print("Konfigurasi OK")
    # Uji Tahap B: engine bisa menjalankan query paling sederhana
    engine = buat_engine()
    with engine.connect() as koneksi:
        hasil = koneksi.execute(text("SELECT 1")).scalar()
    print(f"SELECT 1 -> {hasil}")
    