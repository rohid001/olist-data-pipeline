"""
settingsurasi.py
Membaca pengaturan warehouse dari file .env dan membuat koneksi database.
Semua modul ingestion memakai file ini, jadi password hanya tersimpan di .env.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, Engine, create_engine, text

# Root repo = satu folder di atas folder ingestion/
ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT_DIR / ".env"

VARIABEL_WAJIB = [
    "WAREHOUSE_HOST",
    "WAREHOUSE_PORT",
    "WAREHOUSE_DB",
    "WAREHOUSE_USER",
    "WAREHOUSE_PASSWORD",
]

def get_settings() -> dict:
    """Baca .env, cek semua variabel wajib terisi, lalu kembalikan sebagai dict."""
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"File .env tidak ditemukan di {ENV_FILE}. "
            "Salin .env.example menjadi env lalu isi password-nya."
        )
    load_dotenv(ENV_FILE)
    
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
    
def get_engine() -> Engine:
    """Buat engine SQLAlchemy ke warehouse.
    
    Pakia URL.create (bukan f-string) supaya passowrd berisi karakter
    khusus seperti @ atau # tidak merusak alamat koneksi
    """
    
    settings = get_settings()
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=settings["user"],
        password=settings["password"],
        host=settings["host"],
        port=settings["port"],
        database=settings["database"],
    )
    return create_engine(url)
    
if __name__ == "__main__":
    # Uji Tahap A: env terbaca, password tidak ikut dishownkan
    settings = get_settings()
    for key, value in settings.items():
        shown = "********" if key == "password" else value
        print(f"{key:<9}: {shown}")
    print("Konfigurasi OK")
    # Uji Tahap B: engine bisa menjalankan query paling sederhana
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    print(f"SELECT 1 -> {result}")
    