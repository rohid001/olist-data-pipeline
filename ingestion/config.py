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

REQUIRED_VARS = [
    "WAREHOUSE_HOST",
    "WAREHOUSE_PORT",
    "WAREHOUSE_DB",
    "WAREHOUSE_USER",
    "WAREHOUSE_PASSWORD",
]

def get_settings() -> dict:
    """Ambil pengaturan warehouse dari environtment variable, lalu kembalikan sebagai dict.
    
    Di PC/Laptop nilainya berasal dari file .env. Di CI (Github Actions) tidak ada file .env;
    workflow mengisi variabel yang sama langsung sebagai environtment variable.
    """
    # Bila .env tidak ada, load_dotenv diam saja. override=False (bawaan): variabel yang
    # sudah ada di environment (dari CI atau docker-compose) tidak ditimpa isi .env.
    load_dotenv(ENV_FILE)
    
    missing = [name for name in REQUIRED_VARS if not os.getenv(name)]
    
    if missing:
        raise ValueError(
            f"Missing settings: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in the values."
        )
    
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
    