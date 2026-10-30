"""
create_bi_reader.py
Membuat user database bi_reader untuk Power BI: hanya boleh Membaca tabel di schema marts.
Tidak bisa membaca raw/staging/intermediate dan tidak bisa mengubah apa pun.
Password-nya dibaca dari BI_READER_PASSWORD di .env, jadi tidak pernah tertulis di kode.
Aman dijalankan berulang kali (idempotent). Jalankan dari root repo:
    python -m scripts.create_bi_reader
"""
import os
import sys

from psycopg2 import sql

from ingestion.config import get_engine, get_settings

ROLE = "bi_reader"
SCHEMA = "marts"

#-----------------------------------------------------------------Tahap A
def upsert_role(cur, password: str) -> str:
    """Buat role bi_reader bila belum ada; bila sudah ada, perbarui password-nya."""
    cur.execute("SELECT 1 from pg_roles WHERE rolname = %s", (ROLE,))
    action = "updated" if cur.fetchone() else "created"
    verb = "ALTER" if action == "updated" else "CREATE"
    # nama role lewat sql.Identifier, password lewat parameter %s: tidak ada f-string di SQL
    cur.execute(
        sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD %s").format(sql.Identifier(ROLE)),
        (password,),
    )
    return action

#-----------------------------------------------------------------Tahap B
def grant_read_on_marts(cur, database: str, owner: str) -> None:
    """Beri hak baca schema marts saja, termasuk untuk tabel yang nanti dibuat ulang dbt."""
    statements = [
        "GRANT CONNECT ON DATABASE {database} TO {role}",
        "GRANT USAGE ON SCHEMA {schema} TO {role}",
        "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role}",
        # dbt membuat ulang tabel mart setiap build. Default privileges memberi SELECT
        # otomatis pada tabel baru yang dibuat owner di schema ini.
        "ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema}"
        "GRANT SELECT ON TABLES TO {role}",
    ]
    names = {
        "database": sql.Identifier(database),
        "role": sql.Identifier(ROLE),
        "schema": sql.Identifier(SCHEMA),
        "owner": sql.Identifier(owner),
    }
    for statement in statements:
        cur.execute(sql.SQL(statement).format(**names))

if __name__ == "__main__":
    settings = get_settings() # sekaligus memuat .env ke environment
    password = os.getenv("BI_READER_PASSWORD")
    if not password:
        print("BI_READER_PASSWORD is empty. Add it to .env first.")
        sys.exit(1)
    
    conn = get_engine().raw_connection()
    try:
        cur = conn.cursor()
        action = upsert_role(cur, password)
        grant_read_on_marts(cur, settings["database"], settings["user"])
        conn.commit()
    finally:
        conn.close()
    print(f"Role {ROLE} {action}; read access granted on schema {SCHEMA}")
    