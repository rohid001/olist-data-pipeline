"""
olist_static_reference.py
DAG pendamping: memuat tabel statis (geolocation) satu kali.
Tabel ini tidak punya kolom waktu, jadi tidak ikut batch bulanan.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_DIR= "/opt/olist"
PIPELINE_BIN= "/opt/venvs/pipeline/bin"

with DAG(
    dag_id="olist_static_reference",
    description="Load static reference tables (geolocation) once",
    schedule="@once",
    start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
    catchup=False,
    tags=["olist"],
) as dag:
    BashOperator(
        task_id="load_static",
        bash_command=(
            f"{PIPELINE_BIN}/python -m ingestion.partition --static"
            f" && {PIPELINE_BIN}/python -m ingestion.load_raw --static"
        ),
        cwd=PROJECT_DIR,
    )