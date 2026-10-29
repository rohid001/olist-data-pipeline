"""
olist_monthly_pipeline.py
DAG utama: setiap run memproses SATU bulan data olist, dari CSV sampai mart.
Bulan yang diproses = data_interval_start run itu, misalnya run September 2016
memproses batch 2016-09. Dengan catchup=True, Airflow menjalankan ulang (bacfill)
ke-26 bulan dari september 2016 sampai oktober 2018 secara berurutan.

Semua task memanggil kode yang sama dengan yang dijalankan manual dari Fase 2-6,
lewat env pipeline di dalam image (lihat Dockerfile).
"""

from datetime import timedelta
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

# Lokasi di dalam container (lihat volumes di docker-compose.yml)
PROJECT_DIR = "/opt/olist"
DBT_DIR = f"{PROJECT_DIR}/dbt/olist"
PIPELINE_BIN = "/opt/venvs/pipeline/bin"

# Template Jinja: diisi Airflow saat task berjalan, misalnya "2016-09"
BATCH_MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
default_args = {
    "retries": 2,                               # coba lagi 2 kali bila gagal
    "retry_delay": timedelta(minutes=1),        # tunggu 1 menit sebelum coba lagi
    "execution_timeout": timedelta(minutes=15), # hentikan task yang macet
}

with DAG(
    dag_id="olist_monthly_pipeline",
    description="Monthly Olist batch: partition, load raw, check freshness, dbt build",
    # Interval bulanan: run "September 2016" mencakup 1 Sep - 1 Okt 2016
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
    end_date=pendulum.datetime(2018, 10, 1, tz="UTC"),
    # Airflow 3: catchup default False, jadi harus ditulis eksplisit untuk backfill
    catchup=True,
    # fct_orders incremental: bulan harus diproses satu per satu, berurutan
    max_active_runs=1,
    default_args=default_args,
    tags=["olist"],
) as dag:
    partition_month = BashOperator(
        task_id="partition_month",
        bash_command=f"{PIPELINE_BIN}/python -m ingestion.partition --month {BATCH_MONTH}",
        cwd=PROJECT_DIR,
    )
    
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"{PIPELINE_BIN}/python -m ingestion.load_raw --month {BATCH_MONTH}",
        cwd=PROJECT_DIR,
    )
    
    # Cukup cek raw.orders: semua tabel child dimuat dalam transaksi yang sama.
    # Tabel lain bisa kosong secara sah, misalnya belum ada review di batch 2016-09
    check_freshness = BashOperator(
        task_id="check_freshness",
        bash_command=f"{PIPELINE_BIN}/dbt source freshness --select source:raw.orders",
        cwd=DBT_DIR,
    )
    
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PIPELINE_BIN}/dbt build",
        cwd=DBT_DIR,
    )
    
    partition_month >> load_raw >> check_freshness >> dbt_build