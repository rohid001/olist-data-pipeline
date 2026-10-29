# Image Airflow untuk project ini: Airflow resmi + venv terpisah untuk pipeline.
# dbt, pandas, dan library ingestion dipasang di /opt/venvs/pipeline supaya versinya
# tidak bentrok dengan dependency Airflow sendiri.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN mkdir -p /opt/venvs && chown airflow:0 /opt/venvs

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/venvs/pipeline \
    && /opt/venvs/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements.txt