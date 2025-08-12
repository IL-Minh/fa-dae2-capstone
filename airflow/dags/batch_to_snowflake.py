from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import subprocess


def load_csv(**context):
    # Pick the most recent CSV from /opt/airflow/data/incoming
    data_dir = Path("/opt/airflow/data/incoming")
    latest = max(data_dir.glob("transactions_*.csv"), key=lambda p: p.stat().st_mtime)
    cmd = [
        "uv",
        "run",
        "/opt/airflow/repo/loaders/csv_to_snowflake.py",
        str(latest),
    ]
    env = os.environ.copy()
    subprocess.check_call(cmd, env=env)


with DAG(
    dag_id="batch_csv_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ingest = PythonOperator(task_id="load_latest_csv", python_callable=load_csv)


