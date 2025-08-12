from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator

# Import shared Snowflake connection module
from snowflake_conn import test_snowflake_connection

from airflow import DAG


def load_csv(**context):
    data_dir = Path("/opt/airflow/data/incoming")
    candidates = sorted(data_dir.glob("transactions_*.csv"))
    if not candidates:
        raise FileNotFoundError("No CSVs found in /opt/airflow/data/incoming")

    latest = max(candidates, key=lambda p: p.stat().st_mtime)

    # Test Snowflake connection before proceeding
    if not test_snowflake_connection():
        raise RuntimeError("Snowflake connection test failed")

    # Run the CSV loader
    cmd = ["uv", "run", "/opt/airflow/repo/loaders/csv_to_snowflake.py", str(latest)]
    subprocess.check_call(cmd)


with DAG(
    dag_id="batch_csv_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ingest = PythonOperator(task_id="load_latest_csv", python_callable=load_csv)
