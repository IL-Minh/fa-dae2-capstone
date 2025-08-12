from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import subprocess


def load_csv(**context):
    data_dir = Path("/opt/airflow/data/incoming")
    candidates = sorted(data_dir.glob("transactions_*.csv"))
    if not candidates:
        raise FileNotFoundError("No CSVs found in /opt/airflow/data/incoming")
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    cmd = ["uv", "run", "/opt/airflow/repo/loaders/csv_to_snowflake.py", str(latest)]
    env = os.environ.copy()
    # Ensure Snowflake envs exist
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise RuntimeError(f"Missing Snowflake env vars: {missing}")
    subprocess.check_call(cmd, env=env)


with DAG(
    dag_id="batch_csv_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ingest = PythonOperator(task_id="load_latest_csv", python_callable=load_csv)


