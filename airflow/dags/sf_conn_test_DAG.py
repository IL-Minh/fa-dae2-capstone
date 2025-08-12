from datetime import datetime

from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow import DAG

SNOWFLAKE_CONN_ID = "snowflake_default"  # default ID used by hooks/operators :contentReference[oaicite:1]{index=1}

with DAG(
    dag_id="sf_conn_test_DAG",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "snowflake"],
) as dag:

    @task
    def test_snowflake_connection() -> str:
        """
        Connects to Snowflake using SnowflakeHook and executes a simple query.
        Returns the result as a string.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP;")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return f"Snowflake current time: {result!s}"

    @task
    def log_result(snowflake_time: str):
        """Logs the result returned from Snowflake."""
        print(snowflake_time)

    # Task execution flow
    snowflake_time = test_snowflake_connection()
    log_result(snowflake_time)
