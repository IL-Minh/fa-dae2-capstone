"""
dbt Transformations DAG
"""

import os

import pendulum
from airflow.decorators import dag, task


def get_dbt_snowflake_env_vars():
    return {
        "SNOWFLAKE_ACCOUNT": "{{ conn.snowflake_default.extra_dejson.account }}",
        "SNOWFLAKE_USER": "{{ conn.snowflake_default.login }}",
        "SNOWFLAKE_PRIVATE_KEY_FILE_PWD": "{{ conn.snowflake_default.password }}",
        "SNOWFLAKE_ROLE": "{{ conn.snowflake_default.extra_dejson.role }}",
        "SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake_default.extra_dejson.warehouse }}",
        "SNOWFLAKE_DATABASE": "{{ conn.snowflake_default.extra_dejson.database }}",
        "SNOWFLAKE_SCHEMA": "{{ conn.snowflake_default.schema }}",
        "SNOWFLAKE_PRIVATE_KEY_FILE_PATH": "{{ conn.snowflake_default.extra_dejson.private_key_file }}",
        "DBT_PROFILES_DIR": f"{os.environ.get('AIRFLOW_HOME')}/.dbt",
        "DBT_PROJECT_DIR": f"{os.environ.get('AIRFLOW_HOME')}/dbt_sf",
        "PATH": "/home/airflow/.local/bin:" + os.environ["PATH"],
    }


@dag(
    dag_id="dbt_transformations_dag",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["dbt", "transformations", "snowflake"],
    max_active_runs=1,
    description="Runs dbt transformations to convert raw data into analytics-ready models",
)
def dbt_transformations_dag():
    @task.bash(env={**get_dbt_snowflake_env_vars()})
    def dbt_build() -> str:
        """
        Run dbt build using bash command
        """
        return "dbt build"

    # Execute the task
    dbt_build()


# Create the DAG instance
dbt_transformations_dag()
