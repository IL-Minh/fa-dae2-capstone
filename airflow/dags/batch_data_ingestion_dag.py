"""
### Batch Data Ingestion DAG
A DAG that performs two independent data ingestion tasks:
1. PostgreSQL to Snowflake (simple insert)
2. CSV files from incoming/ folder to Snowflake

Both tasks land data in different tables and are independent of each other.
"""

import pendulum
from airflow.decorators import dag, task
from utils.connection_test import ConnectionTest
from utils.postgres_to_snowflake_processor import PostgresToSnowflakeProcessor
from utils.snowflake_csv_stage_loader import SnowflakeCSVStageLoader


@dag(
    schedule="*/15 * * * *",  # Run every 15 minutes
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["batch-ingestion", "snowflake", "postgres"],
    max_active_runs=1,
)
def batch_data_ingestion_dag():
    """
    ### Batch Data Ingestion DAG
    A DAG that performs two independent data ingestion tasks:
    1. PostgreSQL to Snowflake (simple insert)
    2. CSV files from incoming/ folder to Snowflake

    Both tasks land data in different tables and are independent of each other.
    """

    @task()
    def test_connections():
        """
        #### Connection Testing Task
        Tests connections to PostgreSQL, Snowflake, and checks CSV availability.
        Only proceeds if all tests pass.
        """
        connection_tester = ConnectionTest()
        return connection_tester.test_all_connections()

    @task()
    def postgres_to_snowflake():
        """
        #### PostgreSQL to Snowflake Task
        Reads all records from PostgreSQL transactions_sink table
        and loads them into Snowflake (simple insert, no CDC).
        """
        data_processor = PostgresToSnowflakeProcessor()
        return data_processor.process_postgres_to_snowflake()

    @task()
    def csv_to_snowflake_batch():
        """
        #### CSV to Snowflake Batch Task
        Ingests CSV files from the incoming/ folder into Snowflake using COPY INTO.
        This approach is much more efficient than row-by-row INSERTs.
        """
        snowflake_loader = SnowflakeCSVStageLoader()
        return snowflake_loader.process_csv_batch()

    # Execute connection test first, then main tasks only if test passes
    # In TaskFlow API, we use dependencies to ensure proper execution order
    connection_test = test_connections()

    # Main tasks depend on successful connection test
    postgres_task = postgres_to_snowflake()
    csv_task = csv_to_snowflake_batch()

    # Set dependencies: main tasks only run after connection test succeeds
    postgres_task.set_upstream(connection_test)
    csv_task.set_upstream(connection_test)


# Create the DAG instance
batch_data_ingestion_dag()
