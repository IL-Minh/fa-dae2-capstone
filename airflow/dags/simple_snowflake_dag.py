import pendulum
import polars as pl
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["snowflake", "simple"],
)
def simple_snowflake_dag():
    """
    ### Simple Snowflake DAG
    A basic DAG with one task that connects to Snowflake and executes a simple query.
    """

    @task()
    def snowflake_query_task():
        """
        #### Snowflake Query Task
        Connects to Snowflake using the snowflake_default connection and executes a simple query.
        """
        # Get Snowflake connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # Execute a simple query using the hook's native methods to avoid pandas warnings
        query = (
            "SELECT CURRENT_TIMESTAMP() as current_time, CURRENT_USER() as current_user"
        )

        # Use get_records instead of get_pandas_df to avoid pandas SQLAlchemy warnings
        result = snowflake_hook.get_records(query)

        print("Snowflake connection successful!")
        print(f"Query result: {result}")

        # Convert to polars DataFrame for data processing
        columns = ["current_time", "current_user"]
        df = pl.DataFrame(result, schema=columns)
        print(f"Polars DataFrame: {df}")

        # Return the polars DataFrame as a serializable format
        return df.to_dicts()

    # Execute the task
    snowflake_query_task()


simple_snowflake_dag()
