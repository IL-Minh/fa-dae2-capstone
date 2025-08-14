from datetime import datetime
from pathlib import Path

import pendulum
import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task


@dag(
    schedule="*/15 * * * *",  # Run every 15 minutes
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch-ingestion", "snowflake", "postgres", "cdc"],
    max_active_runs=1,
)
def batch_data_ingestion_dag():
    """
    ### Batch Data Ingestion DAG
    A DAG that performs two independent data ingestion tasks:
    1. PostgreSQL to Snowflake with CDC (Change Data Capture)
    2. CSV files from incoming/ folder to Snowflake

    Both tasks land data in different tables and are independent of each other.
    """

    @task()
    def postgres_to_snowflake_cdc():
        """
        #### PostgreSQL to Snowflake CDC Task
        Reads new/updated records from PostgreSQL transactions_sink table
        and loads them into Snowflake with CDC tracking.
        """
        # Get PostgreSQL connection (kafka postgres instance)
        pg_hook = PostgresHook(
            postgres_conn_id="postgres_kafka_default",
            schema="DB_T0",  # Default schema from kafka-docker-compose
        )

        # Get Snowflake connection
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        try:
            # Get last sync timestamp from Snowflake (CDC tracking)
            last_sync_query = """
            SELECT MAX(last_sync_timestamp) as last_sync
            FROM TRANSACTIONS_CDC_SYNC_LOG
            WHERE sync_type = 'postgres_cdc'
            """

            last_sync_result = sf_hook.get_records(last_sync_query)
            last_sync_timestamp = None

            if last_sync_result and last_sync_result[0][0]:
                last_sync_timestamp = last_sync_result[0][0]
                print(f"Last sync timestamp: {last_sync_timestamp}")
            else:
                # First run - get all data
                last_sync_timestamp = datetime(2021, 1, 1)
                print("First run - syncing all data")

            # Query PostgreSQL for new/updated records since last sync
            if last_sync_timestamp:
                pg_query = """
                SELECT tx_id, user_id, amount, currency, merchant, category,
                       timestamp, ingested_at
                FROM transactions_sink
                WHERE ingested_at > %s
                ORDER BY ingested_at
                """
                pg_params = (last_sync_timestamp,)
            else:
                pg_query = """
                SELECT tx_id, user_id, amount, currency, merchant, category,
                       timestamp, ingested_at
                FROM transactions_sink
                ORDER BY ingested_at
                """
                pg_params = ()

            # Execute PostgreSQL query
            pg_records = pg_hook.get_records(pg_query, parameters=pg_params)

            if not pg_records:
                print("No new records to sync from PostgreSQL")
                return {"rows_synced": 0, "last_sync": last_sync_timestamp}

            print(f"Found {len(pg_records)} new records to sync")

            # Convert to polars DataFrame
            columns = [
                "tx_id",
                "user_id",
                "amount",
                "currency",
                "merchant",
                "category",
                "timestamp",
                "ingested_at",
            ]
            df = pl.DataFrame(pg_records, schema=columns)

            # Prepare data for Snowflake insertion
            data_to_insert = []
            for row in df.iter_rows():
                data_to_insert.append(
                    {
                        "tx_id": row[0],
                        "user_id": row[1],
                        "amount": row[2],
                        "currency": row[3],
                        "merchant": row[4],
                        "category": row[5],
                        "timestamp": row[6],
                        "ingested_at": row[7],
                        "source_system": "postgres_cdc",
                        "sync_timestamp": datetime.utcnow(),
                    }
                )

            # Insert into Snowflake TRANSACTIONS_CDC table
            insert_query = """
            INSERT INTO TRANSACTIONS_CDC (
                tx_id, user_id, amount, currency, merchant, category,
                timestamp, ingested_at, source_system, sync_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            # Execute batch insert
            sf_conn = sf_hook.get_conn()
            sf_cursor = sf_conn.cursor()

            rows_inserted = 0
            for row_data in data_to_insert:
                try:
                    sf_cursor.execute(
                        insert_query,
                        (
                            row_data["tx_id"],
                            row_data["user_id"],
                            row_data["amount"],
                            row_data["currency"],
                            row_data["merchant"],
                            row_data["category"],
                            row_data["timestamp"],
                            row_data["ingested_at"],
                            row_data["source_system"],
                            row_data["sync_timestamp"],
                        ),
                    )
                    rows_inserted += 1
                except Exception as e:
                    print(f"Error inserting row {row_data['tx_id']}: {e}")
                    continue

            # Update CDC sync log
            current_sync_time = datetime.utcnow()
            sync_log_query = """
            INSERT INTO TRANSACTIONS_CDC_SYNC_LOG (
                sync_type, last_sync_timestamp, rows_synced, sync_timestamp
            ) VALUES (%s, %s, %s, %s)
            """
            sf_cursor.execute(
                sync_log_query,
                ("postgres_cdc", current_sync_time, rows_inserted, current_sync_time),
            )

            sf_conn.commit()
            sf_cursor.close()
            sf_conn.close()

            print(
                f"Successfully synced {rows_inserted} rows from PostgreSQL to Snowflake"
            )
            return {
                "rows_synced": rows_inserted,
                "last_sync": current_sync_time,
                "source": "postgres_cdc",
            }

        except Exception as e:
            print(f"Error in postgres_to_snowflake_cdc: {e}")
            raise

    @task()
    def csv_to_snowflake_batch():
        """
        #### CSV to Snowflake Batch Task
        Ingests CSV files from the incoming/ folder into Snowflake.
        Processes all CSV files and loads them into the TRANSACTIONS_BATCH table.
        """
        # Get Snowflake connection
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # Define incoming folder path
        incoming_path = Path("/opt/airflow/data/incoming")

        if not incoming_path.exists():
            print(f"Incoming folder {incoming_path} does not exist")
            return {"rows_loaded": 0, "files_processed": 0}

        # Find all CSV files
        csv_files = list(incoming_path.glob("*.csv"))

        if not csv_files:
            print("No CSV files found in incoming folder")
            return {"rows_loaded": 0, "files_processed": 0}

        print(f"Found {len(csv_files)} CSV files to process")

        total_rows_loaded = 0
        files_processed = 0

        for csv_file in csv_files:
            try:
                print(f"Processing file: {csv_file.name}")

                # Read CSV with polars
                df = pl.read_csv(csv_file)

                # Ensure required columns exist
                required_columns = [
                    "tx_id",
                    "user_id",
                    "amount",
                    "currency",
                    "merchant",
                    "category",
                    "timestamp",
                ]
                missing_columns = [
                    col for col in required_columns if col not in df.columns
                ]

                if missing_columns:
                    print(
                        f"Skipping {csv_file.name} - missing columns: {missing_columns}"
                    )
                    continue

                # Prepare data for Snowflake insertion
                data_to_insert = []
                for row in df.iter_rows():
                    data_to_insert.append(
                        {
                            "tx_id": row[0],
                            "user_id": row[1],
                            "amount": row[2],
                            "currency": row[3],
                            "merchant": row[4],
                            "category": row[5],
                            "timestamp": row[6],
                            "source_file": csv_file.name,
                            "ingested_at": datetime.utcnow(),
                        }
                    )

                # Insert into Snowflake TRANSACTIONS_BATCH table
                insert_query = """
                INSERT INTO TRANSACTIONS_BATCH (
                    tx_id, user_id, amount, currency, merchant, category,
                    timestamp, source_file, ingested_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """

                # Execute batch insert
                sf_conn = sf_hook.get_conn()
                sf_cursor = sf_conn.cursor()

                rows_inserted = 0
                for row_data in data_to_insert:
                    try:
                        sf_cursor.execute(
                            insert_query,
                            (
                                row_data["tx_id"],
                                row_data["user_id"],
                                row_data["amount"],
                                row_data["currency"],
                                row_data["merchant"],
                                row_data["category"],
                                row_data["timestamp"],
                                row_data["source_file"],
                                row_data["ingested_at"],
                            ),
                        )
                        rows_inserted += 1
                    except Exception as e:
                        print(f"Error inserting row {row_data['tx_id']}: {e}")
                        continue

                sf_conn.commit()
                sf_cursor.close()
                sf_conn.close()

                total_rows_loaded += rows_inserted
                files_processed += 1

                print(f"Successfully loaded {rows_inserted} rows from {csv_file.name}")

                # Optionally move processed file to archive folder
                # archive_path = incoming_path / "processed" / csv_file.name
                # archive_path.parent.mkdir(exist_ok=True)
                # csv_file.rename(archive_path)

            except Exception as e:
                print(f"Error processing file {csv_file.name}: {e}")
                continue

        print(
            f"Batch CSV processing complete: {files_processed} files, {total_rows_loaded} rows"
        )
        return {
            "rows_loaded": total_rows_loaded,
            "files_processed": files_processed,
            "source": "csv_batch",
        }

    # Execute both tasks independently (no dependencies)
    postgres_to_snowflake_cdc()
    csv_to_snowflake_batch()

    # Both tasks can run in parallel since they're independent
    # No need to set dependencies between them


# Create the DAG instance
batch_data_ingestion_dag()
