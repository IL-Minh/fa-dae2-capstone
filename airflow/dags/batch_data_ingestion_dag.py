from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


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
        results = {
            "postgres_ok": False,
            "snowflake_ok": False,
            "csv_available": False,
            "errors": [],
        }

        # Test 1: PostgreSQL Connection
        try:
            print("🔍 Testing PostgreSQL connection...")
            pg_hook = PostgresHook(
                postgres_conn_id="postgres_kafka_default",
                schema="DB_T0",
            )

            # Test basic connection
            pg_conn = pg_hook.get_conn()
            pg_cursor = pg_conn.cursor()

            # Test if we can execute a simple query
            pg_cursor.execute("SELECT 1")
            test_result = pg_cursor.fetchone()

            if test_result and test_result[0] == 1:
                print("✅ PostgreSQL connection successful")

                # Additional test: check if transactions_sink table exists
                try:
                    pg_cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = 'transactions_sink'
                        );
                    """)
                    table_exists = pg_cursor.fetchone()[0]

                    if table_exists:
                        print("✅ transactions_sink table exists")
                        results["postgres_ok"] = True
                    else:
                        print(
                            "⚠️  PostgreSQL connected but transactions_sink table not found"
                        )
                        results["errors"].append(
                            "transactions_sink table does not exist"
                        )

                except Exception as table_check_error:
                    print(f"⚠️  Could not check table existence: {table_check_error}")
                    results["errors"].append(f"Table check failed: {table_check_error}")
            else:
                results["errors"].append("PostgreSQL query test failed")

            pg_cursor.close()
            pg_conn.close()

        except Exception as e:
            error_msg = f"PostgreSQL connection failed: {e}"
            print(f"❌ {error_msg}")
            results["errors"].append(error_msg)

        # Test 2: Snowflake Connection
        try:
            print("🔍 Testing Snowflake connection...")
            sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

            # Test basic connection
            sf_conn = sf_hook.get_conn()
            sf_cursor = sf_conn.cursor()

            # Test if we can execute a simple query
            sf_cursor.execute("SELECT 1")
            test_result = sf_cursor.fetchone()

            if test_result and test_result[0] == 1:
                print("✅ Snowflake connection successful")

                # Additional test: check if required tables exist
                try:
                    sf_cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name IN ('TRANSACTIONS_POSTGRES', 'TRANSACTIONS_BATCH')
                    """)
                    table_count = sf_cursor.fetchone()[0]

                    if table_count >= 2:
                        print("✅ Required Snowflake tables exist")
                        results["snowflake_ok"] = True
                    else:
                        print(
                            f"⚠️  Snowflake connected but only {table_count}/2 required tables found"
                        )
                        results["errors"].append(
                            f"Missing Snowflake tables (found {table_count}/2)"
                        )

                except Exception as table_check_error:
                    print(f"⚠️  Could not check Snowflake tables: {table_check_error}")
                    results["errors"].append(
                        f"Snowflake table check failed: {table_check_error}"
                    )
            else:
                results["errors"].append("Snowflake query test failed")

            sf_cursor.close()
            sf_conn.close()

        except Exception as e:
            error_msg = f"Snowflake connection failed: {e}"
            print(f"❌ {error_msg}")
            results["errors"].append(error_msg)

        # Test 3: CSV Files Availability
        try:
            print("🔍 Checking CSV files availability...")
            incoming_path = Path("/opt/airflow/data/incoming")

            if not incoming_path.exists():
                results["errors"].append(
                    f"Incoming folder {incoming_path} does not exist"
                )
                print(f"❌ Incoming folder {incoming_path} does not exist")
            else:
                csv_files = list(incoming_path.glob("*.csv"))
                if csv_files:
                    print(
                        f"✅ Found {len(csv_files)} CSV files: {[f.name for f in csv_files]}"
                    )
                    results["csv_available"] = True
                else:
                    results["errors"].append("No CSV files found in incoming folder")
                    print("❌ No CSV files found in incoming folder")

        except Exception as e:
            error_msg = f"CSV check failed: {e}"
            print(f"❌ {error_msg}")
            results["errors"].append(error_msg)

        # Summary
        if (
            results["postgres_ok"]
            and results["snowflake_ok"]
            and results["csv_available"]
        ):
            print(
                "🎉 All connection tests passed! Ready to proceed with data ingestion."
            )
            return {"status": "success", "message": "All connections verified"}
        else:
            error_summary = "; ".join(results["errors"])
            print(f"❌ Connection tests failed: {error_summary}")
            raise Exception(f"Connection validation failed: {error_summary}")

    @task()
    def postgres_to_snowflake():
        """
        #### PostgreSQL to Snowflake Task
        Reads all records from PostgreSQL transactions_sink table
        and loads them into Snowflake (simple insert, no CDC).
        """
        # Get PostgreSQL connection (kafka postgres instance)
        pg_hook = PostgresHook(
            postgres_conn_id="postgres_kafka_default",
            schema="DB_T0",  # Default schema from kafka-docker-compose
        )

        # Get Snowflake connection
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        try:
            # Query PostgreSQL for all records
            pg_query = """
            SELECT tx_id, user_id, amount, currency, merchant, category,
                   timestamp, ingested_at
            FROM transactions_sink
            ORDER BY ingested_at
            """

            # Execute PostgreSQL query
            pg_records = pg_hook.get_records(pg_query)

            if not pg_records:
                print("No records found in PostgreSQL transactions_sink table")
                return {"rows_synced": 0}

            print(f"Found {len(pg_records)} records to sync from PostgreSQL")

            # Insert into Snowflake TRANSACTIONS_POSTGRES table
            insert_query = """
            INSERT INTO TRANSACTIONS_POSTGRES (
                tx_id, user_id, amount, currency, merchant, category,
                timestamp, ingested_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            # Execute batch insert directly from PostgreSQL results
            sf_conn = sf_hook.get_conn()
            sf_cursor = sf_conn.cursor()

            rows_inserted = 0
            for row in pg_records:
                try:
                    sf_cursor.execute(
                        insert_query,
                        (
                            row[0],  # tx_id
                            row[1],  # user_id
                            row[2],  # amount
                            row[3],  # currency
                            row[4],  # merchant
                            row[5],  # category
                            row[6],  # timestamp
                            row[7],  # ingested_at
                        ),
                    )
                    rows_inserted += 1
                except Exception as e:
                    print(f"Error inserting row {row[0]}: {e}")
                    continue

            sf_conn.commit()
            sf_cursor.close()
            sf_conn.close()

            print(f"Successfully synced {rows_inserted} rows from PostgreSQL")
            return {"rows_synced": rows_inserted}

        except Exception as e:
            print(f"Error in postgres_to_snowflake: {e}")
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

                # Read CSV with standard Python csv module
                import csv

                # Ensure required columns exist by reading header first
                with open(csv_file, "r") as f:
                    reader = csv.reader(f)
                    header = next(reader)

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
                        col for col in required_columns if col not in header
                    ]

                    if missing_columns:
                        print(
                            f"Skipping {csv_file.name} - missing columns: {missing_columns}"
                        )
                        continue

                    # Prepare data for Snowflake insertion
                    data_to_insert = []
                    for row in reader:
                        if len(row) >= 7:  # Ensure we have enough columns
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
                                    "ingested_at": datetime.now(
                                        timezone.utc
                                    ),  # Keep UTC for database consistency
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
