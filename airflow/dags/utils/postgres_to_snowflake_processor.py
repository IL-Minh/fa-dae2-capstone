"""
Data processing utilities for Airflow DAGs.
Provides reusable functions for extracting and processing data from various sources.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class PostgresToSnowflakeProcessor:
    """
    Utility class for processing data from PostgreSQL and loading into Snowflake.
    Uses proper logging instead of print statements.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_postgres_data(
        self, postgres_conn_id: str = "postgres_kafka_default"
    ) -> List[Tuple[Any, ...]]:
        """
        Extract data from PostgreSQL transactions_sink table.

        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL

        Returns:
            List of tuples containing the extracted data
        """
        self.logger.info("Reading data from PostgreSQL transactions_sink table...")

        try:
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

            # Query to get data from PostgreSQL
            select_query = """
                SELECT tx_id, user_id, amount, currency, merchant, category,
                       timestamp, ingested_at
                FROM transactions_sink
                ORDER BY ingested_at
            """

            # Execute query and get results
            postgres_conn = postgres_hook.get_conn()
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(select_query)
            results = postgres_cursor.fetchall()

            self.logger.info(f"Found {len(results)} records to sync from PostgreSQL")

            postgres_cursor.close()
            postgres_conn.close()

            return results

        except Exception as e:
            self.logger.error(f"Error extracting data from PostgreSQL: {e}")
            raise

    def load_to_snowflake_streaming(
        self, data: List[Tuple[Any, ...]], snowflake_conn_id: str = "snowflake_default"
    ) -> int:
        """
        Load data into Snowflake TRANSACTIONS_STREAMING_KAFKA table.

        Args:
            data: List of tuples containing the data to insert
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Number of rows successfully inserted
        """
        if not data:
            self.logger.info("No new data to sync")
            return 0

        self.logger.info(
            "Inserting data into Snowflake TRANSACTIONS_STREAMING_KAFKA table..."
        )

        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

            # Prepare insert query
            insert_query = """
                INSERT INTO TRANSACTIONS_STREAMING_KAFKA (
                    TX_ID, USER_ID, AMOUNT, CURRENCY, MERCHANT, CATEGORY,
                    TIMESTAMP, INGESTED_AT, AIRFLOW_INGESTED_AT
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            # Execute batch insert
            snowflake_conn = snowflake_hook.get_conn()
            snowflake_cursor = snowflake_conn.cursor()

            rows_inserted = 0
            for row in data:
                try:
                    # Add current timestamp for Airflow ingestion tracking
                    airflow_timestamp = datetime.now(timezone.utc)

                    snowflake_cursor.execute(
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
                            airflow_timestamp,  # airflow_ingested_at
                        ),
                    )
                    rows_inserted += 1
                except Exception as e:
                    self.logger.error(f"Error inserting row {row[0]}: {e}")
                    continue

            snowflake_conn.commit()
            snowflake_cursor.close()
            snowflake_conn.close()

            self.logger.info(
                f"Successfully inserted {rows_inserted} rows into TRANSACTIONS_STREAMING_KAFKA"
            )
            return rows_inserted

        except Exception as e:
            self.logger.error(f"Error loading data to Snowflake: {e}")
            raise

    def process_postgres_to_snowflake(
        self,
        postgres_conn_id: str = "postgres_kafka_default",
        snowflake_conn_id: str = "snowflake_default",
    ) -> Dict[str, Any]:
        """
        Complete pipeline: Extract from PostgreSQL and load to Snowflake.

        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Dict containing processing results
        """
        try:
            # Extract data
            data = self.extract_postgres_data(postgres_conn_id)

            # Load to Snowflake
            rows_inserted = self.load_to_snowflake_streaming(data, snowflake_conn_id)

            return {
                "status": "success",
                "rows_extracted": len(data),
                "rows_inserted": rows_inserted,
                "source": "postgres_streaming",
            }

        except Exception as e:
            self.logger.error(f"Error in postgres_to_snowflake pipeline: {e}")
            raise
