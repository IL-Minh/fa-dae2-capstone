"""
Connection testing utilities for Airflow DAGs.
Provides reusable functions to test database connections and validate prerequisites.
"""

import logging
from pathlib import Path
from typing import Any, Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class ConnectionTest:
    """
    Utility class for testing connections to various data sources and destinations.
    Uses proper logging instead of print statements.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def test_all_connections(self) -> Dict[str, Any]:
        """
        Test all required connections and return results.

        Returns:
            Dict containing test results and any errors encountered.
        """
        self.logger.info("Starting connection tests...")

        results = {
            "postgres_ok": False,
            "snowflake_ok": False,
            "csv_available": False,
            "errors": [],
        }

        # Test PostgreSQL connection
        results.update(self._test_postgres_connection())

        # Test Snowflake connection
        results.update(self._test_snowflake_connection())

        # Test CSV availability
        results.update(self._test_csv_availability())

        # Summary
        if all(
            [results["postgres_ok"], results["snowflake_ok"], results["csv_available"]]
        ):
            self.logger.info(
                "🎉 All connection tests passed! Ready to proceed with data ingestion."
            )
            return {"status": "success", "message": "All connections verified"}
        else:
            error_summary = "; ".join(results["errors"])
            self.logger.error(f"❌ Connection tests failed: {error_summary}")
            raise Exception(f"Connection validation failed: {error_summary}")

    def _test_postgres_connection(self) -> Dict[str, Any]:
        """Test PostgreSQL connection and table existence."""
        self.logger.info("🔍 Testing PostgreSQL connection...")

        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_kafka_default")

            # Test basic connection
            pg_conn = pg_hook.get_conn()
            pg_cursor = pg_conn.cursor()

            # Test if we can execute a simple query
            pg_cursor.execute("SELECT 1")
            test_result = pg_cursor.fetchone()

            if test_result and test_result[0] == 1:
                self.logger.info("✅ PostgreSQL connection successful")

                # Check if transactions_sink table exists
                try:
                    pg_cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = 'transactions_sink'
                        );
                    """)
                    table_exists = pg_cursor.fetchone()[0]

                    if table_exists:
                        self.logger.info("✅ transactions_sink table exists")
                        pg_cursor.close()
                        pg_conn.close()
                        return {"postgres_ok": True}
                    else:
                        error_msg = "transactions_sink table does not exist"
                        self.logger.warning(f"⚠️  PostgreSQL connected but {error_msg}")
                        pg_cursor.close()
                        pg_conn.close()
                        return {"postgres_ok": False, "errors": [error_msg]}

                except Exception as table_check_error:
                    error_msg = f"Table check failed: {table_check_error}"
                    self.logger.warning(
                        f"⚠️  Could not check table existence: {table_check_error}"
                    )
                    pg_cursor.close()
                    pg_conn.close()
                    return {"postgres_ok": False, "errors": [error_msg]}
            else:
                pg_cursor.close()
                pg_conn.close()
                return {
                    "postgres_ok": False,
                    "errors": ["PostgreSQL query test failed"],
                }

        except Exception as e:
            error_msg = f"PostgreSQL connection failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return {"postgres_ok": False, "errors": [error_msg]}

    def _test_snowflake_connection(self) -> Dict[str, Any]:
        """Test Snowflake connection and table existence."""
        self.logger.info("🔍 Testing Snowflake connection...")

        try:
            sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

            # Test basic connection
            sf_conn = sf_hook.get_conn()
            sf_cursor = sf_conn.cursor()

            # Test if we can execute a simple query
            sf_cursor.execute("SELECT 1")
            test_result = sf_cursor.fetchone()

            if test_result and test_result[0] == 1:
                self.logger.info("✅ Snowflake connection successful")

                # Check if required tables exist
                try:
                    sf_cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name IN ('TRANSACTIONS_STREAMING_KAFKA', 'TRANSACTIONS_BATCH_CSV')
                    """)
                    table_count = sf_cursor.fetchone()[0]

                    if table_count >= 2:
                        self.logger.info("✅ Required Snowflake tables exist")
                        sf_cursor.close()
                        sf_conn.close()
                        return {"snowflake_ok": True}
                    else:
                        error_msg = f"Missing Snowflake tables (found {table_count}/2)"
                        self.logger.warning(f"⚠️  Snowflake connected but {error_msg}")
                        sf_cursor.close()
                        sf_conn.close()
                        return {"snowflake_ok": False, "errors": [error_msg]}

                except Exception as table_check_error:
                    error_msg = f"Snowflake table check failed: {table_check_error}"
                    self.logger.warning(
                        f"⚠️  Could not check Snowflake tables: {table_check_error}"
                    )
                    sf_cursor.close()
                    sf_conn.close()
                    return {"snowflake_ok": False, "errors": [error_msg]}
            else:
                sf_cursor.close()
                sf_conn.close()
                return {
                    "snowflake_ok": False,
                    "errors": ["Snowflake query test failed"],
                }

        except Exception as e:
            error_msg = f"Snowflake connection failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return {"snowflake_ok": False, "errors": [error_msg]}

    def _test_csv_availability(self) -> Dict[str, Any]:
        """Test CSV files availability."""
        self.logger.info("🔍 Checking CSV files availability...")

        try:
            incoming_path = Path("/opt/airflow/data/incoming")

            if not incoming_path.exists():
                error_msg = f"Incoming folder {incoming_path} does not exist"
                self.logger.error(f"❌ {error_msg}")
                return {"csv_available": False, "errors": [error_msg]}
            else:
                csv_files = list(incoming_path.glob("*.csv"))
                if csv_files:
                    self.logger.info(
                        f"✅ Found {len(csv_files)} CSV files: {[f.name for f in csv_files]}"
                    )
                    return {"csv_available": True}
                else:
                    error_msg = "No CSV files found in incoming folder"
                    self.logger.error(f"❌ {error_msg}")
                    return {"csv_available": False, "errors": [error_msg]}

        except Exception as e:
            error_msg = f"CSV check failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return {"csv_available": False, "errors": [error_msg]}
