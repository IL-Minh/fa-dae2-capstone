"""
Snowflake loading utilities for Airflow DAGs.
Provides reusable functions for processing CSV files and bulk loading into Snowflake.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeCSVStageLoader:
    """
    Utility class for processing CSV files and loading them into Snowflake using COPY INTO.
    Uses proper logging instead of print statements.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_csv_files(
        self, incoming_dir: str = "data/incoming"
    ) -> List[Dict[str, Any]]:
        """
        Process CSV files from the incoming directory.

        Args:
            incoming_dir: Path to directory containing CSV files

        Returns:
            List of processing results for each file
        """
        self.logger.info(f"Processing CSV files from {incoming_dir}")

        # Check if directory exists
        incoming_path = Path(incoming_dir)
        if not incoming_path.exists():
            raise FileNotFoundError(f"Directory {incoming_dir} does not exist")

        # List CSV files
        csv_files = list(incoming_path.glob("*.csv"))
        if not csv_files:
            self.logger.info("No CSV files found in incoming directory")
            return []

        self.logger.info(f"Found {len(csv_files)} CSV files to process")

        results = []

        # Process each CSV file
        for csv_file in csv_files:
            try:
                result = self._process_single_csv(csv_file)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing {csv_file.name}: {e}")
                results.append(
                    {"file": csv_file.name, "status": "error", "error": str(e)}
                )

        return results

    def _process_single_csv(self, csv_file: Path) -> Dict[str, Any]:
        """
        Process a single CSV file and load it into Snowflake using batch processing.

        Args:
            csv_file: Path to the CSV file

        Returns:
            Dict containing processing results
        """
        self.logger.info(f"Processing file: {csv_file.name}")

        try:
            # Get row count for reporting (without processing each row)
            row_count = self._count_csv_rows(csv_file)

            if row_count == 0:
                return {
                    "file": csv_file.name,
                    "status": "skipped",
                    "reason": "No rows found",
                }

            # Load directly to Snowflake using COPY INTO (true batch processing)
            load_result = self._load_csv_direct_to_snowflake(csv_file)

            return {
                "file": csv_file.name,
                "status": "success",
                "rows_processed": row_count,
                "load_result": load_result,
            }

        except Exception as e:
            self.logger.error(f"Error processing {csv_file.name}: {e}")
            return {"file": csv_file.name, "status": "error", "error": str(e)}

    def _count_csv_rows(self, csv_file: Path) -> int:
        """
        Count rows in CSV file without processing them.

        Args:
            csv_file: Path to the CSV file

        Returns:
            Number of rows (excluding header)
        """
        try:
            with open(csv_file, "r") as f:
                # Count lines and subtract 1 for header
                line_count = sum(1 for line in f)
                return max(0, line_count - 1)
        except Exception as e:
            self.logger.warning(f"Could not count rows in {csv_file.name}: {e}")
            return 0

    def _load_csv_direct_to_snowflake(
        self, csv_file: Path, snowflake_conn_id: str = "snowflake_default"
    ) -> Dict[str, Any]:
        """
        Load CSV file directly into Snowflake using COPY INTO.

        Args:
            csv_file: Path to the CSV file
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Dict containing load results
        """
        self.logger.info(
            "  🗄️  Bulk loading CSV directly into Snowflake using COPY INTO..."
        )

        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

            # Determine table and stage based on file type
            if "user_registrations" in csv_file.name.lower():
                # User registrations file
                table_name = "USER_REGISTRATIONS"
                stage_name = "STG_USER_REGISTRATIONS"
                columns = """
                    USER_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, INCOME_BRACKET,
                    CUSTOMER_TIER, RISK_PROFILE, CITY, STATE, COUNTRY,
                    REGISTRATION_DATE, PREFERRED_CATEGORIES, IS_ACTIVE, SOURCE_SYSTEM,
                    INGESTED_AT
                """
            else:
                # Transaction file (default)
                table_name = "TRANSACTIONS_BATCH_CSV"
                stage_name = "STG_TRANSACTIONS_BATCH_CSV"
                columns = """
                    TX_ID, USER_ID, AMOUNT, CURRENCY, MERCHANT,
                    CATEGORY, TIMESTAMP, SOURCE_FILE, INGESTED_AT
                """

            self.logger.info(f"    📋 Loading into table: {table_name}")
            self.logger.info(f"    📁 Using stage: {stage_name}")

            # Upload CSV file directly to the appropriate stage
            put_sql = f"""
            PUT file://{csv_file.absolute()} @{stage_name}
            """
            snowflake_hook.run(put_sql)
            self.logger.info(f"    ✅ Uploaded CSV to stage {stage_name}")

            # Use COPY INTO to bulk load data directly from CSV
            copy_sql = f"""
            COPY INTO {table_name} (
                {columns}
            )
            FROM @{stage_name}/{csv_file.name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('NULL', 'null', '')
                EMPTY_FIELD_AS_NULL = TRUE
                TIMESTAMP_FORMAT = 'AUTO'
            )
            ON_ERROR = 'CONTINUE'
            """

            result = snowflake_hook.run(copy_sql)
            self.logger.info(
                f"    ✅ COPY INTO completed successfully for {table_name}"
            )
            self.logger.info(f"    📊 Result: {result}")

            return {
                "status": "success",
                "table": table_name,
                "stage": stage_name,
                "result": result,
                "source_file": csv_file.name,
            }

        except Exception as e:
            self.logger.error(f"    ❌ Error during Snowflake bulk load: {e}")
            raise

    def process_csv_batch(
        self,
        incoming_dir: str = "data/incoming",
        snowflake_conn_id: str = "snowflake_default",
    ) -> Dict[str, Any]:
        """
        Complete CSV batch processing pipeline.

        Args:
            incoming_dir: Path to directory containing CSV files
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Dict containing overall processing results
        """
        try:
            results = self.process_csv_files(incoming_dir)

            # Calculate summary statistics
            total_files = len(results)
            successful_files = len([r for r in results if r["status"] == "success"])
            failed_files = len([r for r in results if r["status"] == "error"])
            skipped_files = len([r for r in results if r["status"] == "skipped"])

            total_rows = sum(
                [
                    r.get("rows_processed", 0)
                    for r in results
                    if r["status"] == "success"
                ]
            )

            self.logger.info("🎉 CSV batch ingestion completed successfully!")

            return {
                "status": "success",
                "summary": {
                    "total_files": total_files,
                    "successful_files": successful_files,
                    "failed_files": failed_files,
                    "skipped_files": skipped_files,
                    "total_rows_processed": total_rows,
                },
                "file_results": results,
            }

        except Exception as e:
            self.logger.error(f"Error in CSV batch processing: {e}")
            raise
